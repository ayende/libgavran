#include <errno.h>
#include <string.h>

#include "db.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::txn_free_page[]
// <1>
static result_t get_free_space_bitmap_word(txn_t *tx, uint64_t page_num,
                                           uint64_t *word) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page = start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_get_page(tx, &bitmap_page));
  uint64_t *bitmap = bitmap_page.address;
  *word = bitmap[(page_num % BITS_IN_PAGE) / 64];
  return success();
}

// <2>
static result_t mark_page_as_free(txn_t *tx, uint64_t page_num) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page = start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_modify_page(tx, &bitmap_page));
  clear_bit(bitmap_page.address, page_num % BITS_IN_PAGE);
  return success();
}

result_t txn_free_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  ensure(txn_modify_page(tx, page));
  uint64_t pages = page->overflow_size / PAGE_SIZE +
                   (page->overflow_size % PAGE_SIZE ? 1 : 0);

  // <3>
  if ((pages & ~PAGES_IN_METADATA_MASK) == 0)
    pages++; // allocations on 128 pages boundary have an extra page tacked on

  // <4>
  for (size_t i = 0; i < pages; i++) {
    ensure(mark_page_as_free(tx, page->page_num + i));
    // <5>
    page_metadata_t *metadata;
    ensure(txn_modify_metadata(tx, page->page_num + i, &metadata));
    memset(metadata, 0, sizeof(page_metadata_t));
  }

  // <6>
  memset(page->address, 0, PAGE_SIZE * pages);

  // <7>
  uint64_t metadata_page_num = page->page_num & PAGES_IN_METADATA_MASK;
  if (metadata_page_num != page->page_num) {
    uint64_t free_space_bitmap_word;
    ensure(get_free_space_bitmap_word(tx, metadata_page_num,
                                      &free_space_bitmap_word));
    if (free_space_bitmap_word == 1) {
      ensure(get_free_space_bitmap_word(tx, metadata_page_num + 64,
                                        &free_space_bitmap_word));
      if (free_space_bitmap_word == 0) {
        // we have a 1 MB range where the only busy page is the metadata itself?
        // let's free that.
        page_t metadata_page = {.page_num = metadata_page_num};
        ensure(txn_free_page(tx, &metadata_page));
      }
    }
  }

  return success();
}
// end::txn_free_page[]

// tag::txn_allocate_page[]
static result_t mark_page_as_busy(txn_t *tx, uint64_t page_num) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page = start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_modify_page(tx, &bitmap_page));
  set_bit(bitmap_page.address, page_num % BITS_IN_PAGE);
  return success();
}

// <1>
result_t txn_page_busy(txn_t *tx, uint64_t page_num, bool *busy) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page = start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_get_page(tx, &bitmap_page));
  *busy = is_bit_set(bitmap_page.address, page_num % BITS_IN_PAGE);
  return success();
}

// <2>
static result_t allocate_metadata_entry(txn_t *tx, uint64_t page_num,
                                        page_metadata_t **entry) {
  page_t p = {.page_num = page_num & PAGES_IN_METADATA_MASK};

  bool exists;
  ensure(txn_page_busy(tx, p.page_num, &exists));

  ensure(txn_modify_page(tx, &p));
  page_metadata_t *self = p.address;
  if (!exists) {
    // <3>
    // first time, need to allocate it all
    self->type = page_metadata;
    self->overflow_size = PAGE_SIZE;
    ensure(mark_page_as_busy(tx, p.page_num));
  }

  ensure(self->type == page_metadata,
         msg("Expected page to be metadata page, but wasn't"),
         with(page_num, "%lu"), with(p.page_num, "%lu"),
         with(self->type, "%x"));

  size_t index = page_num & ~PAGES_IN_METADATA_MASK;
  page_metadata_t *metadata = self + index;
  ensure(!metadata->type,
         msg("Expected metadata entry to be empty, but was in use"),
         with(page_num, "%lu"), with(p.page_num, "%lu"), with(self->type, "%x"),
         with(index, "%zu"));

  memset(metadata, 0, sizeof(page_metadata_t));

  *entry = metadata;

  return success();
}

result_t txn_allocate_page(txn_t *tx, page_t *page, uint64_t nearby_hint) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  page_metadata_t *freespace_metadata;
  ensure(txn_get_metadata(tx, start, &freespace_metadata));

  if (!page->overflow_size)
    page->overflow_size = PAGE_SIZE;

  // <4>
  uint32_t pages = page->overflow_size / PAGE_SIZE +
                   (page->overflow_size % PAGE_SIZE ? 1 : 0);

  page_t bitmap_page = {.page_num = start};
  ensure(txn_get_page(tx, &bitmap_page));

  bitmap_search_state_t search;
  init_search(&search, bitmap_page.address,
              freespace_metadata->overflow_size / sizeof(uint64_t), pages);
  search.near_position = nearby_hint;

  if (search_free_range_in_bitmap(&search)) {

    page->page_num = search.found_position;
    for (size_t i = 0; i < pages; i++) {
      ensure(mark_page_as_busy(tx, search.found_position + i));
    }

    page_metadata_t *metadata;
    ensure(allocate_metadata_entry(tx, page->page_num, &metadata));
    metadata->overflow_size = page->overflow_size;

    ensure(txn_modify_page(tx, page));
    memset(page->address, 0, PAGE_SIZE * pages);

    return success();
  }

  if (db_try_increase_file_size(tx, pages) && !errors_get_count()) {
    return txn_allocate_page(tx, page, nearby_hint);
  }

  failed(ENOSPC, msg("No more room left in the file to allocate"),
         with(palfs_get_filename(tx->state->db->handle), "%s"));
}
// end::txn_allocate_page[]
