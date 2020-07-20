#include <errno.h>
#include <stdio.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/infrastructure.h>
#include <gavran/internal.h>

// tag::txn_free_space_mark_page[]
static result_t txn_free_space_mark_page(txn_t *tx, uint64_t page_num,
                                         bool busy) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page =
      start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_modify_page(tx, &bitmap_page));
  bitmap_set(bitmap_page.address, page_num % BITS_IN_PAGE, busy);
  return success();
}
// end::txn_free_space_mark_page[]

// tag::txn_allocate_metadata_entry[]
static result_t txn_allocate_metadata_entry(txn_t *tx,
                                            uint64_t page_num,
                                            page_metadata_t **entry) {
  page_t meta_page = {.page_num = page_num & PAGES_IN_METADATA_MASK};
  bool exists;
  ensure(txn_is_page_busy(tx, meta_page.page_num, &exists));
  ensure(txn_raw_modify_page(tx, &meta_page));
  page_metadata_t *self = meta_page.address;
  if (!exists) {
    // first time, need to allocate it all
    self->common.page_flags = page_flags_metadata;
    ensure(txn_free_space_mark_page(tx, meta_page.page_num, true));
  }
  ensure(self->common.page_flags == page_flags_metadata,
         msg("Expected page to be metadata page, but wasn't"),
         with(page_num, "%lu"), with(self->common.page_flags, "%x"));

  page_metadata_t *metadata =
      &self[page_num & ~PAGES_IN_METADATA_MASK];
  ensure(!metadata->common.page_flags,
         msg("Expected metadata entry to be empty, but was in use"),
         with(page_num, "%lu"),
         with(metadata->common.page_flags, "%x"));

  memset(metadata, 0, sizeof(page_metadata_t));
  *entry = metadata;
  return success();
}
// end::txn_allocate_metadata_entry[]

// tag::txn_allocate_page[]
result_t txn_allocate_page(txn_t *tx, page_t *page,
                           page_metadata_t *metadata,
                           uint64_t nearby_hint) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  if (!page->number_of_pages) page->number_of_pages = 1;

  page_t bitmap_page = {.page_num = start};
  ensure(txn_get_page(tx, &bitmap_page));
  bitmap_search_state_t search = {
      .input = {
          .bitmap = bitmap_page.address,
          .bitmap_size = (bitmap_page.number_of_pages * PAGE_SIZE) /
                         sizeof(uint64_t),
          .space_required = page->number_of_pages,
          .near_position = nearby_hint}};
  // <1>
  if ((search.input.space_required & ~PAGES_IN_METADATA_MASK) == 0) {
    // we must use one more in this cases, so the first page
    // would "poke" into an existing range that has metadata pages
    search.input.space_required++;
  }
  if (bitmap_search(&search)) {
    page->page_num = search.output.found_position;
    ensure(txn_modify_page(tx, page));
    memset(page->address, 0, PAGE_SIZE * page->number_of_pages);
    for (size_t i = 0; i < page->number_of_pages; i++) {
      ensure(txn_free_space_mark_page(
          tx, search.output.found_position + i, true));
    }
    // <2>
    ensure(
        txn_allocate_metadata_entry(tx, page->page_num, &metadata));
    return success();
  }

  failed(ENOSPC, msg("No more room left in the file to allocate"),
         with(tx->state->db->handle->filename, "%s"));
}
// end::txn_allocate_page[]

// tag::txn_free_space_bitmap_metadata_range_is_free[]
static result_t txn_free_space_bitmap_metadata_range_is_free(
    txn_t *tx, uint64_t page_num, bool *is_free) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page =
      start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_raw_get_page(tx, &bitmap_page));
  uint64_t *bitmap = bitmap_page.address;
  size_t index = (page_num % BITS_IN_PAGE) / 64;
  *is_free = bitmap[index] == 1 && bitmap[index + 1] == 0;
  return success();
}
// end::txn_free_space_bitmap_metadata_range_is_free[]

// tag::txn_free_page[]
result_t txn_free_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  if ((page->number_of_pages & ~PAGES_IN_METADATA_MASK) == 0)
    page->number_of_pages++;  // allocations on 128 pages boundary
                              // have an extra page tacked on them

  ensure(txn_modify_page(tx, page));
  memset(page->address, 0, PAGE_SIZE * page->number_of_pages);

  for (size_t i = 0; i < page->number_of_pages; i++) {
    ensure(txn_free_space_mark_page(tx, page->page_num + i, false));
  }

  // <1>
  page_metadata_t *metadata;
  ensure(txn_modify_metadata(tx, page->page_num, &metadata));
  memset(metadata, 0, sizeof(page_metadata_t));

  // <2>
  uint64_t metadata_page_num =
      page->page_num & PAGES_IN_METADATA_MASK;
  if (metadata_page_num != page->page_num && page->page_num) {
    bool is_free;
    ensure(txn_free_space_bitmap_metadata_range_is_free(
        tx, metadata_page_num, &is_free));
    if (is_free) {
      page_t metadata_page = {.page_num = metadata_page_num};
      ensure(txn_free(tx, &metadata_page));
    }
  }

  return success();
}
// end::txn_free_page[]
