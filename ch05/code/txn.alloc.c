#include <errno.h>
#include <stdio.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/infrastructure.h>
#include <gavran/internal.h>
/*
static result_t get_free_space_bitmap_word(txn_t *tx,
                                           uint64_t page_num,
                                           uint64_t *word) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page =
      start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_raw_get_page(tx, &bitmap_page));
  uint64_t *bitmap = bitmap_page.address;
  *word = bitmap[(page_num % BITS_IN_PAGE) / 64];
  return success();
}*/

// tag::txn_free_page[]
static result_t db_free_space_mark_free(txn_t *tx,
                                        uint64_t page_num) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page =
      start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_raw_modify_page(tx, &bitmap_page));
  bitmap_clear(bitmap_page.address, page_num % BITS_IN_PAGE);
  return success();
}

result_t txn_free_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  ensure(txn_raw_modify_page(tx, page));
  uint64_t pages = TO_PAGES(page->size);
  memset(page->address, 0, PAGE_SIZE * pages);

  for (size_t i = 0; i < pages; i++) {
    ensure(db_free_space_mark_free(tx, page->page_num + i));
  }

  return success();
}
// end::txn_free_page[]

static result_t txn_free_space_mark_page_busy(txn_t *tx,
                                              uint64_t page_num) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page =
      start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_raw_modify_page(tx, &bitmap_page));
  bitmap_set(bitmap_page.address, page_num % BITS_IN_PAGE);
  return success();
}

result_t txn_is_page_busy(txn_t *tx, uint64_t page_num, bool *busy) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page =
      start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_raw_get_page(tx, &bitmap_page));
  *busy = bitmap_is_set(bitmap_page.address, page_num % BITS_IN_PAGE);
  return success();
}

result_t txn_allocate_page(txn_t *tx, page_t *page,
                           uint64_t nearby_hint) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  if (!page->size) page->size = PAGE_SIZE;

  uint64_t pages = TO_PAGES(page->size);

  page_t bitmap_page = {.page_num = start};
  ensure(txn_raw_get_page(tx, &bitmap_page));

  bitmap_search_state_t search = {
      .input = {.bitmap = bitmap_page.address,
                .bitmap_size = bitmap_page.size / sizeof(uint64_t),
                .space_required = pages,
                .near_position = nearby_hint}};

  if (bitmap_search(&search)) {
    page->page_num = search.output.found_position;
    for (size_t i = 0; i < pages; i++) {
      ensure(txn_free_space_mark_page_busy(
          tx, search.output.found_position + i));
    }

    ensure(txn_raw_modify_page(tx, page));
    memset(page->address, 0, PAGE_SIZE * pages);

    return success();
  }

  failed(ENOSPC, msg("No more room left in the file to allocate"),
         with(tx->state->db->handle->filename, "%s"));
}
// end::txn_allocate_page[]
