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
  ensure(txn_raw_modify_page(tx, &bitmap_page));
  bitmap_set(bitmap_page.address, page_num % BITS_IN_PAGE, busy);
  return success();
}
// end::txn_free_space_mark_page[]

// tag::txn_allocate_page[]
result_t txn_allocate_page(txn_t *tx, page_t *page,
                           uint64_t nearby_hint) {
  file_header_t *header = &tx->state->global_state.header;
  uint64_t start = header->free_space_bitmap_start;

  if (!page->number_of_pages) page->number_of_pages = 1;

  // <1>
  page_t bitmap_page = {.page_num = start};
  ensure(txn_raw_get_page(tx, &bitmap_page));
  bitmap_search_state_t search = {
      .input = {
          .bitmap = bitmap_page.address,
          .bitmap_size = (bitmap_page.number_of_pages * PAGE_SIZE) /
                         sizeof(uint64_t),
          .space_required = page->number_of_pages,
          .near_position = nearby_hint}};
  // <2>
  if (bitmap_search(&search)) {
    page->page_num = search.output.found_position;
    // <3>
    ensure(txn_raw_modify_page(tx, page));
    memset(page->address, 0, PAGE_SIZE * page->number_of_pages);
    for (size_t i = 0; i < page->number_of_pages; i++) {
      ensure(txn_free_space_mark_page(
          tx, search.output.found_position + i, true));
    }

    return success();
  }

  // <4>
  failed(ENOSPC, msg("No more room left in the file to allocate"),
         with(tx->state->db->handle->filename, "%s"));
}
// end::txn_allocate_page[]

// tag::txn_free_page[]
result_t txn_free_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  ensure(txn_raw_modify_page(tx, page));
  memset(page->address, 0, PAGE_SIZE * page->number_of_pages);

  for (size_t i = 0; i < page->number_of_pages; i++) {
    ensure(txn_free_space_mark_page(tx, page->page_num + i, false));
  }

  return success();
}
// end::txn_free_page[]
