#include <errno.h>
#include <string.h>

#include "db.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::txn_free_page[]
result_t txn_free_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  ensure(txn_modify_page(tx, page));

  memset(page->address, 0, PAGE_SIZE);

  page_t free_space_page = {
      .page_num = tx->state->db->header.free_space_bitmap_start +
                  page->page_num / BITS_IN_PAGE};

  ensure(txn_modify_page(tx, &free_space_page));
  clear_bit(free_space_page.address, page->page_num % BITS_IN_PAGE);

  return success();
}
// end::txn_free_page[]

// tag::txn_allocate_page[]
// <1>
static bool mark_page_as_busy(txn_t *tx, uint64_t page_num) {
  file_header_t *header = &tx->state->db->header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page =
      start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  ensure(txn_modify_page(tx, &bitmap_page));
  set_bit(bitmap_page.address, page_num % BITS_IN_PAGE);
  return success();
}

result_t txn_allocate_page(txn_t *tx, page_t *page,
                           uint64_t nearby_hint) {
  file_header_t *header = &tx->state->db->header;
  uint64_t start = header->free_space_bitmap_start;
  uint64_t count = header->free_space_bitmap_in_pages;

  // <2>
  // here we search each page of the free space map independently
  for (uint64_t i = 0; i < count; i++) {
    page_t bitmap_page = {.page_num = i + start};
    ensure(txn_get_page(tx, &bitmap_page));

    // <3>
    bitmap_search_state_t search;
    init_search(&search, bitmap_page.address,
                PAGE_SIZE / sizeof(uint64_t), 1);
    search.near_position = nearby_hint;

    if (search_free_range_in_bitmap(&search)) {
      // <4>
      page->page_num = search.found_position;
      ensure(txn_modify_page(tx, page));
      memset(page->address, 0, PAGE_SIZE);
      ensure(mark_page_as_busy(tx, search.found_position));
      return success();
    }
  }
  // <5>
  failed(ENOSPC, msg("No more room left in the file to allocate"),
         with(palfs_get_filename(tx->state->db->handle), "%s"));
}
// end::txn_allocate_page[]
