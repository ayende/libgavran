#include <gavran/db.h>
#include <gavran/internal.h>
#include <string.h>

// tag::db_find_next_db_size[]
static uint64_t db_find_next_db_size(uint64_t current,
                                     uint64_t requested_size) {
  uint64_t uint_of_growth = next_power_of_two(current / 10);
  uint64_t suggested = uint_of_growth;
  if (suggested > 1024 * 1024 * 1024) suggested = 1024 * 1024 * 1024;
  while (suggested <= requested_size) {
    suggested += uint_of_growth;
  }
  if (suggested < (1024 * 1024)) suggested = 1024 * 1024;

  uint64_t next_p2_file = next_power_of_two(current + suggested);
  if (next_p2_file < current + uint_of_growth * 2)
    return next_p2_file;
  return current + suggested;
}
// end::db_find_next_db_size[]

// tag::db_move_free_space_bitmap[]
static result_t db_move_free_space_bitmap(
    txn_t *tx, uint64_t from, uint64_t to,
    page_metadata_t *old_metadata, page_t *old) {
  // <1>
  uint64_t pages = ROUND_UP(to, BITS_IN_PAGE);
  pages += next_power_of_two(pages / 10);
  // <2>
  void *new_map;
  ensure(mem_alloc_page_aligned(&new_map, pages * PAGE_SIZE));
  defer(free, new_map);
  // <3>
  size_t old_bitmap_size =
      old_metadata->free_space.number_of_pages * PAGE_SIZE;
  memcpy(new_map, old->address, old_bitmap_size);
  memset(new_map + old_bitmap_size, INT32_MAX,
         pages * PAGE_SIZE - old_bitmap_size);
  // <4>
  for (uint64_t i = from; i < to; i++) {
    bitmap_set(new_map, i, false);  // new pages are free
  }
  bitmap_search_state_t search = {
      .input = {.bitmap = new_map,
                .bitmap_size = pages * PAGE_SIZE / sizeof(uint64_t),
                .near_position = 0,  // anywhere is good
                .space_required = pages}};
  // <5>
  if (!bitmap_search(&search)) {
    failed(ENOSPC,
           msg("No place for free space bitmap after resize!"),
           with(pages, "%lu"));
  }
  // <6>
  for (uint64_t i = 0; i <= pages; i++) {  // new bitmap page are busy
    bitmap_set(new_map, search.output.found_position + i, true);
  }
  page_t new_page = {.page_num = search.output.found_position,
                     .number_of_pages = pages};
  // <7>
  ensure(txn_raw_modify_page(tx, &new_page));
  memcpy(new_page.address, new_map, pages * PAGE_SIZE);
  // <8>
  page_metadata_t *free_space_metadata;
  ensure(txn_modify_metadata(tx, search.output.found_position,
                             &free_space_metadata));
  free_space_metadata->free_space.page_flags =
      page_flags_free_space_bitmap;
  free_space_metadata->free_space.number_of_pages = pages;
  // <9>
  tx->state->global_state.header.free_space_bitmap_start =
      search.output.found_position;
  // <10>
  ensure(txn_free_page(tx, old));  // release the old space
  return success();
}
// end::db_move_free_space_bitmap[]

// tag::db_finalize_file_size_increase[]
static result_t db_increase_free_space_bitmap(txn_t *tx,
                                              uint64_t from,
                                              uint64_t to) {
  uint64_t free_space_page =
      tx->state->global_state.header.free_space_bitmap_start;
  page_metadata_t *metadata;
  ensure(txn_modify_metadata(tx, free_space_page, &metadata));
  page_t free_space = {.page_num = free_space_page};
  ensure(txn_modify_page(tx, &free_space));
  if (metadata->free_space.number_of_pages * BITS_IN_PAGE > to) {
    // can do an in place update
    for (uint64_t i = from; i < to; i++) {
      bitmap_set(free_space.address, i, false);
    }
    return success();
  }
  // need to move to a new location
  return db_move_free_space_bitmap(tx, from, to, metadata,
                                   &free_space);
}
static result_t db_finalize_file_size_increase(txn_t *tx,
                                               uint64_t from,
                                               uint64_t to) {
  ensure(db_increase_free_space_bitmap(tx, from, to));
  page_metadata_t *file_header_metadata;
  ensure(txn_modify_metadata(tx, 0, &file_header_metadata));
  tx->state->global_state.header.number_of_pages = to;
  memcpy(&file_header_metadata->file_header,
         &tx->state->global_state.header, sizeof(file_header_t));
  return success();
}
// end::db_finalize_file_size_increase[]

// tag::db_try_increase_file_size[]
static void db_clear_old_mmap(void *state) {
  // no way to report state, will use errors_push for that
  (void)pal_unmap((span_t *)state);
}
static result_t db_new_size_can_fit_free_space_bitmap(
    uint64_t current_size, uint64_t *new_size) {
  uint32_t required_pages =
      TO_PAGES(ROUND_UP(*new_size / PAGE_SIZE, BITS_IN_PAGE)) * 2;
  if (*new_size - current_size > required_pages * PAGE_SIZE)
    return success();
  *new_size += required_pages * PAGE_SIZE;
  return success();
}
implementation_detail result_t
db_try_increase_file_size(txn_t *tx, uint64_t pages) {
  uint64_t new_size = db_find_next_db_size(
      tx->state->global_state.header.number_of_pages * PAGE_SIZE,
      pages * PAGE_SIZE);
  ensure(db_new_size_can_fit_free_space_bitmap(
      tx->state->global_state.span.size, &new_size));
  ensure(new_size < tx->state->db->options.maximum_size,
         msg("Unable to grow the database beyond the maximum size"),
         with(new_size, "%lu"), with(pages, "%lu"),
         with(tx->state->db->options.maximum_size, "%lu"));
  file_handle_t *handle = tx->state->db->handle;
  ensure(pal_set_file_size(handle, 0, new_size));
  uint64_t from = tx->state->global_state.header.number_of_pages;
  uint64_t to = new_size / PAGE_SIZE;
  span_t new_map = {.size = new_size};
  ensure(pal_mmap(handle, 0, &new_map),
         msg("Unable to map the file again"),
         with(new_map.size, "%lu"));
  {
    size_t cancel_defer = 0;
    try_defer(pal_unmap, new_map, cancel_defer);
    // discard new map if we failed to commit
    ensure(txn_register_cleanup_action(&tx->state->on_rollback,
                                       db_clear_old_mmap, &new_map,
                                       sizeof(span_t)));
    cancel_defer = 1;
  }
  // discard old map when no one is looking at this tx
  ensure(txn_register_cleanup_action(
      &tx->state->on_forget, db_clear_old_mmap,
      &tx->state->global_state.span, sizeof(span_t)));
  tx->state->global_state.span = new_map;
  return db_finalize_file_size_increase(tx, from, to);
}
// end::db_try_increase_file_size[]
