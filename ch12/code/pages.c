#include <gavran/db.h>
#include <gavran/internal.h>

// tag::pages_get[]
result_t pages_get(txn_t *tx, page_t *p) {
  uint64_t offset = p->page_num * PAGE_SIZE;
  if (offset + p->number_of_pages * PAGE_SIZE > tx->state->map.size) {
    failed(ERANGE,
           msg("Requests for a page that is outside of the bounds of "
               "the file"),
           with(p->page_num, "%lu"),
           with(tx->state->map.size, "%lu"));
  }

  // <1>
  if (!(tx->state->flags & db_flags_avoid_mmap_io)) {
    p->address = (tx->state->map.address + offset);
    return success();
  }
  // <2>
  void *buffer;
  uint64_t pages = MAX(1, p->number_of_pages);
  ensure(mem_alloc_page_aligned(&buffer, pages * PAGE_SIZE));
  size_t cancel_defer = 0;
  try_defer(free, buffer, cancel_defer);
  // <3>
  ensure(pal_read_file(tx->state->db->handle, PAGE_SIZE * p->page_num,
                       buffer, pages * PAGE_SIZE));
  // <4>
  p->address = buffer;
  ensure(hash_put_new(&tx->working_set, p));
  cancel_defer = 1;
  return success();
}
// end::pages_get[]

result_t pages_write(db_state_t *db, page_t *p) {
  ensure(pal_write_file(db->handle, p->page_num * PAGE_SIZE,
                        p->address, PAGE_SIZE * p->number_of_pages),
         msg("Unable to write page"), with(p->page_num, "%lu"));
  return success();
}
