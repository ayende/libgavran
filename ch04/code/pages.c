#include <gavran/db.h>

// tag::paging-impl[]
result_t pages_get(txn_t *tx, page_t *p) {
  uint64_t offset = p->page_num * PAGE_SIZE;
  if (offset + p->number_of_pages * PAGE_SIZE > tx->state->map.size) {
    failed(ERANGE,
           msg("Requests for a page that is outside of the bounds of "
               "the file"),
           with(p->page_num, "%lu"),
           with(tx->state->map.size, "%lu"));
  }

  p->address = (tx->state->map.address + offset);
  return success();
}

result_t pages_write(db_state_t *db, page_t *p) {
  ensure(pal_write_file(db->handle, p->page_num * PAGE_SIZE,
                        p->address, PAGE_SIZE * p->number_of_pages),
         msg("Unable to write page"), with(p->page_num, "%lu"));
  return success();
}
// end::paging-impl[]
