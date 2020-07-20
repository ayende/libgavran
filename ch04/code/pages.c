#include <gavran/db.h>

// tag::paging-impl[]
result_t pages_get(txn_t *tx, page_t *p) {
  db_global_state_t *state = &tx->state->global_state;
  uint64_t offset = p->page_num * PAGE_SIZE;
  if (offset + p->number_of_pages * PAGE_SIZE > state->span.size) {
    failed(ERANGE,
           msg("Requests for a page that is outside of the bounds of "
               "the file"),
           with(p->page_num, "%lu"), with(state->span.size, "%lu"));
  }

  p->address = ((char *)state->span.address + offset);
  return success();
}

result_t pages_write(db_state_t *db, page_t *p) {
  ensure(pal_write_file(db->handle, p->page_num * PAGE_SIZE,
                        p->address, PAGE_SIZE * p->number_of_pages),
         msg("Unable to write page"), with(p->page_num, "%lu"));
  return success();
}
// end::paging-impl[]
