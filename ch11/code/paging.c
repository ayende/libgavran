#include <errno.h>

#include "impl.h"

result_t pages_get(db_global_state_t *state, page_t *p) {
  uint64_t offset = p->page_num * PAGE_SIZE;
  if (offset + PAGE_SIZE > state->mmap.size) {
    failed(ERANGE,
           msg("Requests for a page that is outside of the bounds of "
               "the file"),
           with(p->page_num, "%lu"), with(state->mmap.size, "%lu"));
  }
  p->address = ((char *)state->mmap.address + offset);
  return success();
}

result_t pages_write(db_state_t *db, page_t *p) {
  size_t pages =
      p->overflow_size / PAGE_SIZE + (p->overflow_size % PAGE_SIZE ? 1 : 0);
  ensure(palfs_write_file(db->handle, p->page_num * PAGE_SIZE, p->address,
                          PAGE_SIZE * pages),
         msg("Unable to write page"), with(p->page_num, "%lu"));
  return success();
}
