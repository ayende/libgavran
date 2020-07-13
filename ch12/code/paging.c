#include <errno.h>
#include <sys/param.h>

#include "impl.h"
#include "platform.mem.h"

result_t pages_get(txn_t *tx, page_t *p) {
  db_global_state_t *state = &tx->state->global_state;
  uint64_t offset = p->page_num * PAGE_SIZE;
  if (offset + PAGE_SIZE > state->mmap.size) {
    failed(ERANGE,
           msg("Requests for a page that is outside of the bounds of "
               "the file"),
           with(p->page_num, "%lu"), with(state->mmap.size, "%lu"));
  }

  if (tx->state->db->options.avoid_mmap_io) {
    void *buffer;
    uint32_t pages = MAX(1, size_to_pages(p->overflow_size));
    ensure(palmem_allocate_pages(&buffer, pages));
    size_t cancel_defer = 0;
    try_defer(free, buffer, cancel_defer);
    ensure(palfs_read_file(tx->state->db->handle, PAGE_SIZE * p->page_num,
                           buffer, pages * PAGE_SIZE));
    p->address = buffer;
    ensure(hash_put_new(&tx->working_set, p));
    cancel_defer = 1;
    return success();
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
