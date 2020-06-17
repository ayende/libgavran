#include <errno.h>

#include "impl.h"

result_t pages_get(db_state_t* db, page_t* p) {
  uint64_t offset = p->page_num * PAGE_SIZE;
  if (offset + PAGE_SIZE > db->mmap.size) {
    failed(ERANGE,
           msg("Requests for a page that is outside of the bounds of "
               "the file"),
           with(p->page_num, "%lu"), with(db->mmap.size, "%lu"));
  }
  p->address = ((char*)db->mmap.address + offset);
  return success();
}

result_t pages_write(db_state_t* db, page_t* p) {
  ensure(palfs_write_file(db->handle, p->page_num * PAGE_SIZE,
                          p->address, PAGE_SIZE),
         msg("Unable to write page"), with(p->page_num, "%lu"));
  return success();
}
