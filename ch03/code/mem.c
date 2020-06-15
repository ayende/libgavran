#include <errno.h>
#include <stdlib.h>

#include "db.h"
#include "errors.h"
#include "paging.h"
#include "platform.mem.h"

// tag::palmem_allocate_pages[]
result_t palmem_allocate_pages(size_t num_of_pages, void** address) {
  if (!posix_memalign(address, PAGE_SIZE, num_of_pages * PAGE_SIZE)) {
    failed(ENOMEM, "Unable to allocate memory for page",
           with(num_of_pages, "%lu"));
  }
  success();
}
// end::palmem_allocate_pages[]