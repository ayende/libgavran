#include <errno.h>
#include <stdlib.h>

#include "db.h"
#include "errors.h"
#include "platform.mem.h"

// tag::palmem_allocate_pages[]
result_t palmem_allocate_pages(page_t *p) {
  int rc = posix_memalign(&p->address, PAGE_SIZE,
                          p->num_of_pages * PAGE_SIZE);
  if (rc) {
    failed(rc, msg("Unable to allocate memory for page"),
           with(p->num_of_pages, "%lu"));
  }
  return success();
}
// end::palmem_allocate_pages[]

result_t palmem_free_page(page_t *p) {
  if (!p->address) return success();
  free(p->address);
  p->address = 0;
  return success();
}