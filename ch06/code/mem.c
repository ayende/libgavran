#include <errno.h>
#include <stdlib.h>

#include "db.h"
#include "errors.h"
#include "platform.mem.h"

// tag::palmem_allocate_pages[]
result_t palmem_allocate_pages(void **p, uint32_t pages) {
  int rc = posix_memalign(p, PAGE_SIZE, PAGE_SIZE * pages);
  if (rc) {
    failed(rc, msg("Unable to allocate memory for page"), with(pages, "%d"));
  }
  return success();
}
// end::palmem_allocate_pages[]

result_t palmem_free_page(void **p) {
  if (!p || !*p)
    return success();
  free(*p);
  *p = 0;
  return success();
}
