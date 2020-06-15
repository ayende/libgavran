#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "defer.h"
#include "errors.h"
#include "paging.h"
#include "platform.fs.h"
#include "platform.mem.h"

#define DB_SIZE (128 * 1024)

static result_t create_and_write_file() {
  size_t size;
  ensure(palfs_compute_handle_size("/db/phones", &size));

  file_handle_t* handle = malloc(size);
  ensure(handle);
  defer(free, handle);

  ensure(palfs_create_file("/db/phones", handle));
  defer(palfs_close_file, handle);
  ensure(palfs_set_file_minsize(handle, 128 * 1024));

  struct mmap_args m = {.size = DB_SIZE};
  ensure(palfs_mmap(handle, 0, &m));
  defer(palfs_unmap, &m);

  // <1>
  txn_t tx = {0};
  page_t p = {.page_num = 0};
  // <2>
  ensure(pages_get(&tx, &p));
  page_t copy = {.num_of_pages = 1, .page_num = 0};
  // <3>
  ensure(txn_allocate_page(&tx, &copy));
  // <4>
  memcpy(copy.address, p.address, PAGE_SIZE);
  // <5>
  const char MSG[] = "Hello Gavran!";
  memcpy(copy.address, MSG, sizeof(MSG));
  // <6>
  ensure(pages_write(&tx, &copy));

  // <7>
  printf("%s\n", p.address);

  success();
}

int main() {
  if (!create_and_write_file()) {
    errors_print_all();
  }
  return 0;
}
