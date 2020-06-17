#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include "defer.h"
#include "errors.h"
#include "platform.fs.h"

#define DB_SIZE (128 * 1024)

static result_t create_and_write_file() {
  size_t size;
  ensure(palfs_compute_handle_size("/db/phones", &size));

  file_handle_t* handle = malloc(size);
  // <1>
  ensure(handle);
  defer(free, handle);

  // <2>
  ensure(palfs_create_file("/db/phones", handle));
  defer(palfs_close_file, handle);

  ensure(palfs_set_file_minsize(handle, 128 * 1024));

  // <3>
  struct mmap_args m = {.size = DB_SIZE};
  ensure(palfs_mmap(handle, 0, &m));
  defer(palfs_unmap, &m);

  const char MSG[] = "Hello Gavran!";
  ensure(palfs_write_file(handle, 0, MSG, sizeof(MSG)));

  printf("%s\n", m.address);

  return success();
}

int main() {
  if (!create_and_write_file()) {
    errors_print_all();
  }
  return 0;
}
