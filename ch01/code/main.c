#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "defer.h"
#include "errors.h"

enable_defer_imp(close, -1, *(int*), "%d");

static result_t read_all(int fd, size_t size, void* buffer) {
  size_t read_bytes = 0;
  while (read_bytes < size) {
    ssize_t cur =
        read(fd, (char*)buffer + read_bytes, size - read_bytes);

    if (cur <= 0) {
      failed(errno, msg("Failed to read requested data"),
             with(read_bytes, "%zu"), with(cur, "%zd"),
             with(size, "%zu"));
    }
    read_bytes += (size_t)cur;
  }
  return success();
}

// tag::read_int_to_str_buffer[]
// <1>
static result_t read_int_to_str_buffer(const char* path,
                                       char** buffer) {
  int fd = open(path, O_RDONLY, 0);
  if (fd == -1) {
    // <2>
    failed(errno, msg("Unable to open file"), with(path, "%s"));
  }
  // <3>
  defer(close, &fd);

  char* tmp_buf = malloc(128);
  // <4>
  ensure(tmp_buf, msg("Unable to allocate buffer"));

  size_t cancel_defer;
  // <3>
  try_defer(free, tmp_buf, cancel_defer);

  int val;
  ensure(read_all(fd, sizeof(int), &val), with(path, "%s"));

  int chars = sprintf(tmp_buf, "%d", val);
  *buffer = realloc(tmp_buf, (size_t)chars + 1);
  ensure(*buffer, msg("Failed to decrease the buffer size?!"));

  cancel_defer = 1;
  // <5>
  return success();
}
// end::read_int_to_str_buffer[]

int main() {
  char* b;
  if (!read_int_to_str_buffer("/tmp", &b)) {
    errors_print_all();
  }
  return 1;
}
