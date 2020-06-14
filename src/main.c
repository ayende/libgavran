#include "defer.h"
#include "errors.h"
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <linux/limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// //#include "pal.h"
// //#include "transactions.h"

static result_t
read_all(int fd, size_t size, void* buffer)
{
  size_t read_bytes = 0;
  while (read_bytes < size) {
    ssize_t cur =
      read(fd, (char*)buffer + read_bytes, size - read_bytes);

    if (cur <= 0) {
      failed(errno,
             "Failed to read requested data",
             with(read_bytes, "%zu"),
             with(cur, "%zd"),
             with(size, "%zu"));
    }
    read_bytes += (size_t)cur;
  }
  success();
}

static result_t
read_int_to_str_buffer(const char* path, char** buffer)
{
  int fd = open(path, O_RDONLY, 0);
  if (fd == -1) {
    failed(errno, "Unable to open file", with(path, "%s"));
  }
  defer(close, &fd);

  char* tmp_buf = malloc(128);
  ensure(tmp_buf, msg("Unable to allocate buffer"));
  size_t cancel_defer;
  try_defer(free, buffer, cancel_defer);

  int val;
  ensure(read_all(fd, sizeof(int), &val), with(path, "%s"));

  int chars = sprintf(tmp_buf, "%d", val);
  *buffer = realloc(tmp_buf, (size_t)chars + 1);
  ensure(*buffer, msg("Failed to decrease the buffer size?!"));

  cancel_defer = 1;
  success();
}

int
main()
{
  char* bu;
  if (!read_int_to_str_buffer("test", &bu)) {
    print_all_errors();
  }
  // char b[10];
  // if (!int_to_string(4222, 2, b)) {
  //   print_all_errors();
  //   return 1;
  // }
  // printf("%s\n", b);
  // database_t db;

  // database_options_t options = {0};
  // txn_t tx;
  // page_t page;

  // assert(open_database("/home/ayende/projects/libgavran/db/orev",
  //                      &options, &db));

  // assert(create_transaction(&db, 0, &tx));
  // page.overflow_size = 17000;
  // assert(allocate_page(&tx, &page, 0));

  // printf("New allocated page %lu\n", page.page_num);
  // strcpy(page.address, "Hello Gavran");

  // assert(commit_transaction(&tx));
  // assert(close_transaction(&tx));
  // assert(create_transaction(&db, 0, &tx));
  // assert(get_page(&tx, &page));

  // printf("%s\n", page.address);

  // assert(close_transaction(&tx));
  // assert(close_database(&db));

  return 0;
}
