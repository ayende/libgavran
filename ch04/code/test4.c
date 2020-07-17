#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "db.h"
#include "defer.h"
#include "errors.h"
#include "impl.h"
#include "pal.h"

#define SNOW_ENABLED
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-prototypes"
#pragma clang diagnostic ignored "-Wpadded"
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
#pragma clang diagnostic ignored "-Wmissing-variable-declarations"
#pragma clang diagnostic ignored "-Wformat-nonliteral"
#pragma clang diagnostic ignored "-Wstrict-prototypes"
#include "snow.h"

static result_t create_and_write_file(const char* path) {
  const int DB_SIZE = 128 * 1024;
  file_handle_t* handle;
  ensure(pal_create_file(path, &handle, 0));
  defer(pal_close_file, handle);
  ensure(pal_set_file_size(handle, DB_SIZE, DB_SIZE));

  span_t m = {.size = DB_SIZE};
  ensure(pal_mmap(handle, 0, &m));
  defer(pal_unmap, m);

  db_global_state_t global_state = {.span = m};
  db_state_t db_state = {.global_state = global_state,
                         .handle = handle};
  page_t p = {.page_num = 0};
  txn_state_t txn_state = {.global_state = global_state};
  txn_t txn = {.state = &txn_state};
  ensure(pages_get(&txn, &p));
  page_t copy = {.size = PAGE_SIZE};
  ensure(mem_alloc_page_aligned(&copy.address, copy.size));
  defer(free, copy.address);
  memcpy(copy.address, p.address, PAGE_SIZE);
  strcpy(copy.address, "Hello Gavran!");
  ensure(pages_write(&db_state, &copy));

  ensure(strcmp("Hello Gavran!", p.address) == 0);

  return success();
}

describe(db_basic_tests) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can read and write to files using paging") {
    assert(create_and_write_file("/tmp/db/phones"));
  }
}
