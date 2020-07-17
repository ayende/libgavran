#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "defer.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::create_and_write_file[]
#define DB_SIZE (128 * 1024)

static result_t create_and_write_file() {
  size_t size;
  ensure(palfs_compute_handle_size("/tmp/db/phones", &size));

  file_handle_t* handle = malloc(size);
  ensure(handle);
  defer(free, handle);

  ensure(palfs_create_file("/tmp/db/phones", handle));
  defer(palfs_close_file, handle);
  ensure(palfs_set_file_minsize(handle, 128 * 1024));

  struct mmap_args m = {.size = DB_SIZE};
  ensure(palfs_mmap(handle, 0, &m));
  defer(palfs_unmap, &m);

  // <1>
  db_state_t state = {.mmap = m, .handle = handle};
  page_t p = {.page_num = 0};
  // <2>
  ensure(pages_get(&state, &p));
  page_t copy = {.num_of_pages = 1, .page_num = 0};
  // <3>
  ensure(palmem_allocate_pages(&copy));
  // <4>
  memcpy(copy.address, p.address, PAGE_SIZE);
  // <5>
  const char MSG[] = "Hello Gavran files API!";
  memcpy(copy.address, MSG, sizeof(MSG));
  // <6>
  ensure(pages_write(&state, &copy));

  // <7>
  printf("%s\n", p.address);

  return success();
}
// end::create_and_write_file[]

// tag::create_and_use_database[]
// <1>
static result_t print_msg(db_t* db) {
  txn_t read_tx;
  ensure(txn_create(db, 0, &read_tx));
  defer(txn_close, &read_tx);
  page_t page = {.page_num = 0};
  ensure(txn_get_page(&read_tx, &page));

  printf("%s\n", page.address);
  return success();
}

static result_t create_and_use_database() {
  db_t db;
  database_options_t options = {.minimum_size = DB_SIZE};
  ensure(db_create("/tmp/db/phones", &options, &db));
  defer(db_close, &db);

  // <2>
  txn_t write_tx;
  ensure(txn_create(&db, 0, &write_tx));
  defer(txn_close, &write_tx);

  page_t page = {.page_num = 0};
  ensure(txn_modify_page(&write_tx, &page));

  strncpy(page.address, "Hello Gavran TX API!", PAGE_SIZE);

  // <3>
  ensure(print_msg(&db));

  ensure(txn_commit(&write_tx));

  // <4>
  ensure(print_msg(&db));

  return success();
}
// end::create_and_use_database[]

int main() {
  if (!create_and_write_file()) {
    errors_print_all();
  }
  if (!create_and_use_database()) {
    errors_print_all();
  }
  printf("Done\n");
  return 0;
}
