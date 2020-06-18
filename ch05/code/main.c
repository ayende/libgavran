#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "defer.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::create_and_use_database[]
static result_t allocate_page_and_use_it() {
  // <1>
  db_t db;
  database_options_t options = {.minimum_size = 128 * 1024};
  ensure(db_create("/tmp/db/orev", &options, &db));
  defer(db_close, &db);

  // <2>
  txn_t tx;
  ensure(txn_create(&db, 0, &tx));
  defer(txn_close, &tx);

  // <3>
  page_t page = {0};
  ensure(txn_allocate_page(&tx, &page, 0));

  // <4>
  printf("New allocated page %lu\n", page.page_num);
  strcpy(page.address, "Hello Gavran");

  // <5>
  ensure(txn_commit(&tx));
  ensure(txn_close(&tx));

  // <6>
  ensure(txn_create(&db, 0, &tx));
  ensure(txn_get_page(&tx, &page));

  printf("%s\n", page.address);

  return success();
}
// end::create_and_use_database[]

int main() {
  if (!allocate_page_and_use_it()) {
    errors_print_all();
  }
  printf("Done\n");
  return 0;
}
