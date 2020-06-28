#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "defer.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::data_loss[]
static result_t data_loss() {
  db_t db;
  database_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create("/tmp/db/orev", &options, &db));
  defer(db_close, &db);

  for (size_t x = 0; x < 3; x++) {
    txn_t wtx;
    ensure(txn_create(&db, TX_WRITE, &wtx));
    defer(txn_close, &wtx);
    for (size_t i = 0; i < 4; i++) {
      page_t page = {.page_num = 2 + i};
      ensure(txn_modify_page(&wtx, &page));
      const char *msg = "Hello Gavran";
      strncpy(page.address, msg, strlen(msg) + 1);
    }

    ensure(txn_commit(&wtx));
    ensure(txn_close(&wtx));
  }

  ensure(db_close(&db));
  ensure(db_create("/tmp/db/orev", &options, &db));
  return success();
}
// end::data_loss[]

int main() {
  system("rm  /tmp/db/*");

  if (!data_loss()) {
    errors_print_all();
  }
  if (!data_loss()) {
    errors_print_all();
  }
  printf("Done\n");
  return 0;
}
