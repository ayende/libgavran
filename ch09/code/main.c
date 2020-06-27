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
  // <1>
  txn_t rtx;
  ensure(txn_create(&db, TX_READ, &rtx));
  page_t rp = {.page_num = 2};
  ensure(txn_get_page(&rtx, &rp));
  printf("Value: %s\n", rp.address);
  // <2>
  txn_t wtx;
  ensure(txn_create(&db, TX_WRITE, &wtx));
  defer(txn_close, &wtx);
  page_t page = {.page_num = 2};
  ensure(txn_modify_page(&wtx, &page));
  const char *msg = "Hello Gavran";
  strncpy(page.address, msg, strlen(msg) + 1);

  ensure(txn_commit(&wtx));
  ensure(txn_close(&wtx));

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
