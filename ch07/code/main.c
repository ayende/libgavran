#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "defer.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::mvcc[]
static result_t mvcc() {
  db_t db;
  database_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create("/tmp/db/orev", &options, &db));
  defer(db_close, &db);

  txn_t wtx;
  ensure(txn_create(&db, WRITE_TX, &wtx));
  defer(txn_close, &wtx);

  page_t page = {.page_num = 2};
  ensure(txn_modify_page(&wtx, &page));

  const char *msg = "Hello Gavran";
  strncpy(page.address, msg, strlen(msg) + 1);

  txn_t rtx;
  ensure(txn_create(&db, READ_TX, &rtx));
  defer(txn_close, &rtx);

  ensure(txn_commit(&wtx));
  ensure(txn_close(&wtx));

  page_t rp = {.page_num = 2};
  ensure(txn_get_page(&rtx, &rp));

  printf("Value: %s\n", rp.address);

  return success();
}
// end::mvcc[]

int main() {
  if (!mvcc()) {
    errors_print_all();
  }
  printf("Done\n");
  return 0;
}
