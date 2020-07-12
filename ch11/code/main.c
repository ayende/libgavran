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
static result_t data_loss(uint8_t *s) {
  db_t db;
  database_options_t options = {.minimum_size = 4 * 1024 * 1024};
  memcpy(options.encryption_key, s, 32);
  ensure(db_create("/tmp/db/orev", &options, &db));
  defer(db_close, &db);

  txn_t wtx;
  ensure(txn_create(&db, TX_WRITE, &wtx));
  defer(txn_close, &wtx);
  for (size_t i = 0; i < 4; i++) {
    page_t page = {.overflow_size = PAGE_SIZE};
    ensure(txn_allocate_page(&wtx, &page, 0));

    const char *msg = "Hello Gavran";
    strncpy(page.address, msg, strlen(msg) + 1);
  }

  ensure(txn_commit(&wtx));
  ensure(txn_close(&wtx));

  ensure(db_close(&db));
  ensure(db_create("/tmp/db/orev", &options, &db));
  {
    txn_t rtx;
    ensure(txn_create(&db, TX_READ, &rtx));
    defer(txn_close, &rtx);
    page_t p = {.page_num = 3};
    ensure(txn_get_page(&rtx, &p));
    printf("%s\n", p.address);
  }
  return success();
}
// end::data_loss[]

int main() {

  system("rm  /tmp/db/*");
  uint8_t s[32];
  randombytes_buf(s, 32);
  if (!data_loss(s)) {
    errors_print_all();
  }
  //  randombytes_buf(s, 32);
  if (!data_loss(s)) {
    errors_print_all();
  }
  printf("Done\n");
  return 0;
}
