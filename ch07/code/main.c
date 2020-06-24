#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "defer.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

static result_t write_and_return_rtx(db_t *db, uint32_t i, txn_t *rtx) {
  txn_t wtx;
  ensure(txn_create(db, TX_WRITE, &wtx));
  defer(txn_close, &wtx);
  page_t p = {.page_num = 2};
  ensure(txn_modify_page(&wtx, &p));
  memcpy(p.address, &i, sizeof(uint32_t));

  ensure(txn_commit(&wtx));

  ensure(txn_create(db, TX_READ, rtx));
  return success();
}

static result_t assert_raw(db_t *db, uint32_t expected) {
  void *a = (char *)db->state->mmap.address + (PAGE_SIZE * 2);
  uint32_t existing = *(uint32_t *)a;
  ensure(expected == existing, with(existing, "%d"), with(expected, "%d"));
  return success();
}

// tag::interleaved_transactions[]
static result_t interleaved_transactions() {
  db_t db;
  database_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create("/tmp/db/orev2", &options, &db));
  defer(db_close, &db);

  txn_t t2, t3, t4, t5, t6, t7, t8, t9;
  ensure(write_and_return_rtx(&db, 2, &t2));
  ensure(write_and_return_rtx(&db, 3, &t3));
  ensure(write_and_return_rtx(&db, 4, &t4));
  ensure(write_and_return_rtx(&db, 5, &t5));
  ensure(write_and_return_rtx(&db, 6, &t6));

  ensure(assert_raw(&db, 0)); // no writes

  txn_t noop;
  ensure(txn_create(&db, TX_WRITE, &noop));
  ensure(txn_close(&noop));

  ensure(txn_close(&t2));
  ensure(assert_raw(&db, 2));
  ensure(txn_close(&t3));
  ensure(assert_raw(&db, 3));

  ensure(write_and_return_rtx(&db, 7, &t7));

  ensure(txn_close(&t5));
  ensure(assert_raw(&db, 3));

  ensure(write_and_return_rtx(&db, 8, &t8));

  ensure(txn_close(&t4));
  ensure(assert_raw(&db, 5));

  ensure(write_and_return_rtx(&db, 9, &t9));

  ensure(txn_close(&t9));
  ensure(assert_raw(&db, 5));
  ensure(txn_close(&t6));
  ensure(txn_close(&t7));
  ensure(assert_raw(&db, 7));
  ensure(txn_close(&t8));
  ensure(assert_raw(&db, 9));
  return success();
}
// end::interleaved_transactions[]

// tag::mvcc[]
static result_t mvcc() {
  db_t db;
  database_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create("/tmp/db/orev", &options, &db));
  defer(db_close, &db);

  // <1>
  txn_t wtx;
  ensure(txn_create(&db, TX_WRITE, &wtx));
  defer(txn_close, &wtx);

  page_t page = {.page_num = 2};
  ensure(txn_modify_page(&wtx, &page));

  const char *msg = "Hello Gavran";
  strncpy(page.address, msg, strlen(msg) + 1);

  // <2>
  txn_t rtx;
  ensure(txn_create(&db, TX_READ, &rtx));
  defer(txn_close, &rtx);

  // <3>
  ensure(txn_commit(&wtx));
  ensure(txn_close(&wtx));

  page_t rp = {.page_num = 2};
  ensure(txn_get_page(&rtx, &rp));

  // <4>
  printf("Value: %s\n", rp.address);

  return success();
}
// end::mvcc[]

int main() {
  system("rm  /tmp/db/*");

  if ((1) && !interleaved_transactions()) {
    errors_print_all();
  }
  if ((1) && !mvcc()) {
    errors_print_all();
  }
  printf("Done\n");
  return 0;
}
