#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

// tag::mvcc[]
static result_t allocate_and_write_in_page(txn_t* tx,
                                           uint64_t* page_num) {
  page_t page = {.number_of_pages = 1};
  page_metadata_t* metadata;
  ensure(txn_allocate_page(tx, &page, &metadata, 0));
  metadata->overflow.page_flags = page_flags_overflow;
  metadata->overflow.number_of_pages = 1;
  const char* msg = "Hello Gavran";
  strcpy(page.address, msg);
  *page_num = page.page_num;
  return success();
}

static result_t mvcc(const char* path) {
  db_t db;
  db_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create(path, &options, &db));
  defer(db_close, db);
  // <1>
  txn_t wtx;
  ensure(txn_create(&db, TX_WRITE, &wtx));
  defer(txn_close, wtx);
  uint64_t page_num;
  ensure(allocate_and_write_in_page(&wtx, &page_num));
  // <2>
  txn_t rtx;
  ensure(txn_create(&db, TX_READ, &rtx));
  defer(txn_close, rtx);
  // <3>
  ensure(txn_commit(&wtx));
  ensure(txn_close(&wtx));

  // <4>
  page_t rp = {.page_num = page_num};
  ensure(txn_get_page(&rtx, &rp));
  ensure(*(char*)rp.address == 0,
         msg("Cannot see data from later transactions"));
  return success();
}
// end::mvcc[]

// tag::tests07[]
static result_t write_and_return_read_tx(db_t* db, uint32_t val,
                                         txn_t* rtx) {
  txn_t wtx;
  ensure(txn_create(db, TX_WRITE, &wtx));
  defer(txn_close, wtx);
  page_t p = {.page_num = 3};
  ensure(txn_raw_modify_page(&wtx, &p));
  *(uint32_t*)p.address = val;
  size_t done = 0;
  ensure(txn_commit(&wtx));
  ensure(txn_create(db, TX_READ, rtx));
  try_defer(txn_close, rtx, done);
  ensure(txn_close(&wtx));
  done = 1;
  return success();
}

describe(transaction_tests) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can not see changes from tx after me") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t page_num;
    {
      txn_t wtx;
      assert(txn_create(&db, TX_WRITE, &wtx));
      defer(txn_close, wtx);
      assert(allocate_and_write_in_page(&wtx, &page_num));
      txn_t rtx;
      assert(txn_create(&db, TX_READ, &rtx));
      defer(txn_close, rtx);
      // commit write tx
      assert(txn_commit(&wtx));
      assert(txn_close(&wtx));
      // check previous tx
      page_t rp = {.page_num = page_num};
      assert(txn_get_page(&rtx, &rp));
      assert(*(char*)rp.address == 0);
    }
    // next transaction should read it
    {
      txn_t rtx;
      assert(txn_create(&db, TX_READ, &rtx));
      defer(txn_close, rtx);
      page_t rp = {.page_num = page_num};
      assert(txn_get_page(&rtx, &rp));
      assert(strcmp(rp.address, "Hello Gavran") == 0);
    }
  }

  it("will write to disk if no active tx prevents it") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t page_num;

    {
      txn_t wtx;
      assert(txn_create(&db, TX_WRITE, &wtx));
      defer(txn_close, wtx);
      assert(allocate_and_write_in_page(&wtx, &page_num));
      txn_t rtx;
      assert(txn_create(&db, TX_READ, &rtx));
      defer(txn_close, rtx);
      // commit write tx
      assert(txn_commit(&wtx));
      assert(txn_close(&wtx));

      char* address = db.state->map.address + (page_num * PAGE_SIZE);
      assert(*address == 0);
    }
    // and now it will write to the disk
    {
      char* address = db.state->map.address + (page_num * PAGE_SIZE);
      assert(strcmp(address, "Hello Gavran") == 0);
    }
  }

  it("will NOT write to disk if with read tx active") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t page_num;

    txn_t wtx;
    assert(txn_create(&db, TX_WRITE, &wtx));
    defer(txn_close, wtx);
    assert(allocate_and_write_in_page(&wtx, &page_num));
    assert(txn_commit(&wtx));
    assert(txn_close(&wtx));

    char* address = db.state->map.address + (page_num * PAGE_SIZE);
    assert(strcmp(address, "Hello Gavran") == 0);
  }

  it("mvcc") { assert(mvcc("/tmp/db/try")); }

  it("interleaved transactions") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    txn_t tx1, tx2, tx3, tx4, tx5, tx6, tx7, tx8;

    uint32_t* ptr = db.state->map.address + PAGE_SIZE * 3;

    assert(write_and_return_read_tx(&db, 1, &tx1));
    assert(write_and_return_read_tx(&db, 2, &tx2));
    assert(write_and_return_read_tx(&db, 3, &tx3));
    assert(write_and_return_read_tx(&db, 4, &tx4));
    assert(write_and_return_read_tx(&db, 5, &tx5));

    assert(*ptr == 0);  // nothing changed in the file yet
    assert(txn_close(&tx1));
    assert(*ptr == 1);
    assert(txn_close(&tx2));
    assert(*ptr == 2);  // written as expected

    assert(write_and_return_read_tx(&db, 6, &tx6));

    // reverse order
    assert(txn_close(&tx4));
    assert(*ptr == 2);  // no change
    assert(txn_close(&tx3));
    assert(*ptr == 4);  // write only the latest unused

    assert(write_and_return_read_tx(&db, 7, &tx7));
    assert(write_and_return_read_tx(&db, 8, &tx8));

    assert(txn_close(&tx5));
    assert(txn_close(&tx8));
    assert(txn_close(&tx6));
    assert(*ptr == 6);  // midway
    assert(txn_close(&tx7));
    assert(*ptr == 8);  // done
  }
}
// end::tests07[]
