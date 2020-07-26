#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

// tag::tests09[]

static result_t write_to_wal_only(const char* path,
                                  uint64_t* last_wal_position) {
  db_t db;
  db_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create(path, &options, &db));
  defer(db_close, db);

  txn_t rtx;
  ensure(txn_create(&db, TX_READ, &rtx));
  //  intentionally leaking to prevent data file writes
  // defer(txn_close, rtx);
  txn_t wtx;
  ensure(txn_create(&db, TX_WRITE, &wtx));
  defer(txn_close, wtx);
  for (size_t i = 3; i < 10; i++) {
    page_t p = {.page_num = i};
    ensure(txn_raw_modify_page(&wtx, &p));
    // random data prevents compression
    randombytes_buf(p.address, PAGE_SIZE);
  }
  ensure(txn_commit(&wtx));
  *last_wal_position = db.state->wal_state.files[0].last_write_pos;
  return success();
}

static result_t assert_no_content(const char* path) {
  db_t db;
  db_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create(path, &options, &db));
  defer(db_close, db);
  txn_t rtx;
  ensure(txn_create(&db, TX_READ, &rtx));
  defer(txn_close, rtx);
  char zero[PAGE_SIZE] = {0};
  for (size_t i = 3; i < 10; i++) {
    page_t p = {.page_num = i};
    ensure(txn_raw_get_page(&rtx, &p));
    ensure(memcmp(zero, p.address, PAGE_SIZE) == 0);
  }
  return success();
}

describe(diff_and_compression) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("corruption of WAL will not apply transactions") {
    uint64_t last_wal_pos;
    assert(write_to_wal_only("/tmp/db/try", &last_wal_pos));
    // corrupt the WAL
    file_handle_t* handle;
    assert(pal_create_file("/tmp/db/try-a.wal", &handle,
                           pal_file_creation_flags_none));
    char* buffer = "snow";
    assert(pal_write_file(handle, last_wal_pos - 12, buffer, 4));
    assert(pal_close_file(handle));

    assert(assert_no_content("/tmp/db/try"));
  }

  it("truncation of WAL will skip transactions") {
    uint64_t last_wal_pos;
    assert(write_to_wal_only("/tmp/db/try", &last_wal_pos));
    // truncate the WAL
    file_handle_t* handle;
    assert(pal_create_file("/tmp/db/try-a.wal", &handle,
                           pal_file_creation_flags_none));
    assert(pal_set_file_size(handle, 0, last_wal_pos - 10000));
    assert(pal_close_file(handle));

    assert(assert_no_content("/tmp/db/try"));
  }

  it("will compress transactions") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t wtx;
    assert(txn_create(&db, TX_WRITE, &wtx));
    defer(txn_close, wtx);
    uint64_t initial = db.state->wal_state.files[0].last_write_pos;
    for (size_t i = 3; i < 10; i++) {
      page_t p = {.page_num = i};
      assert(txn_raw_modify_page(&wtx, &p));
      memset(p.address, 'a' + (char)i, PAGE_SIZE);
    }
    assert(txn_commit(&wtx));

    uint64_t final = db.state->wal_state.files[0].last_write_pos;
    assert((final - initial) == PAGE_SIZE);
  }

  it("will diff data") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    {  // write random data, cannot be compressed
      txn_t wtx;
      assert(txn_create(&db, TX_WRITE, &wtx));
      defer(txn_close, wtx);
      for (size_t i = 3; i < 10; i++) {
        page_t p = {.page_num = i};
        assert(txn_raw_modify_page(&wtx, &p));
        randombytes_buf(p.address, PAGE_SIZE);
      }
      assert(txn_commit(&wtx));
    }
    uint64_t initial = db.state->wal_state.files[0].last_write_pos;

    {
      txn_t wtx;
      assert(txn_create(&db, TX_WRITE, &wtx));
      defer(txn_close, wtx);
      for (size_t i = 3; i < 10; i++) {
        page_t p = {.page_num = i};
        assert(txn_raw_modify_page(&wtx, &p));
        strcpy(p.address, "Hello Gavran");
      }
      assert(txn_commit(&wtx));
    }

    uint64_t final = db.state->wal_state.files[0].last_write_pos;
    // if we didn't diff the data, the random values
    // will ensure that we can't compress well, so we
    // test that diff works in this manner
    assert((final - initial) == PAGE_SIZE);
  }
}
// end::tests09[]
