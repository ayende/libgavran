#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

// tag::tests10[]

static result_t write_a_lot(db_t* db) {
  for (size_t i = 0; i < 3; i++) {
    txn_t wtx;
    ensure(txn_create(db, TX_WRITE, &wtx));
    defer(txn_close, wtx);
    for (size_t j = 0; j < 14; j++) {
      page_metadata_t* metadata;
      page_t p = {.number_of_pages = 1};
      ensure(txn_allocate_page(&wtx, &p, &metadata, 0));
      metadata->overflow.page_flags = page_flags_overflow;
      metadata->overflow.number_of_pages = 1;
      randombytes_buf(p.address, PAGE_SIZE);
    }
    ensure(txn_commit(&wtx));
  }
  return success();
}

describe(size_growth) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("Can allocate and grow the data file") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    uint64_t old_size = db.state->handle->size;
    assert(write_a_lot(&db));
    uint64_t new_size = db.state->handle->size;

    assert(new_size > old_size);
  }

  it("WAL will stay within the specified limit") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024,
                            .wal_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    uint64_t old_size = db.state->wal_state.files[0].span.size;
    assert(write_a_lot(&db));
    uint64_t new_size = db.state->wal_state.files[0].span.size;

    assert(new_size == old_size);
  }

  it("can grow WAL size") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024,
                            .wal_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_READ, &tx));
    defer(txn_close, tx);

    uint64_t old_size = db.state->wal_state.files[0].span.size;
    assert(write_a_lot(&db));
    uint64_t new_size = db.state->wal_state.files[0].span.size;

    assert(new_size > old_size);
  }

  it("will use both WAL files and reset the size") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024,
                            .wal_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx1;
    assert(txn_create(&db, TX_READ, &tx1));
    defer(txn_close, tx1);

    uint64_t old_size_a = db.state->wal_state.files[0].span.size;
    assert(write_a_lot(&db));  // no checkpoint due to tx1, will grow
    uint64_t new_size_a = db.state->wal_state.files[0].span.size;
    assert(new_size_a > old_size_a);

    // now we have a new transaction prevent complete clear
    txn_t tx2;
    assert(txn_create(&db, TX_READ, &tx2));
    defer(txn_close, tx2);

    // reason to stop checkpointing A is gone
    assert(txn_close(&tx1));
    // should now switch to B
    uint64_t old_size_b = db.state->wal_state.files[1].span.size;
    assert(write_a_lot(&db));  // no checkpoint due to tx2, will grow
    uint64_t new_size_b = db.state->wal_state.files[1].span.size;
    assert(new_size_b > old_size_b);

    assert(txn_close(&tx2));
    // can now switch back to A
    // when this happens, we reset A

    uint64_t newest_size_a = db.state->wal_state.files[0].span.size;
    assert(new_size_a > newest_size_a);
  }
}
// end::tests10[]
