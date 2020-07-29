#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sodium.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

static void rude_shutdown_db(db_t* db) {
  (void)pal_unmap(&db->state->map);
  db->state->map.address = 0;
  (void)pal_close_file(db->state->handle);
  db->state->handle = 0;
  (void)db_close(db);
  // errors are expected here
  errors_clear();
}

// tag::data_loss[]
static result_t write_to_page(db_t* db, const void* msg,
                              size_t size) {
  txn_t wtx;
  ensure(txn_create(db, TX_WRITE, &wtx));
  defer(txn_close, wtx);
  page_t page = {.page_num = 3};
  ensure(txn_raw_modify_page(&wtx, &page));
  memcpy(page.address, msg, size);
  ensure(txn_commit(&wtx));
  return success();
}

static result_t data_loss(const char* path) {
  db_t db;
  db_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create(path, &options, &db));
  // <1>
  txn_t rtx;
  ensure(txn_create(&db, TX_READ, &rtx));
  // <2>
  ensure(write_to_page(&db, "Hello Gavran", strlen("Hello Gavran")));

  // <3>
  rude_shutdown_db(&db);

  // <4>
  ensure(db_create("/tmp/db/try", &options, &db));
  defer(db_close, db);
  ensure(txn_create(&db, TX_READ, &rtx));
  page_t p = {.page_num = 3};
  ensure(txn_raw_get_page(&rtx, &p));
  ensure(strcmp("Hello Gavran", p.address) == 0);
  return success();
}
// end::data_loss[]

// tag::tests08[]
describe(durability_tests) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("closing files does not commit pending transactions") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    txn_t rtx;
    assert(txn_create(&db, TX_READ, &rtx));
    assert(
        write_to_page(&db, "Hello Gavran", strlen("Hello Gavran")));
    assert(db_close(&db));

    // let's mmap the data file and check it all
    file_handle_t* handle;
    assert(pal_create_file("/tmp/db/try", &handle,
                           pal_file_creation_flags_none));
    span_t span = {.size = options.minimum_size};
    assert(pal_mmap(handle, 0, &span));
    defer(pal_unmap, span);
    defer(pal_close_file, handle);

    const char* needle = "Hello Gavran";
    void* found =
        memmem(span.address, span.size, needle, strlen(needle));
    assert(found == 0);
  }

  it("on reopen will recover committed transactions") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    txn_t rtx;
    assert(txn_create(&db, TX_READ, &rtx));
    assert(
        write_to_page(&db, "Hello Gavran", strlen("Hello Gavran")));
    assert(db_close(&db));

    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    // now verify that the data is back after recovery
    assert(txn_create(&db, TX_READ, &rtx));
    page_t p = {.page_num = 3};
    assert(txn_raw_get_page(&rtx, &p));
    assert(strcmp("Hello Gavran", p.address) == 0);
  }

  it("closing db with no active txs requires no recovery") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024,
                            .wal_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    // need to write multiple times to hit 50% WAL
    for (size_t i = 0; i < 4; i++) {
      char rand[PAGE_SIZE];
      randombytes_buf(rand, PAGE_SIZE);
      assert(write_to_page(&db, rand, PAGE_SIZE));
    }
    assert(db.state->wal_state.files[0].last_write_pos == 0);
    assert(db_close(&db));

    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    assert(db.state->wal_state.files[0].last_write_pos == 0);
    assert(db.state->wal_state.files[0].last_tx_id == 0);
  }

  it("should avoid data loss after failure") {
    assert(data_loss("/tmp/db/try"));
  }
}
// end::tests08[]
