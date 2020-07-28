#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/internal.h>
#include <gavran/test.h>

// tag::tests12[]
describe(using_32_bits) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can start in 32 bits mode") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    options.avoid_mmap_io = 1;
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    assert(db.state->global_state.span.address == 0,
           "No mapping should be generated");
  }

  it("can modify data and restart in 32 bits mode") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    options.avoid_mmap_io = 1;
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t read_tx;
    assert(txn_create(&db, TX_READ, &read_tx));

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, w);
    page_t p = {.number_of_pages = 1};
    page_metadata_t* metadata;
    assert(txn_allocate_page(&w, &p, &metadata, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));

    assert(txn_close(&read_tx));

    assert(db_close(&db));

    assert(db_create("/tmp/db/try", &options, &db));
  }

  it("can use 32 bits and encryption both") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    options.avoid_mmap_io = 1;
    randombytes_buf(options.encryption_key, 32);
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t read_tx;
    assert(txn_create(&db, TX_READ, &read_tx));

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, w);
    page_t p = {.number_of_pages = 1};
    page_metadata_t* metadata;
    assert(txn_allocate_page(&w, &p, &metadata, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));

    assert(txn_close(&read_tx));

    assert(db_close(&db));

    assert(db_create("/tmp/db/try", &options, &db));
  }
}
// end::tests12[]
