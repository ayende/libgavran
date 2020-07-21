#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

// tag::tests[]
describe(metadata_tests) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can allocate multiple pages") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);

    page_metadata_t* metadata;
    page_t p1 = {.number_of_pages = 4};
    assert(txn_allocate_page(&tx, &p1, &metadata, 0));
    assert(p1.page_num == 2);

    page_t p2 = {.number_of_pages = 4};
    assert(txn_allocate_page(&tx, &p2, &metadata, 0));
    assert(p2.page_num == 6);
  }

  it("will not allocate on metadata boundary (small)") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1028 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);

    page_metadata_t* metadata;
    page_t p1 = {.number_of_pages = 96};
    assert(txn_allocate_page(&tx, &p1, &metadata, 0));
    assert(p1.page_num == 2);

    page_t p2 = {.number_of_pages = 32};
    assert(txn_allocate_page(&tx, &p2, &metadata, 0));
    assert(p2.page_num == 129);
  }

  it("can allocate very large values") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1028 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);

    page_metadata_t* metadata;
    page_t p1 = {.number_of_pages = 268};
    assert(txn_allocate_page(&tx, &p1, &metadata, 0));
    assert(p1.page_num == 116);

    page_t p2 = {.number_of_pages = 32};
    assert(txn_allocate_page(&tx, &p2, &metadata, 0));
    assert(p2.page_num == 2);
  }

  it("after move to next range will still use existing range") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1028 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);

    page_metadata_t* metadata;
    page_t p1 = {.number_of_pages = 96};
    assert(txn_allocate_page(&tx, &p1, &metadata, 0));
    assert(p1.page_num == 2);

    page_t p2 = {.number_of_pages = 32};
    assert(txn_allocate_page(&tx, &p2, &metadata, 0));
    assert(p2.page_num == 129);

    page_t p3 = {.number_of_pages = 16};
    assert(txn_allocate_page(&tx, &p3, &metadata, 0));
    assert(p3.page_num == 98);
  }

  it("can free and reuse") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1028 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);

    page_metadata_t* metadata;
    page_t p1 = {.number_of_pages = 96};
    assert(txn_allocate_page(&tx, &p1, &metadata, 0));
    assert(p1.page_num == 2);

    page_t p2 = {.number_of_pages = 32};
    assert(txn_allocate_page(&tx, &p2, &metadata, 0));
    metadata->overflow.page_flags = page_flags_overflow;
    metadata->overflow.number_of_pages = 32;
    metadata->overflow.size_of_value = 32 * PAGE_SIZE;
    assert(p2.page_num == 129);
    assert(txn_free_page(&tx, &p2));

    page_t p3 = {.number_of_pages = 268};
    assert(txn_allocate_page(&tx, &p3, &metadata, 0));
    assert(p3.page_num == 116);
  }
}
// end::tests[]
