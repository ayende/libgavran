#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

#include "test.config.h"

// tag::tests06[]
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

    page_t p1 = {.number_of_pages = 4};
    assert(txn_allocate_page(&tx, &p1, 0));
    assert(p1.page_num == FIRST_USABLE_PAGE);

    page_t p2 = {.number_of_pages = 4};
    assert(txn_allocate_page(&tx, &p2, 0));
    assert(p2.page_num == FIRST_USABLE_PAGE + 4);
  }

  it("will not allocate on metadata boundary (small)") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1028 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);

    page_t p1 = {.number_of_pages = 96};
    assert(txn_allocate_page(&tx, &p1, 0));
    assert(p1.page_num == FIRST_USABLE_PAGE);

    page_t p2 = {.number_of_pages = 32};
    assert(txn_allocate_page(&tx, &p2, 0));
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

    page_t p1 = {.number_of_pages = 268};
    assert(txn_allocate_page(&tx, &p1, 0));
    assert(p1.page_num == 116);

    page_t p2 = {.number_of_pages = 32};
    assert(txn_allocate_page(&tx, &p2, 0));
    assert(p2.page_num == FIRST_USABLE_PAGE);
  }

  it("after move to next range will still use existing range") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1028 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);

    page_t p1 = {.number_of_pages = 96};
    assert(txn_allocate_page(&tx, &p1, 0));
    assert(p1.page_num == FIRST_USABLE_PAGE);

    page_t p2 = {.number_of_pages = 32};
    assert(txn_allocate_page(&tx, &p2, 0));
    assert(p2.page_num == 129);

    page_t p3 = {.number_of_pages = 16};
    assert(txn_allocate_page(&tx, &p3, 0));
    assert(p3.page_num == FIRST_USABLE_PAGE + 96);
  }

  it("can free and reuse") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1028 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);

    page_t p1 = {.number_of_pages = 96};
    assert(txn_allocate_page(&tx, &p1, 0));
    assert(p1.page_num == FIRST_USABLE_PAGE);

    page_t p2 = {.number_of_pages = 32};
    assert(txn_allocate_page(&tx, &p2, 0));
    p2.metadata->overflow.page_flags      = page_flags_overflow;
    p2.metadata->overflow.number_of_pages = 32;
    p2.metadata->overflow.size_of_value   = 32 * PAGE_SIZE;
    assert(p2.page_num == 129);
    assert(txn_free_page(&tx, &p2));

    page_t p3 = {.number_of_pages = 268};
    assert(txn_allocate_page(&tx, &p3, 0));
    assert(p3.page_num == 116);
  }
}
// end::tests06[]
