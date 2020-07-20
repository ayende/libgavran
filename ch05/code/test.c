#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

// tag::tests[]
describe(db_basic_tests) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can allocate pages") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);
    page_t p = {.size = PAGE_SIZE};
    assert(txn_allocate_page(&tx, &p, 0));
    assert(p.page_num == 2);
    assert(txn_allocate_page(&tx, &p, 0));
    assert(p.page_num == 3);
  }

  it("can allocate until space runs out") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024,
                            .maximum_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);
    for (size_t i = 0; i < 14; i++) {
      page_t p = {.size = PAGE_SIZE};
      assert(txn_allocate_page(&tx, &p, 0));
    }

    {
      page_t p = {.size = PAGE_SIZE};
      assert(!txn_allocate_page(&tx, &p, 0));  // failed
      size_t count;
      int* codes = errors_get_codes(&count);
      assert(count > 0);
      assert(ENOSPC == codes[0]);
    }
  }

  it("can allocate, free and allocate again") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);
    page_t p = {.size = PAGE_SIZE};
    assert(txn_allocate_page(&tx, &p, 0));
    uint64_t page_num = p.page_num;

    assert(txn_free_page(&tx, &p));

    assert(txn_allocate_page(&tx, &p, 0));
    assert(p.page_num == page_num);  // page is reused
  }
}
// end::tests[]
