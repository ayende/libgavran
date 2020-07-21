#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

// tag::allocate_page_and_use_it[]
static result_t allocate_page_and_use_it(const char* path) {
  // <1>
  db_t db;
  db_options_t options = {.minimum_size = 128 * 1024};
  ensure(db_create(path, &options, &db));
  defer(db_close, db);

  // <2>
  txn_t tx;
  ensure(txn_create(&db, TX_WRITE, &tx));
  defer(txn_close, tx);

  // <3>
  page_metadata_t* ignored;
  page_t page = {0};
  ensure(txn_allocate_page(&tx, &page, &ignored, 0));

  // <4>
  strcpy(page.address, "Hello Gavran");

  // <5>
  ensure(txn_commit(&tx));
  ensure(txn_close(&tx));

  // <6>
  ensure(txn_create(&db, 0, &tx));
  ensure(txn_raw_get_page(&tx, &page));

  ensure(strcmp("Hello Gavran", page.address) == 0);

  return success();
}
// end::allocate_page_and_use_it[]

// tag::tests[]
describe(allocation_tests) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can allocate, write and then read data") {
    assert(allocate_page_and_use_it("/tmp/db/try"));
  }

  it("can allocate pages") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);
    page_metadata_t* ignored;
    page_t p = {.number_of_pages = 1};
    assert(txn_allocate_page(&tx, &p, &ignored, 0));
    assert(p.page_num == 2);
    assert(txn_allocate_page(&tx, &p, &ignored, 0));
    assert(p.page_num == 3);
  }

  it("can allocate until space runs out") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024,
                            .maximum_size = 128 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    page_metadata_t* ignored;
    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);
    for (size_t i = 0; i < 14; i++) {
      page_t p = {.number_of_pages = 1};
      assert(txn_allocate_page(&tx, &p, &ignored, 0));
    }

    {
      page_t p = {.number_of_pages = 1};
      assert(!txn_allocate_page(&tx, &p, &ignored, 0));  // failed
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

    page_metadata_t* metadata;
    txn_t tx;
    assert(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);
    page_t p = {.number_of_pages = 1};
    assert(txn_allocate_page(&tx, &p, &metadata, 0));
    metadata->overflow.page_flags = page_flags_overflow;
    metadata->overflow.number_of_pages = 1;
    metadata->overflow.size_of_value = 8000;

    uint64_t page_num = p.page_num;

    assert(txn_free_page(&tx, &p));

    assert(txn_allocate_page(&tx, &p, &metadata, 0));
    assert(p.page_num == page_num);  // page is reused
  }
}
// end::tests[]
