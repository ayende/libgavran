#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

// tag::create_and_write_file[]
static result_t create_and_write_file(const char* path) {
  const int DB_SIZE = 128 * 1024;
  // <1>
  file_handle_t* handle;
  ensure(pal_create_file(path, &handle, 0));
  defer(pal_close_file, handle);
  ensure(pal_set_file_size(handle, DB_SIZE, DB_SIZE));
  // <2>
  span_t m = {.size = DB_SIZE};
  ensure(pal_mmap(handle, 0, &m));
  defer(pal_unmap, m);

  // <3>
  db_global_state_t global_state = {.span = m};
  db_state_t db_state = {.global_state = global_state,
                         .handle = handle};
  page_t p = {.page_num = 0};
  txn_state_t txn_state = {.global_state = global_state,
                           .db = &db_state};
  txn_t txn = {.state = &txn_state};

  // <4>
  ensure(pages_get(&txn, &p));
  page_t copy = {.number_of_pages = 1};
  // <5>
  ensure(mem_alloc_page_aligned(&copy.address,
                                copy.number_of_pages * PAGE_SIZE));
  defer(free, copy.address);
  // <6>
  memcpy(copy.address, p.address, PAGE_SIZE);
  strcpy(copy.address, "Hello Gavran!");
  // <7>
  ensure(pages_write(&db_state, &copy));

  // <8>
  ensure(strcmp("Hello Gavran!", p.address) == 0);

  return success();
}
// end::create_and_write_file[]

// tag::create_and_write_with_tx_api[]
static result_t validate_message(db_t* db, bool expected_value) {
  txn_t read_tx;
  ensure(txn_create(db, TX_READ, &read_tx));
  defer(txn_close, read_tx);
  page_t page = {.page_num = 3};
  ensure(txn_raw_get_page(&read_tx, &page));
  bool actual_value_match =
      strcmp("Hello Gavran!", page.address) == 0;
  ensure(actual_value_match == expected_value);
  return success();
}

static result_t create_and_write_with_tx_api(const char* path) {
  db_t db;
  db_options_t options = {.minimum_size = 128 * 1024};
  ensure(db_create(path, &options, &db));
  defer(db_close, db);

  ensure(validate_message(&db, false));  // no value
  {
    txn_t write_tx;
    ensure(txn_create(&db, TX_WRITE, &write_tx));
    defer(txn_close, write_tx);
    page_t page = {.page_num = 3};
    ensure(txn_raw_modify_page(&write_tx, &page));
    strcpy(page.address, "Hello Gavran!");

    ensure(validate_message(&db, false));  // no value after write
    ensure(txn_commit(&write_tx));
    ensure(validate_message(&db, true));  // value there after commit
  }

  ensure(validate_message(&db, true));  // value there after tx_close
  return success();
}
// end::create_and_write_with_tx_api[]

// tag::tests[]
describe(db_basic_tests) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can read and write to files using paging") {
    assert(create_and_write_file("/tmp/db/phones"));
  }

  it("can read and write to files using transactions") {
    assert(create_and_write_with_tx_api("/tmp/db/phones"));
  }

  it("can create db and tx") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024};
    assert(db_create("/tmp/db/phones", &options, &db));
    defer(db_close, db);
    txn_t read_tx;
    assert(txn_create(&db, TX_READ, &read_tx));
    defer(txn_close, read_tx);
  }

  it("can write data") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024};
    assert(db_create("/tmp/db/phones", &options, &db));
    defer(db_close, db);
    {
      txn_t write_tx;
      assert(txn_create(&db, TX_WRITE, &write_tx));
      defer(txn_close, write_tx);
      page_t p = {.page_num = 3};
      assert(txn_raw_modify_page(&write_tx, &p));
      strcpy(p.address, "Hello Gavran");
      assert(txn_commit(&write_tx));
    }
    {
      txn_t read_tx;
      assert(txn_create(&db, TX_READ, &read_tx));
      defer(txn_close, read_tx);
      page_t p = {.page_num = 3};
      assert(txn_raw_get_page(&read_tx, &p));

      assert(strcmp(p.address, "Hello Gavran") == 0);
    }
  }

  it("can persist changes across restarts") {
    {
      db_t db;
      db_options_t options = {.minimum_size = 128 * 1024};
      assert(db_create("/tmp/db/phones", &options, &db));
      defer(db_close, db);
      {
        txn_t write_tx;
        assert(txn_create(&db, TX_WRITE, &write_tx));
        defer(txn_close, write_tx);
        page_t p = {.page_num = 3};
        assert(txn_raw_modify_page(&write_tx, &p));
        strcpy(p.address, "Hello Gavran");
        assert(txn_commit(&write_tx));
      }
    }
    {
      db_t db;
      db_options_t options = {.minimum_size = 128 * 1024};
      assert(db_create("/tmp/db/phones", &options, &db));
      defer(db_close, db);
      {
        txn_t read_tx;
        assert(txn_create(&db, TX_READ, &read_tx));
        defer(txn_close, read_tx);
        page_t p = {.page_num = 3};
        assert(txn_raw_get_page(&read_tx, &p));

        assert(strcmp(p.address, "Hello Gavran") == 0);
      }
    }
  }

  it("without commit, no data is changed") {
    db_t db;
    db_options_t options = {.minimum_size = 128 * 1024};
    assert(db_create("/tmp/db/phones", &options, &db));
    defer(db_close, db);
    {
      txn_t write_tx;
      assert(txn_create(&db, TX_WRITE, &write_tx));
      defer(txn_close, write_tx);
      page_t p = {.page_num = 3};
      assert(txn_raw_modify_page(&write_tx, &p));
      strcpy(p.address, "Hello Gavran");
      // explicitly not committing
      // assert(txn_commit(&write_tx));
    }
    {
      txn_t read_tx;
      assert(txn_create(&db, TX_READ, &read_tx));
      defer(txn_close, read_tx);
      page_t p = {.page_num = 3};
      assert(txn_raw_get_page(&read_tx, &p));

      assert(*(uint64_t*)p.address == 0);
    }
  }
}
// end::tests[]
