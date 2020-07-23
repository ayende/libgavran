#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

static void rude_shutdown_db(db_t* db) {
  (void)pal_unmap(&db->state->global_state.span);
  db->state->global_state.span.address = 0;
  (void)pal_close_file(db->state->handle);
  db->state->handle = 0;
  (void)db_close(db);
  // errors are expected here
  errors_clear();
}

static result_t write_to_page(db_t* db) {
  txn_t wtx;
  ensure(txn_create(db, TX_WRITE, &wtx));
  defer(txn_close, wtx);
  page_t page = {.page_num = 3};
  ensure(txn_raw_modify_page(&wtx, &page));
  const char* msg = "Hello Gavran";
  strcpy(page.address, msg);
  ensure(txn_commit(&wtx));
  return success();
}

// tag::tests08[]
describe(durability_tests) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  // tag::data_loss[]
  it("should avoid data loss after failure") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    // <1>
    txn_t rtx;
    assert(txn_create(&db, TX_READ, &rtx));
    // <2>
    assert(write_to_page(&db));

    // <3>
    rude_shutdown_db(&db);

    // <4>
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    assert(txn_create(&db, TX_READ, &rtx));
    page_t p = {.page_num = 3};
    assert(txn_raw_get_page(&rtx, &p));
    assert(strcmp("Hello Gavran", p.address) == 0);
  }
  // end::data_loss[]
}
// end::tests08[]
