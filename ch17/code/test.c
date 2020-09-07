#include <byteswap.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/internal.h>
#include <gavran/test.h>

// tag::tests17[]
result_t multiple_vals(uint64_t amount) {
  db_t db;
  db_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create("/tmp/db/try", &options, &db));
  defer(db_close, db);

  {
    txn_t tx;
    ensure(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);
    uint64_t tree_id;
    ensure(btree_create(&tx, &tree_id));

    char* key       = "127.0.0.1";
    btree_val_t set = {.tree_id = tree_id,
        .key = {.address = key, .size = strlen(key)}};
    for (size_t i = 0; i < amount; i++) {
      set.val++;
      ensure(btree_multi_append(&tx, &set));
    }

    uint64_t count    = 0;
    btree_cursor_t it = {.tree_id = tree_id,
        .tx                       = &tx,
        .key = {.address = key, .size = strlen(key)}};
    ensure(btree_multi_cursor_search(&it));
    while (true) {
      ensure(btree_multi_get_next(&it));
      if (it.has_val == false) break;
      count++;
      ensure(count == it.val);
    }
    ensure(count == amount);
  }
  return success();
}

describe(multiple_vals) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("6 multi btree records") { assert(multiple_vals(6)); }

  it("many identical btree records") { assert(multiple_vals(300)); }
}
// end::tests17[]
