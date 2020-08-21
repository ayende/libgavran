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

// tag::tests16[]
static result_t create_btree(db_t* db, uint64_t* tree_id) {
  txn_t w;
  ensure(txn_create(db, TX_WRITE, &w));
  defer(txn_close, w);
  ensure(btree_create(&w, tree_id));
  ensure(txn_commit(&w));
  return success();
}

describe(btree) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can set and get item from tree") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t tree_id;
    assert(create_btree(&db, &tree_id));

    {
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);

      char* msg       = "John";
      btree_val_t set = {.tree_id = tree_id,
          .key = {.address = msg, .size = strlen(msg)},
          .val = 5};
      assert(btree_set(&w, &set, 0));

      btree_val_t get = {.tree_id = tree_id,
          .key = {.address = msg, .size = strlen(msg)}};

      assert(btree_get(&w, &get));
      assert(get.has_val);
      assert(get.val == 5);
    }
  }

  it("write enough to split page") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t tree_id;
    assert(create_btree(&db, &tree_id));

    {
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);

      for (uint32_t i = 0; i < 1024; i++) {
        char buffer[5];
        sprintf(buffer, "%04d", i);
        btree_val_t set = {.tree_id = tree_id,
            .key                    = {.address = buffer, .size = 4},
            .val                    = i};
        assert(btree_set(&w, &set, 0));
        tree_id = set.tree_id;
      }

      for (uint32_t j = 0; j < 1024; j++) {
        char buffer[5];
        sprintf(buffer, "%04d", j);
        btree_val_t get = {.tree_id = tree_id,
            .key                    = {.address = buffer, .size = 4},
            .val                    = j};
        assert(btree_get(&w, &get));
        assert(get.has_val);
        assert(get.val == j);
      }
    }
  }
}
// end::tests16[]
