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

  it("set out of order") {
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

      {
        btree_val_t set = {.tree_id = tree_id,
            .key                    = {.address = "02", .size = 2},
            .val                    = 2};
        assert(btree_set(&w, &set, 0));
      }
      {
        btree_val_t set = {.tree_id = tree_id,
            .key                    = {.address = "01", .size = 2},
            .val                    = 1};
        assert(btree_set(&w, &set, 0));
      }
      {
        btree_val_t get = {
            .tree_id = tree_id, .key = {.address = "01", .size = 2}};

        assert(btree_get(&w, &get));
        assert(get.has_val);
        assert(get.val == 1);
      }
      {
        btree_val_t get = {
            .tree_id = tree_id, .key = {.address = "02", .size = 2}};

        assert(btree_get(&w, &get));
        assert(get.has_val);
        assert(get.val == 2);
      }
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

      char buffer[5];
      for (uint32_t i = 0; i < 10 * 1000; i++) {
        sprintf(buffer, "%04d", i);
        btree_val_t set = {.tree_id = tree_id,
            .key                    = {.address = buffer, .size = 4},
            .val                    = i};
        assert(btree_set(&w, &set, 0));
        tree_id = set.tree_id;
      }

      for (uint32_t j = 0; j < 10 * 1000; j++) {
        sprintf(buffer, "%04d", j);
        btree_val_t get = {.tree_id = tree_id,
            .key                    = {.address = buffer, .size = 4},
            .val                    = j};
        assert(btree_get(&w, &get));
        if (!get.has_val) {
          assert(btree_get(&w, &get));
        }
        assert(get.has_val);
        assert(get.val == j);
      }

      {
        memset(buffer, 0, 5);
        btree_val_t it = {.tree_id = tree_id,
            .key                   = {.address = "0000", .size = 4}};
        assert(btree_get(&w, &it));
        assert(it.has_val);
        for (uint32_t j = 1; j < 10 * 1000; j++) {
          sprintf(buffer, "%04d", j);
          assert(btree_get_next(&w, &it));
          assert(it.has_val);
          assert(it.val == j);
          assert(memcmp(it.key.address, buffer, 4) == 0);
        }
      }
    }
  }

  it("scan through data") {
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

      for (uint32_t i = 0; i < 10; i++) {
        char buffer[5];
        sprintf(buffer, "%04d", i * 2);
        btree_val_t set = {.tree_id = tree_id,
            .key                    = {.address = buffer, .size = 4},
            .val                    = i * 2};
        assert(btree_set(&w, &set, 0));
        tree_id = set.tree_id;
      }

      {
        char buffer[5];
        sprintf(buffer, "%04d", 5);
        btree_val_t get = {.tree_id = tree_id,
            .key                    = {.address = buffer, .size = 4}};
        assert(btree_get(&w, &get));
        assert(get.has_val == false);
        uint64_t expected = 6;
        for (size_t i = 0; i < 7; i++) {
          assert(btree_get_next(&w, &get));
          assert(get.has_val);
          assert(get.val == expected + i * 2);
        }
        assert(btree_get_next(&w, &get));
        assert(get.has_val == false);  // at end
        get.key.address = buffer;
        get.key.size    = 4;
        assert(btree_get(&w, &get));
        assert(get.has_val == false);
        expected = 4;
        for (size_t i = 0; i < 3; i++) {
          assert(btree_get_prev(&w, &get));
          assert(get.has_val);
          assert(get.val == expected - i * 2);
        }
        assert(btree_get_prev(&w, &get));
        assert(get.has_val == false);  // at start
      }
    }
  }
}
// end::tests16[]
