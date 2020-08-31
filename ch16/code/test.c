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

// tag::get_last_entries[]
typedef void (*callback_t)(uint64_t v, void* state);

static void count_items(uint64_t v, void* state) {
  (void)v;
  (*((uint32_t*)state))++;
}

static result_t get_last_entries(db_t* db, uint64_t tree_id,
    time_t time, callback_t callback, void* state) {
  txn_t tx;
  ensure(txn_create(db, TX_READ, &tx));
  defer(txn_close, tx);
  uint8_t buffer[10];
  // <1>
  uint64_t start = (uint64_t)time;
  varint_encode(start, buffer);
  btree_cursor_t cursor = {
      .key = {.address = buffer, .size = varint_get_length(start)},
      .tree_id = tree_id,
      .tx      = &tx,
  };
  // <2>
  ensure(btree_cursor_search(&cursor));
  // <3>
  defer(btree_free_cursor, cursor);
  while (true) {
    // <4>
    ensure(btree_get_next(&cursor));
    // <5>
    if (cursor.has_val == false) break;
    callback(cursor.val, state);
  }
  return success();
}
// end::get_last_entries[]

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

  it("records over time") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t tree_id;
    assert(create_btree(&db, &tree_id));
    time_t yesterday = (time(0) - 24 * 60 * 60);

    {
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);
      uint8_t buffer[10];

      for (size_t i = 0; i < 24; i++) {
        uint64_t t = ((uint64_t)yesterday + (60 * 60 * i));
        varint_encode(t, buffer);
        btree_val_t set = {.tree_id = tree_id,
            .key = {.address = buffer, .size = varint_get_length(t)},
            .val = (uint64_t)i};
        assert(btree_set(&w, &set, 0));
      }

      assert(txn_commit(&w));
    }
    uint32_t count         = 0;
    time_t three_hours_ago = yesterday + (21 * 60 * 60);
    assert(get_last_entries(
        &db, tree_id, three_hours_ago, count_items, &count));
    assert(count == 3);
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
      char buffer[5];
      for (int i = 2048 - 1; i >= 0; i--) {
        sprintf(buffer, "%04d", i);
        btree_val_t set = {.tree_id = tree_id,
            .key                    = {.address = buffer, .size = 4},
            .val                    = (uint64_t)i};
        assert(btree_set(&w, &set, 0));
        tree_id = set.tree_id;
      }

      {
        memset(buffer, 0, 5);
        btree_cursor_t it = {
            .tree_id = tree_id,
            .tx      = &w,
        };
        assert(btree_cursor_at_start(&it));
        defer(btree_free_cursor, it);
        for (uint32_t j = 0; j < 2048; j++) {
          sprintf(buffer, "%04d", j);
          assert(btree_get_next(&it));
          assert(it.has_val);
          assert(it.val == j);
          assert(memcmp(it.key.address, buffer, 4) == 0);
        }
      }
    }
  }

  it("work with deep trees") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t tree_id;
    assert(create_btree(&db, &tree_id));

    const int size = 2048;

    {
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);

      for (uint32_t i = 0; i < size; i++) {
        char buffer[256];
        sprintf(buffer, "%0255d", i);
        btree_val_t set = {.tree_id = tree_id,
            .key = {.address = buffer, .size = 255},
            .val = i};

        assert(btree_set(&w, &set, 0));

        tree_id = set.tree_id;
      }
      assert(txn_commit(&w));
    }
    {
      txn_t r;
      assert(txn_create(&db, TX_READ, &r));
      defer(txn_close, r);

      uint32_t cur = size - 1;
      char buffer[256];
      btree_cursor_t c = {.tree_id = tree_id, .tx = &r};
      assert(btree_cursor_at_end(&c));
      defer(btree_free_cursor, c);
      uint32_t count = 0;
      while (true) {
        assert(btree_get_prev(&c));
        if (c.has_val == false) break;
        count++;
        assert(c.val == cur);
        sprintf(buffer, "%0255d", cur--);
        assert(memcmp(buffer, c.key.address, c.key.size) == 0);
      }
      assert(count == size);
    }

    {
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);

      for (uint32_t i = 0; i < size; i++) {
        char buffer[256];
        sprintf(buffer, "%0255d", i);
        btree_val_t del = {.tree_id = tree_id,
            .key = {.address = buffer, .size = 255}};
        assert(btree_del(&w, &del));
        assert(del.has_val);
        assert(del.val == i);
      }
      assert(txn_commit(&w));
    }
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
        btree_cursor_t it = {.tree_id = tree_id,
            .tx                       = &w,
            .key = {.address = "0000", .size = 4}};
        assert(btree_cursor_search(&it));
        defer(btree_free_cursor, it);
        for (uint32_t j = 0; j < 10 * 1000; j++) {
          sprintf(buffer, "%04d", j);
          assert(btree_get_next(&it));
          assert(it.has_val);
          assert(it.val == j);
          assert(memcmp(it.key.address, buffer, 4) == 0);
        }
      }
    }
  }

  it("read, write, del") {
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

      char buffer[6];
      for (uint32_t i = 0; i < 10 * 1000; i++) {
        sprintf(buffer, "%05d", i * 2);

        btree_val_t set = {.tree_id = tree_id,
            .key                    = {.address = buffer, .size = 5},
            .val                    = i * 2};
        assert(btree_set(&w, &set, 0));
        tree_id = set.tree_id;
      }

      for (uint32_t i = 0; i < 10 * 1000; i++) {
        sprintf(buffer, "%05d", i * 2 + 1);
        btree_val_t set = {.tree_id = tree_id,
            .key                    = {.address = buffer, .size = 5},
            .val                    = i * 2 + 1};

        assert(btree_set(&w, &set, 0));
        tree_id = set.tree_id;

        btree_val_t get = {
            .tree_id = tree_id, {.address = buffer, .size = 5}};
        assert(btree_get(&w, &get));
        assert(get.has_val);
        assert(get.val == set.val);
      }

      for (uint32_t j = 5000; j < 15 * 1000; j++) {
        sprintf(buffer, "%05d", j);
        btree_val_t del = {
            .tree_id = tree_id,
            .key     = {.address = buffer, .size = 5},
        };
        assert(btree_del(&w, &del));
        assert(del.has_val);
        assert(del.val == j);
      }

      {
        btree_cursor_t it = {// start of everything
            .tree_id = tree_id,
            .tx      = &w};
        assert(btree_cursor_at_start(&it));
        defer(btree_free_cursor, it);
        for (uint32_t j = 0; j < 5 * 1000; j++) {
          sprintf(buffer, "%05d", j);
          assert(btree_get_next(&it));
          assert(it.has_val);
          assert(it.val == j);
          assert(memcmp(it.key.address, buffer, 5) == 0);
        }

        for (uint32_t j = 15 * 1000; j < 20 * 1000; j++) {
          sprintf(buffer, "%04d", j);
          assert(btree_get_next(&it));
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
        btree_cursor_t c = {.tree_id = tree_id,
            .tx                      = &w,
            .key = {.address = buffer, .size = 4}};
        assert(btree_cursor_search(&c));
        defer(btree_free_cursor, c);
        uint64_t expected = 6;
        for (size_t i = 0; i < 7; i++) {
          assert(btree_get_next(&c));
          assert(c.has_val);
          assert(c.val == expected + i * 2);
        }
        assert(btree_get_next(&c));
        assert(c.has_val == false);  // at end
        c.key.address = buffer;
        c.key.size    = 4;
        assert(btree_cursor_search(&c));
        assert(c.has_val == false);
        expected = 4;
        for (size_t i = 0; i < 3; i++) {
          assert(btree_get_prev(&c));
          assert(c.has_val);
          assert(c.val == expected - i * 2);
        }
        assert(btree_get_prev(&c));
        assert(c.has_val == false);  // at start
      }
    }
  }
}
// end::tests16[]
