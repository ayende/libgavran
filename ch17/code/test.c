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
result_t multiple_vals_tree(uint64_t amount) {
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

    {
      uint64_t count    = 0;
      btree_cursor_t it = {.tree_id = tree_id,
          .tx                       = &tx,
          .key = {.address = key, .size = strlen(key)}};
      defer(btree_free_cursor, it);
      ensure(btree_multi_cursor_search(&it));
      while (true) {
        ensure(btree_multi_get_next(&it));
        if (it.has_val == false) break;
        count++;
        ensure(count == it.val);
      }
      ensure(count == amount);
    }

    for (size_t i = 1; i < amount; i += 2) {
      btree_val_t del = {.tree_id = tree_id,
          .val                    = i,
          .key = {.address = key, .size = strlen(key)}};
      ensure(btree_multi_del(&tx, &del));
    }

    {
      uint64_t count    = 0;
      btree_cursor_t it = {.tree_id = tree_id,
          .tx                       = &tx,
          .key = {.address = key, .size = strlen(key)}};
      defer(btree_free_cursor, it);
      ensure(btree_multi_cursor_search(&it));
      while (true) {
        ensure(btree_multi_get_next(&it));
        if (it.has_val == false) break;
        count += 2;
        ensure(count == it.val);
      }
      ensure(count == amount);
    }
  }
  return success();
}

result_t multiple_vals_hash(uint64_t amount) {
  db_t db;
  db_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create("/tmp/db/try", &options, &db));
  defer(db_close, db);

  {
    txn_t tx;
    ensure(txn_create(&db, TX_WRITE, &tx));
    defer(txn_close, tx);
    uint64_t hash_id, container_id;
    ensure(hash_create(&tx, &hash_id));
    ensure(container_create(&tx, &container_id));

    hash_val_t set = {.hash_id = hash_id, .key = 127001};
    for (size_t i = 0; i < amount; i++) {
      set.val = i + 1;
      ensure(hash_multi_append(&tx, &set, container_id));
    }

    {
      uint64_t count = 0;
      hash_val_t it  = {.hash_id = set.hash_id, .key = 127001};
      pages_map_t* pages;
      ensure(pagesmap_new(8, &pages));
      defer(free, pages);
      while (true) {
        ensure(hash_multi_get_next(&tx, &pages, &it, container_id));
        if (it.has_val == false) break;
        count++;
        ensure(it.val > 0 && it.val <= amount);
      }
      ensure(count == amount);
    }

    for (size_t i = 1; i < amount; i += 2) {
      hash_val_t del = {.hash_id = hash_id, .val = i, .key = 127001};
      ensure(hash_multi_del(&tx, &del, container_id));
    }

    {
      uint64_t count = 0;
      hash_val_t it  = {.hash_id = set.hash_id, .key = 127001};
      pages_map_t* pages;
      ensure(pagesmap_new(8, &pages));
      defer(free, pages);
      while (true) {
        ensure(hash_multi_get_next(&tx, &pages, &it, container_id));
        if (it.has_val == false) break;
        count += 2;
        ensure(it.val > 0 && it.val <= amount);
      }
      ensure(count == amount);
    }
  }
  return success();
}
describe(multiple_vals) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("6 multi btree records") { assert(multiple_vals_tree(6)); }

  it("many identical btree records") {
    assert(multiple_vals_tree(300));
  }

  it("6 multi hash records") { assert(multiple_vals_hash(6)); }

  it("many identical hash records") {
    assert(multiple_vals_hash(300));
  }
}
// end::tests17[]
