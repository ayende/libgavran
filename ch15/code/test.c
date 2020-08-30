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

// tag::remember_item[]
static result_t remember_item(
    db_t* db, uint64_t container_id, char* json, uint64_t* item_id) {
  txn_t w;
  ensure(txn_create(db, TX_WRITE, &w));
  defer(txn_close, w);
  container_item_t item = {.container_id = container_id,
      .data = {.address = json, .size = strlen(json)}};
  ensure(container_item_put(&w, &item));
  *item_id = item.item_id;
  ensure(txn_commit(&w));
  return success();
}
// end::remember_item[]

// tag::tests15[]
static result_t write_items(
    db_t* db, uint64_t container_id, char* json) {
  txn_t w;
  ensure(txn_create(db, TX_WRITE, &w));
  defer(txn_close, w);
  container_item_t item = {.container_id = container_id,
      .data = {.address = json, .size = strlen(json)}};
  for (size_t i = 0; i < 1024; i++) {
    ensure(container_item_put(&w, &item));
  }

  ensure(txn_commit(&w));
  return success();
}

static result_t read_item(db_t* db, uint64_t container_id,
    uint64_t item_id, char* expected) {
  txn_t r;
  ensure(txn_create(db, TX_READ, &r));
  defer(txn_close, r);
  container_item_t item = {
      .container_id = container_id, .item_id = item_id};
  ensure(container_item_get(&r, &item));
  ensure(strlen(expected) == item.data.size);
  ensure(memcmp(item.data.address, expected, strlen(expected)) == 0);

  return success();
}

static result_t create_container(db_t* db, uint64_t* container_id) {
  txn_t w;
  ensure(txn_create(db, TX_WRITE, &w));
  defer(txn_close, w);
  ensure(container_create(&w, container_id));
  ensure(txn_commit(&w));
  return success();
}

describe(containers) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can remember items") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t container_id;
    assert(create_container(&db, &container_id));

    char* json1 = "{'Hi': 'There'}";
    uint64_t item_id1;
    assert(remember_item(&db, container_id, json1, &item_id1));
    char* json2 = "{'Msg': 'This is a longer string'}";
    uint64_t item_id2;
    assert(remember_item(&db, container_id, json2, &item_id2));

    assert(read_item(&db, container_id, item_id1, json1));
    assert(read_item(&db, container_id, item_id2, json2));
  }

  it("can update item size (small)") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t container_id;
    assert(create_container(&db, &container_id));
    char* json1 = "{'Hi': 'small text'}";
    char* json2 =
        "{'Hi': 'This is a larger amount of text that we write'}";

    uint64_t item_id;
    {
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);
      container_item_t item = {.container_id = container_id,
          .data = {.address = json1, .size = strlen(json1)}};
      assert(container_item_put(&w, &item));
      item_id = item.item_id;
      bool in_place;
      item.data.address = json2;
      item.data.size    = strlen(json2);

      assert(container_item_update(&w, &item, &in_place));
      assert(in_place);
      assert(txn_commit(&w));
    }
    { assert(read_item(&db, container_id, item_id, json2)); }
  }

  it("can update item size (large)") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t container_id;
    assert(create_container(&db, &container_id));
    void *buf1, *buf2;
    assert(mem_calloc(&buf1, 1024 * 12));
    defer(free, buf1);
    assert(mem_calloc(&buf2, 1024 * 14));
    defer(free, buf2);
    memset(buf1, 'a', 1024 * 12 - 1);
    memset(buf2, 'b', 1024 * 13 - 1);

    uint64_t item_id;
    {
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);
      container_item_t item = {.container_id = container_id,
          .data = {.address = buf1, .size = strlen(buf1)}};
      assert(container_item_put(&w, &item));
      item_id = item.item_id;
      bool in_place;
      item.data.address = buf2;
      item.data.size    = strlen(buf2);

      assert(container_item_update(&w, &item, &in_place));
      assert(in_place);
      assert(txn_commit(&w));
    }
    { assert(read_item(&db, container_id, item_id, buf2)); }
  }

#define ITEM_ARRAY_SIZE 185
  it("force page defrag") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t container_id;
    assert(create_container(&db, &container_id));
    uint64_t items[ITEM_ARRAY_SIZE];
    {
      char* json1 = "{'Hi': 'This is a small amount of text'}";
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);
      container_item_t item = {.container_id = container_id,
          .data = {.address = json1, .size = strlen(json1)}};
      for (size_t i = 0; i < ITEM_ARRAY_SIZE; i++) {
        assert(container_item_put(&w, &item));
        items[i] = item.item_id;
      }
      assert(txn_commit(&w));
    }

    {
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);
      // delete to create gaps in the page
      for (size_t i = 0; i < ITEM_ARRAY_SIZE / 2; i += 2) {
        container_item_t item = {
            .container_id = container_id, .item_id = items[i]};
        assert(container_item_del(&w, &item));
      }
      assert(txn_commit(&w));
    }
    // everything on same page
    assert(items[0] / PAGE_SIZE ==
           items[ITEM_ARRAY_SIZE - 1] / PAGE_SIZE);
    {
      char* json2 =
          "{'This': 'is a larger amount of json that cannot fit into "
          "old holes from previous writes/deletes'}";
      uint64_t new_item_id;
      assert(remember_item(&db, container_id, json2, &new_item_id));
      assert(new_item_id == items[0]);
    }
  }

  it("can write more items than fit in a single page") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t container_id;
    assert(create_container(&db, &container_id));

    char* buffer   = "{'Hi':'There','Using':'longer json string'}";
    size_t buf_len = strlen(buffer);
    assert(write_items(&db, container_id, buffer));

    {
      txn_t r;
      assert(txn_create(&db, TX_READ, &r));
      defer(txn_close, r);
      size_t count         = 0;
      container_item_t cur = {.container_id = container_id};
      while (container_get_next(&r, &cur) && cur.data.address) {
        assert(buf_len == cur.data.size);
        assert(memcmp(cur.data.address, buffer, buf_len) == 0);
        count++;
      }
      assert(count == 1024);
    }
  }

  it("can delete items") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t container_id;
    assert(create_container(&db, &container_id));

    char* buffer   = "{'Hi':'There','Using':'longer json string'}";
    size_t buf_len = strlen(buffer);
    uint64_t i1, i2, i3;
    assert(remember_item(&db, container_id, buffer, &i1));
    assert(remember_item(&db, container_id, buffer, &i2));
    assert(remember_item(&db, container_id, buffer, &i3));

    {
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);
      container_item_t item = {
          .container_id = container_id, .item_id = i2};
      assert(container_item_del(&w, &item));
      assert(txn_commit(&w));
    }

    {
      txn_t r;
      assert(txn_create(&db, TX_READ, &r));
      defer(txn_close, r);
      size_t count         = 0;
      container_item_t cur = {.container_id = container_id};
      while (container_get_next(&r, &cur) && cur.data.address) {
        assert(buf_len == cur.data.size);
        assert(memcmp(cur.data.address, buffer, buf_len) == 0);
        count++;
      }
      assert(count == 2);
    }
  }

  it("can write large item") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);
    uint64_t container_id;
    assert(create_container(&db, &container_id));

    char* buffer   = "{'Hi':'There','Using':'longer json string'}";
    size_t buf_len = strlen(buffer);
    assert(write_items(&db, container_id, buffer));

    void* buf_large;
    assert(mem_calloc(&buf_large, 1024 * 7));
    defer(free, buf_large);
    memset(buf_large, 'a', 1024 * 7);
    ((char*)buf_large)[1024 * 7 - 1] = 0;
    for (size_t i = 0; i < 3; i++) {
      uint64_t item_id;
      assert(remember_item(&db, container_id, buf_large, &item_id));
    }
    assert(write_items(&db, container_id, buffer));

    {
      txn_t r;
      assert(txn_create(&db, TX_READ, &r));
      defer(txn_close, r);
      size_t count = 0, count_large = 0;
      container_item_t cur = {.container_id = container_id};
      while (container_get_next(&r, &cur) && cur.data.address) {
        if (buf_len == cur.data.size) {
          assert(memcmp(cur.data.address, buffer, buf_len) == 0);
          count++;
        } else if (1024 * 7 - 1 == cur.data.size) {
          assert(memcmp(cur.data.address, buf_large, buf_len) == 0);
          count_large++;
        }
      }
      assert(count == 1024 * 2);
      assert(count_large == 3);
    }
  }
}
// end::tests15[]
