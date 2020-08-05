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

// tag::tests14[]

describe(hash) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can get and set value from hash") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    uint64_t hash_id;
    {
      txn_t w;
      assert(txn_create(&db, TX_WRITE, &w));
      defer(txn_close, w);
      assert(hash_create(&w, &hash_id));
      hash_val_t hv_write = {.hash_id = hash_id, .key = 2, .val = 3};
      assert(hash_set(&w, &hv_write, 0));
      hash_val_t hv_read = {.hash_id = hash_id, .key = 2};
      assert(hash_get(&w, &hv_read));  // read a written value
      assert(hv_read.val == 3);
      hash_val_t hv_old = {.hash_id = hash_id};
      hv_write.val      = 4;
      assert(hash_set(&w, &hv_write, &hv_old));
      assert(hv_old.val == 3);  // get old on update
      assert(hash_del(&w, &hv_old));
      assert(hv_old.val == 4);  // get old on delete
      assert(hash_get(&w, &hv_read));
      assert(hv_read.has_val == false);
      assert(txn_commit(&w));
    }
  }
}
// end::tests14[]
