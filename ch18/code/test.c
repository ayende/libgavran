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

// tag::tests18[]
describe(tables) {
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

    {
      txn_t tx;
      assert(txn_create(&db, TX_READ, &tx));
      defer(txn_close, tx);

      table_schema_t root;
      assert(table_get_schema(&tx, "root", &root));
      assert(root.count == 2);
      assert(root.types[0] == index_type_container);
      assert(root.types[1] == index_type_btree);

      assert(root.index_ids[0] == 2);
      assert(root.index_ids[1] == 4);
    }
  }
}
// end::tests18[]
