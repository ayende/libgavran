#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "defer.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::allocate_page_and_use_it[]
static result_t allocate_page_and_use_it() {
  // <1>
  db_t db;
  database_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create("/tmp/db/orev", &options, &db));
  defer(db_close, &db);

  // <2>
  txn_t tx;
  ensure(txn_create(&db, 0, &tx));
  defer(txn_close, &tx);

  {
    page_t page = {.overflow_size = 96 * 8192};
    ensure(txn_allocate_page(&tx, &page, 0));

    // <4>
    printf("New allocated page %lu\n", page.page_num);
    memset(page.address, 'x', page.overflow_size);
  }

  {
    page_t page = {.overflow_size = 43 * 8192};
    ensure(txn_allocate_page(&tx, &page, 0));

    // <4>
    printf("New allocated page %lu\n", page.page_num);
    memset(page.address, 'x', page.overflow_size);
  }

  {
    page_t page = {.overflow_size = 16 * 8192};
    ensure(txn_allocate_page(&tx, &page, 0));

    // <4>
    printf("New allocated page %lu\n", page.page_num);
    memset(page.address, 'x', page.overflow_size);
  }

  {
    page_t page = {.page_num = 129};

    ensure(txn_free_page(&tx, &page));

    bool busy;
    ensure(txn_page_busy(&tx, 128, &busy));
    printf("%x - 128\n", busy);
  }

  // // <5>
  // ensure(txn_commit(&tx));
  // ensure(txn_close(&tx));

  // // <6>
  // ensure(txn_create(&db, 0, &tx));
  // ensure(txn_get_page(&tx, &page));

  // printf("overflow size: %d\n", page.overflow_size);
  // char *buf = page.address;
  // for (size_t i = 0; i < page.overflow_size - 1; i++) {
  //   if (buf[i] != 'x') {
  //     printf("Wrong value\n");
  //   }
  // }

  return success();
}
// end::allocate_page_and_use_it[]

int main() {
  if (!allocate_page_and_use_it()) {
    errors_print_all();
  }
  printf("Done\n");
  return 0;
}
