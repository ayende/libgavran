#include "errors.h"
#include "pal.h"
#include "transactions.h"
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

int main()
{

  // uint64_t x[] = {0x80520A27102E21, 0xE0000E00020, 0x1A40027F025802C8,
  //                 0xEE6ACAE56C6C3DCC};

  //   struct range_finder range;
  //   init_range(x, 4, 1, &range);

  //   while (find_next(&range)) {
  //     printf("%zu (%zu)\n", range.selection.position,
  //            range.selection.size_available);
  //   }

  database_t db;
  database_options_t options = {0};
  txn_t tx;
  page_t page;

  assert(open_database("db/orev", &options, &db));

  assert(create_transaction(&db, 0, &tx));
  assert(allocate_page(&tx, &page));

  printf("New allocated page %lu\n", page.page_num);
  strcpy(page.address, "Hello Gavran");

  assert(commit_transaction(&tx));
  assert(close_transaction(&tx));
  assert(create_transaction(&db, 0, &tx));
  assert(get_page(&tx, &page));

  printf("%s\n", page.address);

  assert(close_transaction(&tx));
  assert(close_database(&db));

  return 0;
}
