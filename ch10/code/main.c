#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "defer.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::data_loss[]
static result_t data_loss() {
  db_t db;
  database_options_t options = {.minimum_size = 4 * 1024 * 1024};
  ensure(db_create("/tmp/db/orev", &options, &db));
  defer(db_close, &db);
  struct wal_files_list *list;
  ensure(palfs_get_wal_files(db.state->handle, &list));
  defer(palfs_release_wal_files, list);
  for (size_t i = 0; i < list->size; i++) {
    printf("%s - %lu\n", list->files[i]->name, list->files[i]->number);
  }

  // for (size_t x = 0; x < 3; x++) {
  //   txn_t wtx;
  //   ensure(txn_create(&db, TX_WRITE, &wtx));
  //   defer(txn_close, &wtx);
  //   for (size_t i = 0; i < 4; i++) {
  //     page_t page = {.page_num = 2 + i};
  //     ensure(txn_modify_page(&wtx, &page));
  //     const char *msg = "Hello Gavran";
  //     strncpy(page.address, msg, strlen(msg) + 1);
  //   }

  //   ensure(txn_commit(&wtx));
  //   ensure(txn_close(&wtx));
  // }

  // {
  //   txn_t wtx;
  //   ensure(txn_create(&db, TX_WRITE, &wtx));
  //   defer(txn_close, &wtx);
  //   ensure(db_try_increase_file_size(&wtx, 65536));
  //   page_t p = {.overflow_size = PAGE_SIZE};
  //   ensure(txn_allocate_page(&wtx, &p, 64961));
  //   strcpy(p.address, "Hello Extended");
  //   ensure(txn_commit(&wtx));
  //   ensure(txn_close(&wtx));
  // }

  // ensure(db_close(&db));
  // ensure(db_create("/tmp/db/orev", &options, &db));
  // {
  //   txn_t rtx;
  //   ensure(txn_create(&db, TX_READ, &rtx));
  //   defer(txn_close, &rtx);
  //   page_t p = {.page_num = 64961};
  //   ensure(txn_get_page(&rtx, &p));
  //   printf("%s\n", p.address);
  // }
  return success();
}
// end::data_loss[]

int main() {

  // system("rm  /tmp/db/*");

  if (!data_loss()) {
    errors_print_all();
  }
  // if (!data_loss()) {
  //   errors_print_all();
  // }
  printf("Done\n");
  return 0;
}
