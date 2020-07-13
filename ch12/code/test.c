
#include <errno.h>
#include <stdio.h>
#include <string.h>

// #pragma clang diagnostic push
// #pragma clang diagnostic ignored "-Wmissing-prototypes"
// #pragma clang diagnostic ignored "-Wpadded"
// #pragma clang diagnostic ignored "-Wcast-align"
// #pragma clang diagnostic ignored "-Wmissing-variable-declarations"
// #pragma clang diagnostic ignored "-Wformat-nonliteral"
// #include "bdd-for-c.h"
// #pragma clang diagnostic pop

#include "db.h"
#include "defer.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

#define SNOW_ENABLED
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-prototypes"
#pragma clang diagnostic ignored "-Wpadded"
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
#pragma clang diagnostic ignored "-Wmissing-variable-declarations"
#pragma clang diagnostic ignored "-Wformat-nonliteral"
#pragma clang diagnostic ignored "-Wstrict-prototypes"
#include "snow.h"

#define DB_NAME "/tmp/db/orev"

#define open_file_and_map(fname, dest)                                         \
  file_handle_t *CONCAT(file_, __LINE__) = malloc(1024);                       \
  defer(free, CONCAT(file_, __LINE__));                                        \
  assert(palfs_create_file(fname, CONCAT(file_, __LINE__),                     \
                           palfs_file_creation_flags_none));                   \
  defer(palfs_close_file, CONCAT(file_, __LINE__));                            \
  struct mmap_args CONCAT(mmap_, __LINE__);                                    \
  assert(palfs_get_filesize(CONCAT(file_, __LINE__),                           \
                            &(CONCAT(mmap_, __LINE__)).size));                 \
  assert(palfs_mmap(CONCAT(file_, __LINE__), 0, &(CONCAT(mmap_, __LINE__))));  \
  defer(palfs_unmap, &CONCAT(mmap_, __LINE__));                                \
  assert(palfs_enable_writes(CONCAT(mmap_, __LINE__).address,                  \
                             CONCAT(mmap_, __LINE__).size));                   \
  *dest = CONCAT(mmap_, __LINE__);

describe(hash_data_page) {

  before_each() {
    system("rm  /tmp/db/*");
    errors_clear();
  }

  it("can start in 32 bits mode") {
    db_t db;
    database_options_t options = {.minimum_size = 4 * 1024 * 1024};
    options.avoid_mmap_io = 1;
    assert(db_create(DB_NAME, &options, &db), "failed to create db");
    defer(db_close, &db);

    assert(db.state->global_state.mmap.address == 0,
           "No mapping should be generated");
  }

  it("can modify data and restart in 32 bits mode") {
    db_t db;
    database_options_t options = {.minimum_size = 4 * 1024 * 1024};
    options.avoid_mmap_io = 1;
    assert(db_create(DB_NAME, &options, &db), "failed to create db");
    defer(db_close, &db);

    txn_t read_tx;
    assert(txn_create(&db, TX_READ, &read_tx));

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, &w);
    page_t p = {.overflow_size = PAGE_SIZE};
    assert(txn_allocate_page(&w, &p, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));

    assert(txn_close(&read_tx));

    assert(db_close(&db));

    assert(db_create(DB_NAME, &options, &db), "failed to create db");
  }

  // tag::crypto_hash[]
  it("can start with clean db") {
    db_t db;
    database_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create(DB_NAME, &options, &db), "failed to create db");
    defer(db_close, &db);
  }

  it("can detect disk corruption") {
    db_t db;
    database_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create(DB_NAME, &options, &db));
    defer(db_close, &db);

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, &w);
    page_t p = {.overflow_size = PAGE_SIZE};
    assert(txn_allocate_page(&w, &p, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));

    assert(db_close(&db));

    struct mmap_args map;
    open_file_and_map(DB_NAME, &map);

    *(char *)(map.address + p.page_num * PAGE_SIZE + 100) = 1;

    assert(db_create(DB_NAME, &options, &db));
    assert(txn_create(&db, TX_WRITE, &w));

    void *r = txn_get_page(&w, &p);
    size_t err_count;
    int err_code = errors_get_codes(&err_count)[0];
    assert(err_count > 0);
    errors_clear();
    assert(err_code == ENODATA);
    assert(!r);
  }
  // end::crypto_hash[]

  it("can detect disk corruption on encrypted database") {
    db_t db;
    database_options_t options = {.minimum_size = 4 * 1024 * 1024};
    randombytes_buf(options.encryption_key, 32);
    assert(db_create(DB_NAME, &options, &db));
    defer(db_close, &db);

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, &w);
    page_t p = {.overflow_size = PAGE_SIZE};
    assert(txn_allocate_page(&w, &p, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));

    assert(db_close(&db));

    struct mmap_args map;
    open_file_and_map(DB_NAME, &map);

    *(char *)(map.address + p.page_num * PAGE_SIZE + 100) = 1;

    assert(db_create(DB_NAME, &options, &db));
    assert(txn_create(&db, TX_WRITE, &w));

    void *r = txn_get_page(&w, &p);
    size_t err_count;
    int err_code = errors_get_codes(&err_count)[0];
    assert(err_count > 0);
    errors_clear();
    assert(err_code == EINVAL);
    assert(!r);
  }
  // tag::encrypted_db[]
  it("can create encrypted db") {
    db_t db;
    database_options_t options = {.minimum_size = 4 * 1024 * 1024};
    randombytes_buf(options.encryption_key, 32);
    assert(db_create(DB_NAME, &options, &db), "failed to create db");
    defer(db_close, &db);
  }

  it("on encrypted db, cannot find data on disk") {
    db_t db;
    database_options_t options = {.minimum_size = 4 * 1024 * 1024};
    randombytes_buf(options.encryption_key, 32);
    assert(db_create(DB_NAME, &options, &db));
    defer(db_close, &db);

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, &w);
    page_t p = {.overflow_size = PAGE_SIZE};
    assert(txn_allocate_page(&w, &p, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));

    txn_t read_tx;
    assert(txn_create(&db, TX_READ, &read_tx));

    assert(txn_create(&db, TX_WRITE, &w));

    assert(txn_allocate_page(&w, &p, 0));
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));
    assert(txn_close(&read_tx));
    assert(db_close(&db));

    struct mmap_args mmap;
    open_file_and_map(DB_NAME, &mmap);
    assert(memmem(mmap.address, mmap.size, "Gavran", strlen("Gavran")) == 0,
           "Expected to find NO match");

    open_file_and_map(DB_NAME "-a.wal", &mmap);
    assert(memmem(mmap.address, mmap.size, "Gavran", strlen("Gavran")) == 0,
           "Expected to find NO match");

    open_file_and_map(DB_NAME "-b.wal", &mmap);
    assert(memmem(mmap.address, mmap.size, "Gavran", strlen("Gavran")) == 0,
           "Expected to find NO match");
  }
  // end::encrypted_db[]

  it("on normal db, can find data on disk") {
    db_t db;
    database_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create(DB_NAME, &options, &db));
    defer(db_close, &db);

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, &w);
    page_t p = {.overflow_size = PAGE_SIZE};
    assert(txn_allocate_page(&w, &p, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));

    assert(db_close(&db));

    struct mmap_args mmap;
    open_file_and_map(DB_NAME, &mmap);

    void *c = memmem(mmap.address, mmap.size, "Gavran", strlen("Gavran"));
    assert(c != 0, "Expected to find a match");
  }
}

snow_main();

// #define testcase(NAME, ...)                                                    \
//   it(#NAME) {                                                                  \
//     struct operation_result * (^CONCAT(test_, __LINE__))(void) =               \
//         ^struct operation_result *(void) {                                     \
//       do {                                                                     \
//         __VA_ARGS__                                                            \
//       } while (0);                                                             \
//       return success();                                                        \
//     };                                                                         \
//     __BDD_MACRO__(__BDD_CHECK_,                                                \
//                   CONCAT(test_, __LINE__)() && errors_get_count() == 0);       \
//   }
// spec("gavran") {

//   before_each() {
//     system("rm  /tmp/db/*");
//     errors_clear();
//   }

//   testcase("can detect disk corruption", {
//     db_t db;
//     database_options_t options = {.minimum_size = 4 * 1024 * 1024};
//     ensure(db_create(DB_NAME, &options, &db));
//     defer(db_close, &db);

//     txn_t w;
//     ensure(txn_create(&db, TX_WRITE, &w));
//     defer(txn_close, &w);
//     page_t p = {.overflow_size = PAGE_SIZE};
//     ensure(txn_allocate_page(&w, &p, 0));
//     const char str[] = "Hello From Gavran";
//     strcpy(p.address, str);
//     ensure(txn_commit(&w));
//     ensure(txn_close(&w));

//     ensure(db_close(&db));

//     void *address;
//     open_file_and_map(DB_NAME, &address);

//     *(char *)(address + p.page_num * PAGE_SIZE + 100) = 1;

//     ensure(db_create(DB_NAME, &options, &db));
//     ensure(txn_create(&db, TX_WRITE, &w));

//     void *r = txn_get_page(&w, &p);
//     size_t err_count;
//     int err_code = errors_get_codes(&err_count)[0];
//     errors_clear();
//     check(err_code == ENODATA);
//     check(!r);
//   });

//   testcase("can create a database", {
//     db_t db;
//     database_options_t options = {.minimum_size = 4 * 1024 * 1024};
//     ensure(db_create(DB_NAME, &options, &db));
//     defer(db_close, &db);
//   });

//   testcase("create encrypted database", {
//     db_t db;
//     database_options_t options = {.minimum_size = 4 * 1024 * 1024};
//     randombytes_buf(options.encryption_key, crypto_aead_aes256gcm_KEYBYTES);
//     ensure(db_create(DB_NAME, &options, &db));
//     defer(db_close, &db);
//   });

//   testcase("on normal db, can find data on disk", {
//     db_t db;
//     database_options_t options = {.minimum_size = 4 * 1024 * 1024};
//     randombytes_buf(options.encryption_key, crypto_aead_aes256gcm_KEYBYTES);
//     ensure(db_create(DB_NAME, &options, &db));
//     defer(db_close, &db);

//     txn_t w;
//     ensure(txn_create(&db, TX_WRITE, &w));
//     defer(txn_close, &w);
//     page_t p = {.overflow_size = PAGE_SIZE};
//     ensure(txn_allocate_page(&w, &p, 0));
//     const char str[] = "Hello From Gavran";
//     strcpy(p.address, str);
//     ensure(txn_commit(&w));
//     ensure(txn_close(&w));

//     ensure(db_close(&db));

//     void *address;
//     open_file_and_map(DB_NAME, &address);

//     check(strstr(address, "Gavran") == 0);
//   });

//   testcase("on encrypted db, cannot find data on disk after commit", {
//     db_t db;
//     database_options_t options = {.minimum_size = 4 * 1024 * 1024};
//     randombytes_buf(options.encryption_key, crypto_aead_aes256gcm_KEYBYTES);
//     ensure(db_create(DB_NAME, &options, &db));
//     defer(db_close, &db);

//     txn_t w;
//     ensure(txn_create(&db, TX_WRITE, &w));
//     defer(txn_close, &w);
//     page_t p = {.overflow_size = PAGE_SIZE};
//     ensure(txn_allocate_page(&w, &p, 0));
//     const char str[] = "Hello From Gavran";
//     strcpy(p.address, str);
//     ensure(txn_commit(&w));
//     ensure(txn_close(&w));

//     ensure(db_close(&db));

//     void *address;
//     open_file_and_map(DB_NAME, &address);
//     check(strstr(address, "Gavran") == 0);

//     open_file_and_map(DB_NAME "-a.wal", &address);
//     check(strstr(address, "Gavran") == 0);

//     open_file_and_map(DB_NAME "-b.wal", &address);
//     check(strstr(address, "Gavran") == 0);
//   });

//   return success();
// }
