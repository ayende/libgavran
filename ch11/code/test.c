#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/internal.h>
#include <gavran/test.h>

// tag::tests11[]
static result_t open_file_and_get_map(const char *path,
                                      file_handle_t **handle,
                                      span_t *map) {
  ensure(pal_create_file(path, handle, pal_file_creation_flags_none));
  map->size = (*handle)->size;
  ensure(pal_mmap(*handle, 0, map));
  ensure(pal_enable_writes(map));
  return success();
}

#define open_file_and_map(fname, dest)                          \
  file_handle_t *CONCAT(file_, __LINE__);                       \
  assert(open_file_and_get_map(fname, &CONCAT(file_, __LINE__), \
                               &dest));                         \
  defer(pal_close_file, CONCAT(file_, __LINE__));               \
  defer(pal_unmap, dest);

describe(validation_and_encryption) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can start with clean db") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db),
           "failed to create db");
    defer(db_close, db);
  }

  it("can detect disk corruption") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, w);
    page_t p = {.number_of_pages = 1};
    page_metadata_t *metadata;
    assert(txn_allocate_page(&w, &p, &metadata, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));
    // forcing a flush to disk, only allowed manually from tests
    assert(wal_checkpoint(db.state, UINT64_MAX));

    assert(db_close(&db));

    span_t map;
    open_file_and_map("/tmp/db/try", map);

    *(char *)(map.address + p.page_num * PAGE_SIZE + 100) = 1;

    assert(db_create("/tmp/db/try", &options, &db));
    assert(txn_create(&db, TX_WRITE, &w));

    void *r = txn_get_page(&w, &p);
    size_t err_count;
    int err_code = errors_get_codes(&err_count)[0];
    assert(err_count > 0);
    errors_clear();
    assert(err_code == ENODATA);
    assert(!r);
  }

  it("can detect disk corruption on encrypted database") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    randombytes_buf(options.encryption_key, 32);
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, w);
    page_t p = {.number_of_pages = 1};
    page_metadata_t *metadata;
    assert(txn_allocate_page(&w, &p, &metadata, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));
    // forcing a flush to disk, only allowed manually from tests
    assert(wal_checkpoint(db.state, UINT64_MAX));

    assert(db_close(&db));

    span_t map;
    open_file_and_map("/tmp/db/try", map);

    *(char *)(map.address + p.page_num * PAGE_SIZE + 100) = 1;

    assert(db_create("/tmp/db/try", &options, &db));
    assert(txn_create(&db, TX_WRITE, &w));

    void *r = txn_get_page(&w, &p);
    size_t err_count;
    int err_code = errors_get_codes(&err_count)[0];
    assert(err_count > 0);
    errors_clear();
    assert(err_code == EINVAL);
    assert(!r);
  }

  it("can create encrypted db") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    randombytes_buf(options.encryption_key, 32);
    assert(db_create("/tmp/db/try", &options, &db),
           "failed to create db");
    defer(db_close, db);
  }

  it("can read encrypted data from old tx") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    randombytes_buf(options.encryption_key, 32);
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, w);
    page_t p = {.number_of_pages = 1};
    page_metadata_t *metadata;
    assert(txn_allocate_page(&w, &p, &metadata, 0));
    metadata->overflow.page_flags = page_flags_overflow;
    metadata->overflow.number_of_pages = 1;
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);

    txn_t leaked;
    assert(txn_create(&db, TX_READ, &leaked));
    defer(free, leaked.working_set);

    assert(txn_commit(&w));
    assert(txn_close(&w));
    {
      txn_t rtx;
      assert(txn_create(&db, TX_READ, &rtx));
      defer(txn_close, rtx);
      page_t rp = {.page_num = p.page_num};
      assert(txn_get_page(&rtx, &rp));
      assert(strcmp(str, rp.address) == 0);
    }
  }

  it("on encrypted db, cannot find data on disk") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    randombytes_buf(options.encryption_key, 32);
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, w);
    page_t p = {.number_of_pages = 1};
    page_metadata_t *metadata;
    assert(txn_allocate_page(&w, &p, &metadata, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));

    txn_t leaked;
    assert(txn_create(&db, TX_READ, &leaked));
    defer(free, leaked.working_set);

    assert(txn_create(&db, TX_WRITE, &w));

    assert(txn_allocate_page(&w, &p, &metadata, 0));
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));
    assert(db_close(&db));

    span_t mmap;
    open_file_and_map("/tmp/db/try", mmap);
    assert(memmem(mmap.address, mmap.size, "Gavran",
                  strlen("Gavran")) == 0,
           "Expected to find NO match");

    open_file_and_map("/tmp/db/try-a.wal", mmap);
    assert(memmem(mmap.address, mmap.size, "Gavran",
                  strlen("Gavran")) == 0,
           "Expected to find NO match");

    open_file_and_map("/tmp/db/try-b.wal", mmap);
    assert(memmem(mmap.address, mmap.size, "Gavran",
                  strlen("Gavran")) == 0,
           "Expected to find NO match");
  }

  it("on normal db, can find data on disk") {
    db_t db;
    db_options_t options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try", &options, &db));
    defer(db_close, db);

    txn_t w;
    assert(txn_create(&db, TX_WRITE, &w));
    defer(txn_close, w);
    page_metadata_t *metadata;
    page_t p = {.number_of_pages = 1};
    assert(txn_allocate_page(&w, &p, &metadata, 0));
    const char str[] = "Hello From Gavran";
    strcpy(p.address, str);
    assert(txn_commit(&w));
    assert(txn_close(&w));

    assert(db_close(&db));

    span_t mmap;
    open_file_and_map("/tmp/db/try", mmap);

    void *c =
        memmem(mmap.address, mmap.size, "Gavran", strlen("Gavran"));
    assert(c != 0, "Expected to find a match");
  }
}
// end::tests11[]
