#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/internal.h>
#include <gavran/test.h>

// tag::tests13[]
typedef struct db_and_error_state {
  db_t* db;
  bool has_errors;
} db_and_error_state_t;

static void ship_wal_logs(void* state, uint64_t tx_id,
                          span_t* wal_record) {
  db_and_error_state_t* db_n_err = state;
  reusable_buffer_t buffer = {0};
  defer(free, buffer.address);
  if (flopped(wal_apply_wal_record(db_n_err->db, &buffer, tx_id,
                                   wal_record))) {
    db_n_err->has_errors = true;
  }
}

describe(log_shipping) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can use log shipping between instances") {
    db_t src, dst;

    db_options_t dst_options = {.minimum_size = 4 * 1024 * 1024};
    assert(db_create("/tmp/db/try-dst", &dst_options, &dst));
    defer(db_close, dst);

    db_and_error_state_t state = {.db = &dst};
    db_options_t src_options = {.minimum_size = 4 * 1024 * 1024,
                                .wal_write_callback = ship_wal_logs,
                                .wal_write_callback_state = &state};
    assert(db_create("/tmp/db/try-src", &src_options, &src));
    defer(db_close, src);

    uint64_t page;
    {
      txn_t w;
      assert(txn_create(&src, TX_WRITE, &w));
      defer(txn_close, w);
      page_t p = {.number_of_pages = 1};
      page_metadata_t* metadata;
      assert(txn_allocate_page(&w, &p, &metadata, 0));
      page = p.page_num;
      metadata->overflow.page_flags = page_flags_overflow;
      metadata->overflow.number_of_pages = 1;
      strcpy(p.address, "Hello Remotely");
      assert(txn_commit(&w));
    }
    {
      txn_t r;
      assert(txn_create(&dst, TX_READ, &r));
      defer(txn_close, r);
      page_t p = {.page_num = page};
      assert(txn_get_page(&r, &p));
      assert(strcmp("Hello Remotely", p.address) == 0);
    }
  }

  it("can use log shipping between instances with 32 bits") {
    db_t src, dst;

    db_options_t dst_options = {.minimum_size = 4 * 1024 * 1024,
                                .flags = db_flags_avoid_mmap_io};
    assert(db_create("/tmp/db/try-dst", &dst_options, &dst));
    defer(db_close, dst);

    db_and_error_state_t state = {.db = &dst};
    db_options_t src_options = {.minimum_size = 4 * 1024 * 1024,
                                .wal_write_callback = ship_wal_logs,
                                .flags = db_flags_avoid_mmap_io,
                                .wal_write_callback_state = &state};
    assert(db_create("/tmp/db/try-src", &src_options, &src));
    defer(db_close, src);

    uint64_t page;
    {
      txn_t w;
      assert(txn_create(&src, TX_WRITE, &w));
      defer(txn_close, w);
      page_t p = {.number_of_pages = 1};
      page_metadata_t* metadata;
      assert(txn_allocate_page(&w, &p, &metadata, 0));
      page = p.page_num;
      metadata->overflow.page_flags = page_flags_overflow;
      metadata->overflow.number_of_pages = 1;
      strcpy(p.address, "Hello Remotely");
      assert(txn_commit(&w));
    }
    {
      txn_t r;
      assert(txn_create(&dst, TX_READ, &r));
      defer(txn_close, r);
      page_t p = {.page_num = page};
      assert(txn_get_page(&r, &p));
      assert(strcmp("Hello Remotely", p.address) == 0);
    }
  }

  it("can use log shipping between instances with encryption") {
    db_t src, dst;

    char key[32];
    randombytes_buf(key, 32);

    db_options_t dst_options = {.minimum_size = 4 * 1024 * 1024};
    memcpy(dst_options.encryption_key, key, 32);
    assert(db_create("/tmp/db/try-dst", &dst_options, &dst));
    defer(db_close, dst);

    db_and_error_state_t state = {.db = &dst};
    db_options_t src_options = {.minimum_size = 4 * 1024 * 1024,
                                .wal_write_callback = ship_wal_logs,
                                .wal_write_callback_state = &state};
    memcpy(src_options.encryption_key, key, 32);
    assert(db_create("/tmp/db/try-src", &src_options, &src));
    defer(db_close, src);

    uint64_t page;
    {
      txn_t w;
      assert(txn_create(&src, TX_WRITE, &w));
      defer(txn_close, w);
      page_t p = {.number_of_pages = 1};
      page_metadata_t* metadata;
      assert(txn_allocate_page(&w, &p, &metadata, 0));
      page = p.page_num;
      metadata->overflow.page_flags = page_flags_overflow;
      metadata->overflow.number_of_pages = 1;
      strcpy(p.address, "Hello Remotely");
      assert(txn_commit(&w));
    }
    {
      txn_t r;
      assert(txn_create(&dst, TX_READ, &r));
      defer(txn_close, r);
      page_t p = {.page_num = page};
      assert(txn_get_page(&r, &p));
      assert(strcmp("Hello Remotely", p.address) == 0);
    }
  }
}
// end::tests13[]
