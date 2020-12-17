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

// tag::tests13[]
typedef struct db_and_error_state {
  db_t* db;
  bool has_errors;
} db_and_error_state_t;

static void ship_wal_logs(
    void* state, uint64_t tx_id, span_t* wal_record) {
  db_and_error_state_t* db_n_err = state;
  reusable_buffer_t buffer       = {0};
  defer(free, buffer.address);
  if (flopped(wal_apply_wal_record(
          db_n_err->db, &buffer, tx_id, wal_record))) {
    db_n_err->has_errors = true;
  }
}

// tag::incremental_backup[]
static void incremental_backup(
    void* state, uint64_t tx_id, span_t* wal_record) {
  FILE* dst  = state;
  time_t now = time(0);
  fwrite(&now, sizeof(time_t), 1, dst);
  fwrite(&tx_id, sizeof(uint64_t), 1, dst);
  uint64_t size = wal_record->size;
  fwrite(&size, sizeof(uint64_t), 1, dst);
  fwrite(wal_record->address, 1, wal_record->size, dst);
}
// end::incremental_backup[]

// tag::apply_backups[]
static result_t apply_backups(
    const char* db_file, const char* backup_file, time_t until) {
  db_t db;
  db_options_t options = {.minimum_size = 4 * 1024 * 1024,
      .flags = db_flags_log_shipping_target};
  ensure(db_create(db_file, &options, &db));
  defer(db_close, db);
  file_handle_t* f;
  ensure(
      pal_create_file(backup_file, &f, pal_file_creation_flags_none));
  defer(pal_close_file, f);
  span_t span = {.size = f->size};
  ensure(pal_mmap(f, 0, &span));
  defer(pal_unmap, span);
  void* start                  = span.address;
  void* end                    = span.address + span.size;
  reusable_buffer_t tmp_buffer = {0};
  defer(free, tmp_buffer.address);
  while (start < end) {
    time_t time = *(time_t*)start;
    if (time > until) break;
    start += sizeof(time_t);
    uint64_t tx_id = *(uint64_t*)start;
    start += sizeof(uint64_t);
    uint64_t tx_size = *(uint64_t*)start;
    start += sizeof(uint64_t);
    span_t tx = {.size = (size_t)tx_size};
    ensure(mem_alloc_page_aligned(&tx.address, tx.size));
    defer(free, tx.address);
    memcpy(tx.address, start, tx.size);
    ensure(wal_apply_wal_record(&db, &tmp_buffer, tx_id, &tx));
    start += tx_size;
  }
  return success();
}
// end::apply_backups[]

static inline void defer_fclose(cancel_defer_t* cd) {
  if (cd->cancelled && *cd->cancelled) return;
  fclose(*(void**)cd->target);
}

describe(log_shipping) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }

  it("can use log shipping between instances") {
    db_t src, dst;

    db_options_t dst_options = {.minimum_size = 4 * 1024 * 1024,
        .flags = db_flags_log_shipping_target};
    assert(db_create("/tmp/db/try-dst", &dst_options, &dst));
    defer(db_close, dst);

    db_and_error_state_t state = {.db = &dst};
    db_options_t src_options   = {.minimum_size = 4 * 1024 * 1024,
        .wal_write_callback                   = ship_wal_logs,
        .wal_write_callback_state             = &state};
    assert(db_create("/tmp/db/try-src", &src_options, &src));
    defer(db_close, src);

    uint64_t page;
    {
      txn_t w;
      assert(txn_create(&src, TX_WRITE, &w));
      defer(txn_close, w);
      page_t p = {.number_of_pages = 1};
      assert(txn_allocate_page(&w, &p, 0));
      page                                 = p.page_num;
      p.metadata->overflow.page_flags      = page_flags_overflow;
      p.metadata->overflow.number_of_pages = 1;
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

  it("can use log shipping for backups") {
    {
      uint64_t page;
      {
        // tag::incremental_backup_setup[]
        db_t src;
        FILE* backup = fopen("/tmp/db/try.backup", "wb");
        defer(fclose, backup);
        db_options_t src_options = {.minimum_size = 4 * 1024 * 1024,
            .wal_write_callback       = incremental_backup,
            .wal_write_callback_state = backup};
        assert(db_create("/tmp/db/try", &src_options, &src));
        defer(db_close, src);
        // end::incremental_backup_setup[]

        {
          txn_t w;
          assert(txn_create(&src, TX_WRITE, &w));
          defer(txn_close, w);
          page_t p = {.number_of_pages = 1};
          assert(txn_allocate_page(&w, &p, 0));
          page                                 = p.page_num;
          p.metadata->overflow.page_flags      = page_flags_overflow;
          p.metadata->overflow.number_of_pages = 1;
          strcpy(p.address, "Hello Remotely");
          assert(txn_commit(&w));
        }
      }

      assert(apply_backups(
          "/tmp/db/restored", "/tmp/db/try.backup", LONG_MAX));
      {
        db_t db;
        db_options_t options = {.minimum_size = 4 * 1024 * 1024};
        assert(db_create("/tmp/db/restored", &options, &db));
        defer(db_close, db);

        txn_t r;
        assert(txn_create(&db, TX_READ, &r));
        defer(txn_close, r);
        page_t p = {.page_num = page};
        assert(txn_get_page(&r, &p));
        assert(strcmp("Hello Remotely", p.address) == 0);
      }
    }
  }

  it("can use log shipping between instances with 32 bits") {
    db_t src, dst;

    db_options_t dst_options = {.minimum_size = 4 * 1024 * 1024,
        .flags =
            db_flags_avoid_mmap_io | db_flags_log_shipping_target};
    assert(db_create("/tmp/db/try-dst", &dst_options, &dst));
    defer(db_close, dst);

    db_and_error_state_t state = {.db = &dst};
    db_options_t src_options   = {.minimum_size = 4 * 1024 * 1024,
        .wal_write_callback                   = ship_wal_logs,
        .flags                    = db_flags_avoid_mmap_io,
        .wal_write_callback_state = &state};
    assert(db_create("/tmp/db/try-src", &src_options, &src));
    defer(db_close, src);

    uint64_t page;
    {
      txn_t w;
      assert(txn_create(&src, TX_WRITE, &w));
      defer(txn_close, w);
      page_t p = {.number_of_pages = 1};
      assert(txn_allocate_page(&w, &p, 0));
      page                                 = p.page_num;
      p.metadata->overflow.page_flags      = page_flags_overflow;
      p.metadata->overflow.number_of_pages = 1;
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

    db_options_t dst_options = {.minimum_size = 4 * 1024 * 1024,
        .flags = db_flags_log_shipping_target};
    memcpy(dst_options.encryption_key, key, 32);
    assert(db_create("/tmp/db/try-dst", &dst_options, &dst));
    defer(db_close, dst);

    db_and_error_state_t state = {.db = &dst};
    db_options_t src_options   = {.minimum_size = 4 * 1024 * 1024,
        .wal_write_callback                   = ship_wal_logs,
        .wal_write_callback_state             = &state};
    memcpy(src_options.encryption_key, key, 32);
    assert(db_create("/tmp/db/try-src", &src_options, &src));
    defer(db_close, src);

    uint64_t page;
    {
      txn_t w;
      assert(txn_create(&src, TX_WRITE, &w));
      defer(txn_close, w);
      page_t p = {.number_of_pages = 1};
      assert(txn_allocate_page(&w, &p, 0));
      page                                 = p.page_num;
      p.metadata->overflow.page_flags      = page_flags_overflow;
      p.metadata->overflow.number_of_pages = 1;
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

  it("log shipping with encryption, destination without key") {
    db_t src, dst;
    uint64_t page;

    char key[32];
    randombytes_buf(key, 32);
    {
      db_options_t dst_options = {.minimum_size = 4 * 1024 * 1024,
          .flags = db_flags_log_shipping_target};
      assert(db_create("/tmp/db/try-dst", &dst_options, &dst));
      defer(db_close, dst);

      db_and_error_state_t state = {.db = &dst};
      db_options_t src_options   = {.minimum_size = 4 * 1024 * 1024,
          .wal_write_callback                   = ship_wal_logs,
          .wal_write_callback_state             = &state};
      memcpy(src_options.encryption_key, key, 32);
      assert(db_create("/tmp/db/try-src", &src_options, &src));
      defer(db_close, src);

      {
        txn_t w;
        assert(txn_create(&src, TX_WRITE, &w));
        defer(txn_close, w);
        page_t p = {.number_of_pages = 1};
        assert(txn_allocate_page(&w, &p, 0));
        page                                 = p.page_num;
        p.metadata->overflow.page_flags      = page_flags_overflow;
        p.metadata->overflow.number_of_pages = 1;
        strcpy(p.address, "Hello Remotely");
        assert(txn_commit(&w));
      }
    }
    {
      db_options_t dst_options = {.minimum_size = 4 * 1024 * 1024,
          .flags = db_flags_log_shipping_target};
      memcpy(dst_options.encryption_key, key, 32);
      assert(db_create("/tmp/db/try-dst", &dst_options, &dst));
      defer(db_close, dst);

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
