#include <sodium.h>
#include <stdalign.h>
#include <string.h>
#include <zstd.h>

#include "impl.h"
#include "platform.mem.h"

typedef struct wal_state {
  file_handle_t *handle;
  struct mmap_args map;
  uint64_t last_write_pos;
} wal_state_t;

enum wal_tx_page_flags {
  wal_tx_page_flags_none = 0,
  wal_tx_page_flags_diff = 1,
};

typedef struct wal_tx_page {
  uint64_t page_num;
  uint64_t offset;
  uint32_t number_of_pages;
  uint32_t flags;
} wal_tx_page_t;

// tag::wal_tx_t[]
enum wal_tx_flags {
  wal_tx_flags_none = 0,
  wal_tx_flags_compressed = 1,
};

typedef struct wal_tx {
  uint8_t tx_hash_sodium[crypto_generichash_BYTES];
  uint64_t tx_id;
  uint64_t tx_size;
  uint32_t number_of_modified_pages;
  enum wal_tx_flags flags;
  struct wal_tx_page pages[];
} wal_tx_t;
// end::wal_tx_t[]

// tag::wal_page_diff[]
typedef struct wal_page_diff {
  uint32_t offset;
  int32_t length; // negative means zero filled
} wal_page_diff_t;
// end::wal_page_diff[]

struct wal_recovery_operation {
  db_t *db;
  wal_state_t *wal;
  void *start;
  void *end;
  uint64_t last_recovered_tx_id;
  void *tx_buffer;
  size_t tx_buffer_size;
  size_t tx_buffer_used;
};

static void *wal_apply_diff(void *input, void *input_end, page_t *page) {
  wal_page_diff_t diff;
  while (input < input_end) {
    memcpy(&diff, input, sizeof(wal_page_diff_t));
    input += sizeof(wal_page_diff_t);
    if (diff.length < 0) {
      memset(page->address + diff.offset, 0, (size_t)(-diff.length));
    } else {
      memcpy(page->address + diff.offset, input, (size_t)diff.length);
      input += diff.length;
    }
  }
  return input;
}

// tag::wal_diff_page[]
static void *wal_diff_page(uint64_t *restrict origin,
                           uint64_t *restrict modified, size_t size,
                           void *output) {
  void *current = output;
  void *end = output + size * sizeof(uint64_t);
  for (size_t i = 0; i < size; i++) {
    if (origin[i] == modified[i]) {
      continue;
    }
    bool zeroes = true;
    size_t diff_start = i;
    for (; i < size && (i - diff_start) < (1024 * 1024); i++) {
      zeroes &= modified[i] == 0; // single diff size limited to 8MB
      if (origin[i] == modified[i]) {
        if (zeroes)
          continue; // we'll try to extend zero filled ranges
        break;
      }
    }
    if (i == size)
      i--; // reached the end of the buffer, go back

    int32_t len = (int32_t)((i - diff_start) * sizeof(uint64_t));
    wal_page_diff_t diff = {.offset = (uint32_t)(diff_start * sizeof(uint64_t)),
                            .length = len};
    if (zeroes) {
      diff.length = -diff.length; // indicates zero fill
    }
    void *required_write =
        current + sizeof(wal_page_diff_t) + (diff.length > 0 ? diff.length : 0);
    if (required_write >= end) {
      memcpy(output, modified, size);
      return end;
    }
    memcpy(current, &diff, sizeof(wal_page_diff_t));
    current += sizeof(wal_page_diff_t);
    if (diff.length > 0) {
      memcpy(current, modified + diff_start, (size_t)diff.length);
      current += diff.length;
    }
  }

  return current;
}
// end::wal_diff_page[]

// tag::wal_validate_transaction[]
static bool wal_validate_transaction(db_t *db, void *start, void *end) {
  wal_tx_t *tx = start;
  if (start + tx->tx_size > end)
    return false;
  if (db->state->global_state.header.last_tx_id >= tx->tx_id)
    return false;
  if (tx->tx_size <= crypto_generichash_BYTES)
    return false;
  uint8_t hash[crypto_generichash_BYTES];
  size_t remaining =
      tx->tx_size % PAGE_SIZE ? PAGE_SIZE - (tx->tx_size % PAGE_SIZE) : 0;
  if (crypto_generichash((uint8_t *)hash, crypto_generichash_BYTES,
                         (const uint8_t *)tx + crypto_generichash_BYTES,
                         tx->tx_size - crypto_generichash_BYTES + remaining, 0,
                         0))
    return false;

  return sodium_compare(hash, tx->tx_hash_sodium, crypto_generichash_BYTES) ==
         0;
}
// end::wal_validate_transaction[]

// tag::wal_recover_validate_after[]
static result_t wal_get_next_range(struct wal_recovery_operation *state,
                                   void **current, void **end) {

  *end = state->end;
  if (state->start >= state->end) {
    *current = 0;
    return success();
  }

  return success();
}

static void wal_increment_next_range_start(struct wal_recovery_operation *state,
                                           size_t amount) {
  state->start += amount;
}

static result_t wal_recover_validate_after_end_of_transactions(
    struct wal_recovery_operation *state) {
  while (true) {
    void *current;
    void *end;
    ensure(wal_get_next_range(state, &current, &end));
    if (!current)
      break;

    if (!wal_validate_transaction(state->db, current, end)) {
      wal_increment_next_range_start(state, PAGE_SIZE);
      continue;
    }

    wal_tx_t *maybe_tx = current;
    if (state->db->state->global_state.header.last_tx_id >=
        maybe_tx->tx_id) { // probably old tx
      size_t remaining = maybe_tx->tx_size % PAGE_SIZE
                             ? PAGE_SIZE - (maybe_tx->tx_size % PAGE_SIZE)
                             : 0;

      wal_increment_next_range_start(state,
                                     maybe_tx->tx_size + remaining - PAGE_SIZE);
      continue;
    }

    ssize_t corrupted_pos = current - state->wal->map.address;
    wal_tx_t *corrupted_tx = current;

    failed(ENODATA,
           msg("Found valid transaction after rejecting (smaller) invalid "
               "transaction. WAL is probably corrupted"),
           with(corrupted_pos, "%zd"), with(maybe_tx->tx_id, "%lu"),
           with(corrupted_tx->tx_id, "%lu"),
           with(state->db->state->global_state.header.last_tx_id, "%lu"));
  }
  return success();
}
// end::wal_recover_validate_after[]

// tag::wal_get_transaction[]
static result_t wal_get_transaction(struct wal_recovery_operation *state,
                                    wal_tx_t **txp) {
  // <1>
  wal_tx_t *wt = state->start;
  if (wt->flags == wal_tx_flags_none) {
    *txp = wt;
    state->tx_buffer_used = wt->tx_size;
    return success();
  }
  // <2>
  size_t required_size =
      ZSTD_getDecompressedSize(state->start + sizeof(wal_tx_t),
                               wt->tx_size - sizeof(wal_tx_t)) +
      sizeof(wal_tx_t);
  if (required_size > state->tx_buffer_size) {
    void *r = realloc(state->tx_buffer, required_size);
    ensure(r, msg("Unable to allocate memory to decompress transaction"),
           with(required_size, "%lu"));

    state->tx_buffer = r;
    state->tx_buffer_size = required_size;
  }

  // <3>
  size_t res = ZSTD_decompress(
      state->tx_buffer + sizeof(wal_tx_t), required_size - sizeof(wal_tx_t),
      state->start + sizeof(wal_tx_t), wt->tx_size - sizeof(wal_tx_t));
  if (ZSTD_isError(res)) {
    const char *zstd_error = ZSTD_getErrorName(res);
    failed(ENODATA, msg("Failed to decompress transaction"),
           with(wt->tx_id, "%lu"), with(zstd_error, "%s"));
  }
  // <4>
  memcpy(state->tx_buffer, state->start, sizeof(wal_tx_t));
  *txp = state->tx_buffer;
  state->tx_buffer_used = res + sizeof(wal_tx_t);
  return success();
}
// end::wal_get_transaction[]

// tag::wal_recover[]

static result_t disable_db_mmap_writes(struct mmap_args *m) {
  // no way to report it, the errors_get_count() will be triggered
  return palfs_disable_writes(m->address, m->size);
}
enable_defer(disable_db_mmap_writes);

static void init_wal_recovery(db_t *db, wal_state_t *wal,
                              struct wal_recovery_operation *state) {
  memset(state, 0, sizeof(struct wal_recovery_operation));
  state->start = wal->map.address;
  state->db = db;
  state->wal = wal;
  state->end = state->start + wal->map.size;
  state->last_recovered_tx_id = 0;
}

static result_t wal_next_valid_transaction(struct wal_recovery_operation *state,
                                           wal_tx_t **txp) {
  if (state->start >= state->end ||
      !wal_validate_transaction(state->db, state->start, state->end)) {
    *txp = 0;
    state->wal->last_write_pos =
        (uint64_t)(state->start - state->wal->map.address);

    return success();
  }
  ensure(wal_get_transaction(state, txp));
  wal_tx_t *tx = *txp;
  state->last_recovered_tx_id = tx->tx_id;

  size_t remaining =
      tx->tx_size % PAGE_SIZE ? PAGE_SIZE - (tx->tx_size % PAGE_SIZE) : 0;
  state->start = state->start + tx->tx_size + remaining;
  return success();
}

static result_t free_wal_recovery(struct wal_recovery_operation *state) {
  free(state->tx_buffer);
  return success();
}
enable_defer(free_wal_recovery);

static result_t wal_complete_recovery(struct wal_recovery_operation *state) {
  if (state->last_recovered_tx_id != 0) {
    page_metadata_t *first_page_metadata =
        state->db->state->global_state.mmap.address;

    ensure(first_page_metadata->type == page_metadata,
           msg("First page was not a metadata page?"));
    ensure(first_page_metadata->file_header.last_tx_id ==
               state->last_recovered_tx_id,
           msg("Unable to match recovered transaction ids"),
           with(first_page_metadata->file_header.last_tx_id, "%lu"),
           with(state->last_recovered_tx_id, "%lu"));
    state->db->state->global_state.header = first_page_metadata->file_header;
  } else {
    // empty db?
    state->db->state->global_state.header.number_of_pages =
        state->db->state->global_state.mmap.size / PAGE_SIZE;
  }

  state->db->state->default_read_tx->global_state =
      state->db->state->global_state;
  return success();
}

static result_t wal_recover(db_t *db, wal_state_t *wal) {
  ensure(palfs_enable_writes(db->state->global_state.mmap.address,
                             db->state->global_state.mmap.size));
  defer(disable_db_mmap_writes, &db->state->global_state.mmap);

  struct wal_recovery_operation recovery_state;
  init_wal_recovery(db, wal, &recovery_state);
  defer(free_wal_recovery, &recovery_state);

  while (true) {
    wal_tx_t *tx;
    ensure(wal_next_valid_transaction(&recovery_state, &tx));
    if (!tx)
      break;
    void *input = (void *)tx + sizeof(wal_tx_t) +
                  sizeof(wal_tx_page_t) * tx->number_of_modified_pages;
    for (size_t i = 0; i < tx->number_of_modified_pages; i++) {
      page_t modified_page = {.page_num = tx->pages[i].page_num,
                              .overflow_size =
                                  tx->pages[i].number_of_pages * PAGE_SIZE};
      ensure(pages_get(&db->state->global_state, &modified_page));

      if (tx->pages[i].flags == wal_tx_page_flags_diff) {
        size_t end_offset = i + 1 < tx->number_of_modified_pages
                                ? tx->pages[i + 1].offset
                                : recovery_state.tx_buffer_used;
        input = wal_apply_diff(input, recovery_state.tx_buffer + end_offset,
                               &modified_page);
      } else {
        memcpy(modified_page.address, input, modified_page.overflow_size);
        input += modified_page.overflow_size;
      }
    }
  }
  // we need to check if there are valid transactions _after_ where we stopped
  ensure(wal_recover_validate_after_end_of_transactions(&recovery_state));

  ensure(wal_complete_recovery(&recovery_state));

  return success();
}

// end::wal_recover[]

// tag::wal_open_and_recover[]
result_t wal_open_and_recover(db_t *db) {
  const char *db_file_name = palfs_get_filename(db->state->handle);
  size_t db_name_len = strlen(db_file_name);
  char *wal_file_name = malloc(db_name_len + 1 + 4); // \0 + .wal
  ensure(wal_file_name, msg("Unable to allocate WAL file name"),
         with(db_file_name, "%s"));
  defer(free, wal_file_name);

  memcpy(wal_file_name, db_file_name, db_name_len);
  memcpy(wal_file_name + db_name_len, ".wal", 5); // include \0

  size_t handle_len;
  ensure(palfs_compute_handle_size(wal_file_name, &handle_len));
  handle_len += sizeof(wal_state_t);

  wal_state_t *wal = calloc(1, handle_len);
  ensure(wal, msg("Unable to allocate WAL state"), with(db_file_name, "%s"));
  size_t cancel_defer = 0;
  try_defer(free, wal, cancel_defer);
  wal->handle = (void *)(wal + 1);
  ensure(palfs_create_file(wal_file_name, wal->handle,
                           palfs_file_creation_flags_durable));
  ensure(palfs_set_file_minsize(wal->handle, db->state->options.wal_size));
  ensure(palfs_get_filesize(wal->handle, &wal->map.size));
  ensure(palfs_mmap(wal->handle, 0, &wal->map));
  defer(palfs_unmap, &wal->map);

  ensure(wal_recover(db, wal));

  db->state->wal_state = wal;
  cancel_defer = 1;

  return success();
}
// end::wal_open_and_recover[]

result_t wal_close(db_state_t *db) {
  if (!db || !db->wal_state)
    return success();
  // need to proceed even if there are failures
  bool failure = !palfs_unmap(&db->wal_state->map);
  failure |= !palfs_close_file(db->wal_state->handle);

  if (failure) {
    errors_push(EIO, msg("Unable to properly close the wal"));
  }

  free(db->wal_state);
  db->wal_state = 0;
  if (failure) {
    return failure_code();
  }
  return success();
}

// tag::wal_setup_transaction_data[]
static void *wal_setup_transaction_data(txn_state_t *tx, wal_tx_t *wt,
                                        void *output) {
  size_t number_of_buckets = get_number_of_buckets(tx);
  size_t index = 0;
  for (size_t i = 0; i < number_of_buckets; i++) {
    page_t *entry = &tx->entries[i];
    if (!entry->address)
      continue;

    wt->pages[index].number_of_pages = size_to_pages(entry->overflow_size);
    wt->pages[index].page_num = entry->page_num;

    size_t size = wt->pages[index].number_of_pages * PAGE_SIZE;
    void *end = wal_diff_page(entry->previous, entry->address,
                              size / sizeof(uint64_t), output);

    wt->pages[index].offset = (uint64_t)(output - (void *)wt);
    wt->pages[index].flags = (size == (size_t)(end - output))
                                 ? wal_tx_page_flags_none
                                 : wal_tx_page_flags_diff;
    output = end;
    index++;
  }
  return output;
}
// end::wal_setup_transaction_data[]

// tag::wal_compress_transaction[]
static void *wal_compress_transaction(wal_tx_t *wt, void *start, void *end,
                                      void *buffer_end) {
  // <1>
  size_t input_size = (size_t)(end - start);
  size_t required_size = ZSTD_compressBound(input_size);
  void *buffer_to_free = 0;
  void *buffer_to_work;
  // <2>
  if (required_size > (size_t)(buffer_end - end)) {
    // need to allocate separately
    buffer_to_work = buffer_to_free = malloc(required_size);
    if (!buffer_to_work) {
      // no memory, so let's just return uncompressed.
      return end;
    }
  } else {
    buffer_to_work = end;
  }
  // <3>
  defer(free, buffer_to_free); // noop if 0

  // <4>
  size_t res =
      ZSTD_compress(buffer_to_work, required_size, start, input_size, 0);
  if (ZSTD_isError(res) || // we got an error, let's just return uncompressed
      res >= input_size) { // compressed bigger than input? skip it

    return end;
  }
  wt->flags = wal_tx_flags_compressed;
  // <5>
  memmove(start, buffer_to_work, res);
  return start + res;
}
// end::wal_compress_transaction[]

// tag::wal_append[]
result_t wal_append(txn_state_t *tx) {
  wal_state_t *wal = tx->db->wal_state;
  // <1>
  size_t tx_header_size =
      sizeof(wal_tx_t) + tx->modified_pages * sizeof(wal_tx_page_t);
  uint32_t required_pages =
      (uint32_t)size_to_pages(tx_header_size) + tx->modified_pages;

  // <2>
  void *tmp_buf;
  ensure(palmem_allocate_pages(&tmp_buf, required_pages));
  memset(tmp_buf, 0, required_pages * PAGE_SIZE);
  defer(palmem_free_page, &tmp_buf);
  wal_tx_t *wt = tmp_buf;
  wt->number_of_modified_pages = tx->modified_pages;
  wt->tx_id = tx->global_state.header.last_tx_id;

  // <3>
  void *end = wal_setup_transaction_data(tx, wt, tmp_buf + tx_header_size);
  end = wal_compress_transaction(wt, tmp_buf + sizeof(wal_tx_t), end,
                                 tmp_buf + required_pages * PAGE_SIZE);

  wt->tx_size = (uint64_t)(end - tmp_buf);
  size_t remaining =
      wt->tx_size % PAGE_SIZE ? PAGE_SIZE - (wt->tx_size % PAGE_SIZE) : 0;
  // <4>
  memset(end, 0, remaining);

  size_t page_aligned_tx_size = wt->tx_size + remaining;

  // <5>
  ensure(!crypto_generichash(wt->tx_hash_sodium, crypto_generichash_BYTES,
                             (const uint8_t *)wt + crypto_generichash_BYTES,
                             page_aligned_tx_size - crypto_generichash_BYTES, 0,
                             0),
         msg("Unable to compute hash for transaction"));

  // <6>
  ensure(palfs_write_file(wal->handle, wal->last_write_pos, tmp_buf,
                          page_aligned_tx_size));
  wal->last_write_pos += page_aligned_tx_size;
  return success();
}
// end::wal_append[]

// tag::wal_will_checkpoint[]
bool wal_will_checkpoint(db_state_t *db, uint64_t tx_id) {
  if (!db || !db->wal_state)
    return false;

  return db->global_state.header.last_tx_id == tx_id &&
         db->wal_state->last_write_pos > db->options.wal_size / 2;
}
// end::wal_will_checkpoint[]

// tag::wal_checkpoint[]
result_t wal_checkpoint(db_state_t *db, uint64_t tx_id) {
  if (!wal_will_checkpoint(db, tx_id)) {
    return success();
  }
  void *zero;
  ensure(palmem_allocate_pages(&zero, 1),
         msg("Unable to allocate page for WAL reset"));
  defer(palmem_free_page, &zero);
  memset(zero, 0, PAGE_SIZE);
  wal_tx_t *wt = zero;
  wt->tx_id = tx_id;
  wt->tx_size = PAGE_SIZE;
  wt->number_of_modified_pages = 0;
  ensure(!crypto_generichash(wt->tx_hash_sodium, crypto_generichash_BYTES,
                             zero + crypto_generichash_BYTES,
                             PAGE_SIZE - crypto_generichash_BYTES, 0, 0),
         msg("Unable to compute hash for transaction"));
  // reset the start of the log, preventing recovering from proceeding
  ensure(palfs_write_file(db->wal_state->handle, 0, zero, PAGE_SIZE),
         msg("Unable to reset WAL first page"));
  db->wal_state->last_write_pos = PAGE_SIZE;
  return success();
}
// end::wal_checkpoint[]

uint64_t TEST_wal_get_last_write_position(db_t *db) {
  return db->state->wal_state->last_write_pos;
}
