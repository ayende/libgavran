#include <sodium.h>
#include <stdalign.h>
#include <string.h>
#include <sys/param.h>
#include <zstd.h>

#include "impl.h"
#include "platform.mem.h"

enum wal_txn_page_flags {
  wal_txn_page_flags_none = 0,
  wal_txn_page_flags_diff = 1,
};

typedef struct wal_txn_page {
  uint64_t page_num;
  uint64_t offset;
  uint32_t number_of_pages;
  uint32_t flags;
} wal_txn_page_t;

// tag::wal_txn_t[]
enum wal_txn_flags {
  wal_txn_flags_none = 0,
  wal_txn_flags_compressed = 1,
};

typedef struct wal_txn {
  uint8_t tx_hash_sodium[crypto_generichash_BYTES];
  uint64_t tx_id;
  uint64_t tx_size;
  uint32_t number_of_modified_pages;
  enum wal_txn_flags flags;
  struct wal_txn_page pages[];
} wal_txn_t;
// end::wal_txn_t[]

// tag::wal_page_diff[]
typedef struct wal_page_diff {
  uint32_t offset;
  int32_t length;  // negative means zero filled
} wal_page_diff_t;
// end::wal_page_diff[]

// tag::wal_recovery_operation[]
struct wal_recovery_operation {
  db_t *db;
  wal_state_t *wal;
  // <4>
  struct wal_file_state *files[2];
  // <5>
  size_t current_recovery_file_index;
  void *start;
  void *end;
  uint64_t last_recovered_tx_id;
  void *tx_buffer;
  size_t tx_buffer_size;
  size_t tx_buffer_used;
};
// end::wal_recovery_operation[]

static void *wal_apply_diff(void *input, void *input_end,
                            page_t *page) {
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
      zeroes &= modified[i] == 0;  // single diff size limited to 8MB
      if (origin[i] == modified[i]) {
        if (zeroes)
          continue;  // we'll try to extend zero filled ranges
        break;
      }
    }
    if (i == size) i--;  // reached the end of the buffer, go back

    int32_t len = (int32_t)((i - diff_start) * sizeof(uint64_t));
    wal_page_diff_t diff = {
        .offset = (uint32_t)(diff_start * sizeof(uint64_t)),
        .length = len};
    if (zeroes) {
      diff.length = -diff.length;  // indicates zero fill
    }
    void *required_write = current + sizeof(wal_page_diff_t) +
                           (diff.length > 0 ? diff.length : 0);
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
static bool wal_validate_transaction(void *start, void *end) {
  wal_txn_t *tx = start;
  if (start + tx->tx_size > end) return false;
  if (tx->tx_size <= crypto_generichash_BYTES) return false;
  uint8_t hash[crypto_generichash_BYTES];
  size_t remaining = tx->tx_size % PAGE_SIZE
                         ? PAGE_SIZE - (tx->tx_size % PAGE_SIZE)
                         : 0;
  if (start + tx->tx_size + remaining > end) return false;
  if (crypto_generichash(
          (uint8_t *)hash, crypto_generichash_BYTES,
          (const uint8_t *)tx + crypto_generichash_BYTES,
          tx->tx_size - crypto_generichash_BYTES + remaining, 0, 0))
    return false;

  return sodium_compare(hash, tx->tx_hash_sodium,
                        crypto_generichash_BYTES) == 0;
}
// end::wal_validate_transaction[]

// tag::wal_range[]
static result_t wal_get_next_range(
    struct wal_recovery_operation *state, void **current,
    void **end) {
  *end = state->end;
  if (state->start >= state->end) {
    *current = 0;
    return success();
  }
  *current = state->start;
  return success();
}

static void wal_increment_next_range_start(
    struct wal_recovery_operation *state, size_t amount) {
  state->start += amount;
}
// end::wal_range[]

// tag::wal_validate_after_end_of_transactions[]
static result_t wal_validate_after_end_of_transactions(
    struct wal_recovery_operation *state) {
  while (true) {
    // <1>
    void *current;
    void *end;
    ensure(wal_get_next_range(state, &current, &end));
    if (!current) break;

    // <2>
    if (!wal_validate_transaction(current, end)) {
      // <3>
      wal_increment_next_range_start(state, PAGE_SIZE);
      continue;
    }

    // <4>
    wal_txn_t *maybe_tx = current;
    if (state->last_recovered_tx_id >
        maybe_tx->tx_id) {  // probably old tx
      size_t remaining =
          maybe_tx->tx_size % PAGE_SIZE
              ? PAGE_SIZE - (maybe_tx->tx_size % PAGE_SIZE)
              : 0;
      // <5>
      wal_increment_next_range_start(state,
                                     maybe_tx->tx_size + remaining);
      continue;
    }

    // <6>
    ssize_t corrupted_pos =
        current -
        state->files[state->current_recovery_file_index]->map.address;
    wal_txn_t *corrupted_tx = current;

    failed(ENODATA,
           msg("Found valid transaction after rejecting (smaller) "
               "invalid "
               "transaction. WAL is probably corrupted"),
           with(corrupted_pos, "%zd"), with(maybe_tx->tx_id, "%lu"),
           with(corrupted_tx->tx_id, "%lu"),
           with(state->db->state->global_state.header.last_tx_id,
                "%lu"));
  }
  return success();
}
// end::wal_validate_after_end_of_transactions[]

// tag::wal_get_transaction[]
static result_t wal_get_transaction(
    struct wal_recovery_operation *state, wal_txn_t **txp) {
  // <1>
  wal_txn_t *wt = state->start;
  if (wt->flags == wal_tx_flags_none) {
    *txp = wt;
    state->tx_buffer_used = wt->tx_size;
    return success();
  }
  // <2>
  size_t required_size =
      ZSTD_getDecompressedSize(state->start + sizeof(wal_txn_t),
                               wt->tx_size - sizeof(wal_txn_t)) +
      sizeof(wal_txn_t);
  if (required_size > state->tx_buffer_size) {
    void *r = realloc(state->tx_buffer, required_size);
    ensure(r,
           msg("Unable to allocate memory to decompress transaction"),
           with(required_size, "%lu"));

    state->tx_buffer = r;
    state->tx_buffer_size = required_size;
  }

  // <3>
  size_t res = ZSTD_decompress(state->tx_buffer + sizeof(wal_txn_t),
                               required_size - sizeof(wal_txn_t),
                               state->start + sizeof(wal_txn_t),
                               wt->tx_size - sizeof(wal_txn_t));
  if (ZSTD_isError(res)) {
    const char *zstd_error = ZSTD_getErrorName(res);
    failed(ENODATA, msg("Failed to decompress transaction"),
           with(wt->tx_id, "%lu"), with(zstd_error, "%s"));
  }
  // <4>
  memcpy(state->tx_buffer, state->start, sizeof(wal_txn_t));
  *txp = state->tx_buffer;
  state->tx_buffer_used = res + sizeof(wal_txn_t);
  return success();
}
// end::wal_get_transaction[]

static result_t disable_db_mmap_writes(struct mmap_args *m) {
  // no way to report it, the errors_get_count() will be triggered
  return palfs_disable_writes(m->address, m->size);
}
enable_defer(disable_db_mmap_writes);
// tag::wal_init_recover_state[]
static void wal_init_recover_state(
    db_t *db, wal_state_t *wal,
    struct wal_recovery_operation *state) {
  memset(state, 0, sizeof(struct wal_recovery_operation));
  state->db = db;
  state->wal = wal;
  state->last_recovered_tx_id = 0;
  // find the appropriate order to scan through the WAL records
  uint64_t tx_ids[2] = {0, 0};
  for (size_t i = 0; i < 2; i++) {
    void *start = wal->files[i].map.address;
    void *end = start + wal->files[i].map.size;
    if (wal_validate_transaction(start, end)) {
      wal_txn_t *tx = wal->files[i].map.address;
      if (tx->number_of_modified_pages) {
        // the first transaction is real, need to go over the WAL file
        // txs
        tx_ids[i] = tx->tx_id;
      } else {
        // the first transaction is a marker, just recover the tx_id
        // from it
        state->last_recovered_tx_id =
            MAX(state->last_recovered_tx_id, tx->tx_id);
      }
    }
  }
  if (!tx_ids[0]) {
    state->current_recovery_file_index = 1;
    wal->current_append_file_index = 0;
    if (!tx_ids[1]) {
      // both are empty? Nothing to be done, then
      state->start = state->end = 0;
      return;  // will return nothing here
    }
    state->files[0] = 0;
    state->files[1] = &wal->files[1];
  } else if (!tx_ids[1]) {
    state->current_recovery_file_index = 1;
    state->files[0] = 0;
    state->files[1] = &wal->files[0];
  } else {
    state->current_recovery_file_index = 0;
    if (tx_ids[0] < tx_ids[1]) {
      wal->current_append_file_index = 1;
      state->files[0] = &wal->files[0];
      state->files[1] = &wal->files[1];
    } else {
      wal->current_append_file_index = 0;
      state->files[0] = &wal->files[1];
      state->files[1] = &wal->files[0];
    }
  }

  state->start =
      state->files[state->current_recovery_file_index]->map.address;
  state->end =
      state->start +
      state->files[state->current_recovery_file_index]->map.size;
}
// end::wal_init_recover_state[]

// tag::wal_next_valid_transaction[]
static result_t wal_next_valid_transaction(
    struct wal_recovery_operation *state, wal_txn_t **txp) {
  // <1>
  if (state->start >= state->end ||
      !wal_validate_transaction(state->start, state->end) ||
      state->last_recovered_tx_id >=
          ((wal_txn_t *)state->start)->tx_id) {
    void *end_of_valid_tx = state->start;
    // <2>
    // we need to check if there are valid transactions _after_ where
    // we stopped running through the valid transactions in the file
    ensure(wal_validate_after_end_of_transactions(state));

    // <3>
    if (state->current_recovery_file_index) {
      *txp = 0;
      if (state->files[1]) {
        state->files[1]->last_write_pos = (uint64_t)(
            end_of_valid_tx - state->files[1]->map.address);
      }
      return success();
    }
    // <4>
    state->files[0]->last_write_pos =
        (uint64_t)(end_of_valid_tx - state->files[0]->map.address);

    state->current_recovery_file_index++;
    state->start = state->files[1]->map.address;
    state->end = state->start + state->files[1]->map.size;
    // <5>
    return wal_next_valid_transaction(state, txp);
  }

  // <6>
  ensure(wal_get_transaction(state, txp));
  wal_txn_t *tx = *txp;
  state->last_recovered_tx_id = tx->tx_id;

  size_t remaining = tx->tx_size % PAGE_SIZE
                         ? PAGE_SIZE - (tx->tx_size % PAGE_SIZE)
                         : 0;
  state->start = state->start + tx->tx_size + remaining;
  return success();
}
// end::wal_next_valid_transaction[]

// tag::free_wal_recovery[]
static result_t free_wal_recovery(
    struct wal_recovery_operation *state) {
  free(state->tx_buffer);
  return success();
}
enable_defer(free_wal_recovery);
// end::free_wal_recovery[]

// tag::wal_complete_recovery[]
static result_t wal_complete_recovery(
    struct wal_recovery_operation *state) {
  page_metadata_t *first_page_metadata =
      state->db->state->global_state.mmap.address;
  state->db->state->global_state.header =
      first_page_metadata->file_header;
  if (state->last_recovered_tx_id == 0) {  // empty db, probably
    state->db->state->global_state.header.number_of_pages =
        state->db->state->global_state.mmap.size / PAGE_SIZE;
  } else {
    ensure(first_page_metadata->type == page_metadata,
           msg("First page was not a metadata page?"));
  }
  state->db->state->global_state.header.last_tx_id =
      state->last_recovered_tx_id;
  state->db->state->default_read_tx->global_state =
      state->db->state->global_state;
  return success();
}
// end::wal_complete_recovery[]

// tag::wal_recover[]
static result_t wal_recover(db_t *db, wal_state_t *wal) {
  ensure(palfs_enable_writes(db->state->global_state.mmap.address,
                             db->state->global_state.mmap.size));
  defer(disable_db_mmap_writes, &db->state->global_state.mmap);

  struct wal_recovery_operation recovery_state;
  wal_init_recover_state(db, wal, &recovery_state);
  defer(free_wal_recovery, &recovery_state);

  while (true) {
    wal_txn_t *tx;
    ensure(wal_next_valid_transaction(&recovery_state, &tx));
    if (!tx) break;
    void *input =
        (void *)tx + sizeof(wal_txn_t) +
        sizeof(wal_tx_page_t) * tx->number_of_modified_pages;
    for (size_t i = 0; i < tx->number_of_modified_pages; i++) {
      page_t modified_page = {
          .page_num = tx->pages[i].page_num,
          .overflow_size = tx->pages[i].number_of_pages * PAGE_SIZE};
      ensure(pages_get(&db->state->global_state, &modified_page));

      if (tx->pages[i].flags == wal_tx_page_flags_diff) {
        size_t end_offset = i + 1 < tx->number_of_modified_pages
                                ? tx->pages[i + 1].offset
                                : recovery_state.tx_buffer_used;
        input = wal_apply_diff(input,
                               recovery_state.tx_buffer + end_offset,
                               &modified_page);
      } else {
        memcpy(modified_page.address, input,
               modified_page.overflow_size);
        input += modified_page.overflow_size;
      }
    }
  }

  ensure(wal_complete_recovery(&recovery_state));

  return success();
}
// end::wal_recover[]

// tag::wal_open_and_recover[]
static result_t wal_open_single_file(
    struct wal_file_state *file_state, db_t *db, char wal_code) {
  const char *db_file_name = palfs_get_filename(db->state->handle);
  size_t db_name_len = strlen(db_file_name);
  char *wal_file_name = malloc(db_name_len + 1 + 6);  // \0 + -a.wal
  ensure(wal_file_name, msg("Unable to allocate WAL file name"),
         with(db_file_name, "%s"));
  defer(free, wal_file_name);

  memcpy(wal_file_name, db_file_name, db_name_len);
  wal_file_name[db_name_len++] = '-';
  wal_file_name[db_name_len++] = wal_code;
  memcpy(wal_file_name + db_name_len, ".wal", 5);  // include \0

  size_t handle_len;
  ensure(palfs_compute_handle_size(wal_file_name, &handle_len));

  void *handle = malloc(handle_len);
  ensure(handle, msg("Unable to allocate file handle for WAL"),
         with(db_file_name, "%s"));
  size_t cancel_defer = 0;
  try_defer(free, handle, cancel_defer);
  file_state->handle = handle;
  ensure(palfs_create_file(wal_file_name, file_state->handle,
                           palfs_file_creation_flags_durable));
  ensure(palfs_set_file_minsize(file_state->handle,
                                db->state->options.wal_size));
  ensure(
      palfs_get_filesize(file_state->handle, &file_state->map.size));
  ensure(palfs_mmap(file_state->handle, 0, &file_state->map));

  cancel_defer = 1;
  return success();
}

result_t wal_open_and_recover(db_t *db) {
  memset(&db->state->wal_state, 0, sizeof(wal_state_t));
  wal_state_t *wal = &db->state->wal_state;

  ensure(wal_open_single_file(&wal->files[0], db, 'a'));
  defer(palfs_unmap, &wal->files[0].map);
  ensure(wal_open_single_file(&wal->files[1], db, 'b'));
  defer(palfs_unmap, &wal->files[1].map);

  return wal_recover(db, wal);
}
// end::wal_open_and_recover[]

result_t wal_close(db_state_t *db) {
  if (!db) return success();
  // need to proceed even if there are failures
  bool failure = false;
  for (size_t i = 0; i < 2; i++) {
    failure = !palfs_unmap(&db->wal_state.files[i].map);
    failure |= !palfs_close_file(db->wal_state.files[i].handle);
    free(db->wal_state.files[i].handle);
  }

  if (failure) {
    errors_push(EIO, msg("Unable to properly close the wal"));
  }

  memset(&db->wal_state, 0, sizeof(wal_state_t));
  if (failure) {
    return failure_code();
  }
  return success();
}

// tag::wal_setup_transaction_data[]
static void *wal_setup_transaction_data(txn_state_t *tx,
                                        wal_txn_t *wt, void *output) {
  size_t number_of_buckets = get_number_of_buckets(tx);
  size_t index = 0;
  for (size_t i = 0; i < number_of_buckets; i++) {
    page_t *entry = &tx->entries[i];
    if (!entry->address) continue;

    wt->pages[index].number_of_pages =
        size_to_pages(entry->overflow_size);
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
static void *wal_compress_transaction(wal_txn_t *wt, void *start,
                                      void *end, void *buffer_end) {
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
  defer(free, buffer_to_free);  // noop if 0

  // <4>
  size_t res = ZSTD_compress(buffer_to_work, required_size, start,
                             input_size, 0);
  if (ZSTD_isError(
          res) ||  // we got an error, let's just return uncompressed
      res >= input_size) {  // compressed bigger than input? skip it

    return end;
  }
  wt->flags = wal_tx_flags_compressed;
  // <5>
  memmove(start, buffer_to_work, res);
  return start + res;
}
// end::wal_compress_transaction[]

static result_t wal_prepare_txn_buffer(txn_state_t *tx,
                                       void **txn_buffer,
                                       size_t *txn_size) {
  size_t tx_header_size =
      sizeof(wal_txn_t) + tx->modified_pages * sizeof(wal_tx_page_t);
  uint32_t required_pages =
      (uint32_t)size_to_pages(tx_header_size) + tx->modified_pages;

  // <2>
  void *tmp_buf;
  ensure(palmem_allocate_pages(&tmp_buf, required_pages));
  memset(tmp_buf, 0, required_pages * PAGE_SIZE);
  size_t cancel_defer = 0;
  try_defer(palmem_free_page, &tmp_buf, cancel_defer);
  wal_txn_t *wt = tmp_buf;
  wt->number_of_modified_pages = tx->modified_pages;
  wt->tx_id = tx->global_state.header.last_tx_id;

  // <3>
  void *end =
      wal_setup_transaction_data(tx, wt, tmp_buf + tx_header_size);
  end =
      wal_compress_transaction(wt, tmp_buf + sizeof(wal_txn_t), end,
                               tmp_buf + required_pages * PAGE_SIZE);

  wt->tx_size = (uint64_t)(end - tmp_buf);
  size_t remaining = wt->tx_size % PAGE_SIZE
                         ? PAGE_SIZE - (wt->tx_size % PAGE_SIZE)
                         : 0;
  // <4>
  memset(end, 0, remaining);

  size_t page_aligned_tx_size = wt->tx_size + remaining;

  // <5>
  ensure(!crypto_generichash(
             wt->tx_hash_sodium, crypto_generichash_BYTES,
             (const uint8_t *)wt + crypto_generichash_BYTES,
             page_aligned_tx_size - crypto_generichash_BYTES, 0, 0),
         msg("Unable to compute hash for transaction"));

  *txn_buffer = tmp_buf;
  *txn_size = page_aligned_tx_size;
  cancel_defer = 1;
  return success();
}

// tag::wal_append[]
result_t wal_append(txn_state_t *tx) {
  void *txn_buffer;
  size_t txn_size;
  ensure(wal_prepare_txn_buffer(tx, &txn_buffer, &txn_size));
  defer(free, txn_buffer);

  wal_state_t *wal = &tx->db->wal_state;

  struct wal_file_state *cur_file =
      &wal->files[wal->current_append_file_index];
  if (cur_file->last_write_pos + txn_size > cur_file->map.size) {
    // we need to increase the WAL size
    uint64_t next_size =
        MAX(next_power_of_two(cur_file->map.size / 10), txn_size * 2);
    ensure(palfs_set_file_minsize(cur_file->handle, next_size));
    cur_file->map.size = next_size;
  }
  ensure(palfs_write_file(cur_file->handle, cur_file->last_write_pos,
                          txn_buffer, txn_size));
  cur_file->last_write_pos += txn_size;
  cur_file->last_tx_id = tx->global_state.header.last_tx_id;
  return success();
}
// end::wal_append[]

// tag::wal_checkpoint[]
bool wal_will_checkpoint(db_state_t *db, uint64_t tx_id) {
  if (!db) return false;

  size_t cur_file_index = db->wal_state.current_append_file_index;
  size_t other_file_index = (cur_file_index + 1) & 1;
  bool cur_full = db->wal_state.files[cur_file_index].last_write_pos >
                  db->options.wal_size / 2;
  bool other_ready =
      tx_id > db->wal_state.files[other_file_index].last_tx_id;

  return cur_full && other_ready;
}

static result_t wal_reset_file(db_state_t *db,
                               struct wal_file_state *file) {
  void *zero;
  ensure(palmem_allocate_pages(&zero, 1),
         msg("Unable to allocate page for WAL reset"));
  defer(palmem_free_page, &zero);
  memset(zero, 0, PAGE_SIZE);
  wal_txn_t *wt = zero;
  wt->tx_id = file->last_tx_id;
  wt->tx_size = sizeof(wal_txn_t);
  ensure(!crypto_generichash(
             wt->tx_hash_sodium, crypto_generichash_BYTES,
             zero + crypto_generichash_BYTES,
             PAGE_SIZE - crypto_generichash_BYTES, 0, 0),
         msg("Unable to compute hash for transaction"));
  // reset the start of the log, preventing recovery from proceeding
  ensure(palfs_write_file(file->handle, 0, zero, PAGE_SIZE),
         msg("Unable to reset WAL first page"));

  if (file->last_write_pos > db->options.wal_size) {
    ensure(palfs_truncate_file(file->handle, db->options.wal_size));
    file->map.size = db->options.wal_size;
  }
  file->last_write_pos = 0;
  return success();
}

result_t wal_checkpoint(db_state_t *db, uint64_t tx_id) {
  if (!wal_will_checkpoint(db, tx_id)) {
    return success();
  }

  size_t other_index =
      (db->wal_state.current_append_file_index + 1) & 1;
  struct wal_file_state *other = &db->wal_state.files[other_index];

  struct wal_file_state *cur =
      &db->wal_state.files[db->wal_state.current_append_file_index];

  ensure(wal_reset_file(db, other));

  if (tx_id >=
      cur->last_tx_id) {  // can reset the current WAL as well
    ensure(wal_reset_file(db, cur));
  } else {
    // the current log is still in use, switch to the other one
    db->wal_state.current_append_file_index = other_index;
  }

  return success();
}
// end::wal_checkpoint[]

uint64_t TEST_wal_get_last_write_position(db_t *db) {
  wal_state_t *wal = &db->state->wal_state;
  return wal->files[wal->current_append_file_index].last_write_pos;
}