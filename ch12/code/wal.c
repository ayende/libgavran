#include <sodium.h>
#include <stdalign.h>
#include <string.h>
#include <sys/param.h>
#include <zstd.h>

#include "impl.h"
#include "platform.mem.h"

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
  uint8_t tx_hash_sodium[32];
  uint64_t tx_id;
  uint64_t page_aligned_tx_size;
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

// tag::wal_recovery_operation[]
struct wal_recovery_operation {
  db_t *db;
  wal_state_t *wal;
  struct file_recovery_info {
    struct wal_file_state *state;
    void *buffer;
    size_t size;
    size_t used;
  } files[2];
  size_t current_recovery_file_index;
  void *start;
  void *end;
  uint64_t last_recovered_tx_id;
};
// end::wal_recovery_operation[]

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
      memcpy(output, modified, size * sizeof(uint64_t));
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

static size_t wal_page_align_tx_size(wal_tx_t *tx) {
  size_t remaining =
      tx->tx_size % PAGE_SIZE ? PAGE_SIZE - (tx->tx_size % PAGE_SIZE) : 0;
  return tx->tx_size + remaining;
}

// tag::wal_validate_transaction[]
static result_t wal_validate_transaction_hash(void *start, void *end,
                                              wal_tx_t **tx_p) {
  *tx_p = 0;
  wal_tx_t *maybe_tx = start;
  if (start + maybe_tx->page_aligned_tx_size > end ||
      maybe_tx->page_aligned_tx_size <= crypto_generichash_BYTES)
    return success();

  uint8_t hash[crypto_generichash_BYTES];
  if (crypto_generichash(
          (uint8_t *)hash, crypto_generichash_BYTES,
          (const uint8_t *)maybe_tx + crypto_generichash_BYTES,
          maybe_tx->page_aligned_tx_size - crypto_generichash_BYTES, 0, 0))
    return success();

  if (sodium_compare(hash, maybe_tx->tx_hash_sodium,
                     crypto_generichash_BYTES) == 0)
    *tx_p = maybe_tx;
  return success();
}

static result_t wal_decompress_transaction(struct file_recovery_info *file,
                                           wal_tx_t *in, wal_tx_t **txp);

static result_t wal_validate_transaction(struct file_recovery_info *file,
                                         void *start, void *end,
                                         wal_tx_t **tx_p) {
  ensure(wal_validate_transaction_hash(start, end, tx_p),
         msg("Unable to validate transaction hash"));

  if (*tx_p)
    ensure(wal_decompress_transaction(file, *tx_p, tx_p));
  return success();
}

// end::wal_validate_transaction[]

// tag::wal_range[]
static result_t wal_get_next_range(struct wal_recovery_operation *state,
                                   void **current, void **end) {

  *end = state->end;
  if (state->start >= state->end) {
    *current = 0;
    return success();
  }
  *current = state->start;
  return success();
}

static void wal_increment_next_range_start(struct wal_recovery_operation *state,
                                           size_t amount) {
  state->start += amount;
}
// end::wal_range[]

// tag::wal_validate_after_end_of_transactions[]
static result_t
wal_validate_after_end_of_transactions(struct wal_recovery_operation *state) {
  while (true) {
    // <1>
    void *current;
    void *end;
    ensure(wal_get_next_range(state, &current, &end));
    if (!current)
      break;

    // <2>
    wal_tx_t *tx;
    if (!wal_validate_transaction(
            &state->files[state->current_recovery_file_index], current, end,
            &tx) ||
        !tx) {
      // <3>
      wal_increment_next_range_start(state, PAGE_SIZE);
      continue;
    }

    // <4>
    if (state->last_recovered_tx_id > tx->tx_id) { // probably old tx
      // <5>
      wal_increment_next_range_start(state, tx->page_aligned_tx_size);
      continue;
    }

    // <6>
    ssize_t corrupted_pos =
        current -
        state->files[state->current_recovery_file_index].state->map.address;
    wal_tx_t *corrupted_tx = current;

    failed(ENODATA,
           msg("Found valid transaction after rejecting (smaller) invalid "
               "transaction. WAL is probably corrupted"),
           with(corrupted_pos, "%zd"), with(tx->tx_id, "%lu"),
           with(corrupted_tx->tx_id, "%lu"),
           with(state->db->state->global_state.header.last_tx_id, "%lu"));
  }
  return success();
}
// end::wal_validate_after_end_of_transactions[]

// tag::wal_get_transaction[]
static result_t wal_decompress_transaction(struct file_recovery_info *file,
                                           wal_tx_t *in, wal_tx_t **txp) {
  // <1>
  if (in->flags == wal_tx_flags_none) {
    *txp = in;
    return success();
  }
  // <2>
  size_t required_size =
      ZSTD_getDecompressedSize((void *)in + sizeof(wal_tx_t),
                               in->tx_size - sizeof(wal_tx_t)) +
      sizeof(wal_tx_t);
  size_t buffer_offset = 0;
  if (in == file->buffer) { // if the tx is already in the buffer...
    buffer_offset = file->used;
  }
  if (required_size + buffer_offset > file->size) {
    void *r = realloc(file->buffer, buffer_offset + required_size);
    ensure(r, msg("Unable to allocate memory to decompress transaction"),
           with(required_size, "%lu"));
    if (in == file->buffer) { // if the tx is already in the buffer...
      in = r;
    }
    file->buffer = r;
    file->size = required_size + buffer_offset;
  }

  // <3>
  size_t res = ZSTD_decompress(file->buffer + buffer_offset + sizeof(wal_tx_t),
                               required_size - sizeof(wal_tx_t),
                               (void *)in + sizeof(wal_tx_t),
                               in->tx_size - sizeof(wal_tx_t));
  if (ZSTD_isError(res)) {
    const char *zstd_error = ZSTD_getErrorName(res);
    failed(ENODATA, msg("Failed to decompress transaction"),
           with(in->tx_id, "%lu"), with(zstd_error, "%s"));
  }
  // <4>
  memcpy(file->buffer + buffer_offset, in, sizeof(wal_tx_t));
  *txp = file->buffer + buffer_offset;
  file->used = res + sizeof(wal_tx_t) + buffer_offset;
  return success();
}
// end::wal_get_transaction[]

static result_t disable_db_mmap_writes(struct mmap_args *m) {
  if (!m->address)
    return success();
  // no way to report it, the errors_get_count() will be triggered
  return palfs_disable_writes(m->address, m->size);
}
enable_defer(disable_db_mmap_writes);

// tag::wal_init_recover_state[]
static void wal_init_recover_state(db_t *db, wal_state_t *wal,
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
    wal_tx_t *tx;
    if (wal_validate_transaction(&state->files[i], start, end, &tx) && tx) {
      if (tx->number_of_modified_pages) {
        // the first transaction is real, need to go over the WAL file txs
        tx_ids[i] = tx->tx_id;
      } else {
        // the first transaction is a marker, just recover the tx_id from it
        state->last_recovered_tx_id =
            MAX(state->last_recovered_tx_id, tx->tx_id);
      }
    }
  }
  errors_clear(); // errors are expected here, txs might not pass validation
  if (!tx_ids[0]) {
    state->current_recovery_file_index = 1;
    wal->current_append_file_index = 0;
    if (!tx_ids[1]) {
      // both are empty? Nothing to be done, then
      state->start = state->end = 0;
      return; // will return nothing here
    }
    state->files[0].state = 0;
    state->files[1].state = &wal->files[1];
  } else if (!tx_ids[1]) {
    state->current_recovery_file_index = 1;
    state->files[0].state = 0;
    state->files[1].state = &wal->files[0];
  } else {
    state->current_recovery_file_index = 0;
    if (tx_ids[0] < tx_ids[1]) {
      wal->current_append_file_index = 1;
      state->files[0].state = &wal->files[0];
      state->files[1].state = &wal->files[1];
    } else {
      wal->current_append_file_index = 0;
      state->files[0].state = &wal->files[1];
      state->files[1].state = &wal->files[0];
    }
  }

  state->start =
      state->files[state->current_recovery_file_index].state->map.address;
  state->end = state->start +
               state->files[state->current_recovery_file_index].state->map.size;
}
// end::wal_init_recover_state[]

// tag::wal_next_valid_transaction[]
static result_t wal_next_valid_transaction(struct wal_recovery_operation *state,
                                           wal_tx_t **txp) {
  // <1>
  if (state->start >= state->end ||
      !wal_validate_transaction(
          &state->files[state->current_recovery_file_index], state->start,
          state->end, txp) ||
      !*txp || state->last_recovered_tx_id >= (*txp)->tx_id) {

    void *end_of_valid_tx = state->start;
    // <2>
    // we need to check if there are valid transactions _after_ where we stopped
    // running through the valid transactions in the file
    ensure(wal_validate_after_end_of_transactions(state));

    // <3>
    if (state->current_recovery_file_index) {
      *txp = 0;
      if (state->files[1].state) {
        state->files[1].state->last_write_pos =
            (uint64_t)(end_of_valid_tx - state->files[1].state->map.address);
      }
      return success();
    }
    // <4>
    state->files[0].state->last_write_pos =
        (uint64_t)(end_of_valid_tx - state->files[0].state->map.address);

    state->current_recovery_file_index++;
    state->start = state->files[1].state->map.address;
    state->end = state->start + state->files[1].state->map.size;
    // <5>
    return wal_next_valid_transaction(state, txp);
  }

  state->last_recovered_tx_id = (*txp)->tx_id;

  state->start = state->start + (*txp)->page_aligned_tx_size;
  return success();
}
// end::wal_next_valid_transaction[]

// tag::free_wal_recovery[]
static result_t free_wal_recovery(struct wal_recovery_operation *state) {
  for (size_t i = 0; i < 2; i++) {
    free(state->files[i].buffer);
  }

  return success();
}
enable_defer(free_wal_recovery);
// end::free_wal_recovery[]

// tag::wal_complete_recovery[]
static result_t wal_complete_recovery(struct wal_recovery_operation *state) {
  txn_t recovery_tx;
  ensure(txn_create(state->db, TX_READ, &recovery_tx));
  defer(txn_close, &recovery_tx);
  page_t page = {.page_num = 0};
  ensure(txn_get_page(&recovery_tx, &page));

  page_metadata_t *first_page_metadata = page.address;

  state->db->state->global_state.header = first_page_metadata->file_header;
  if (state->last_recovered_tx_id == 0) { // empty db, probably
    state->db->state->global_state.header.number_of_pages =
        state->db->state->global_state.mmap.size / PAGE_SIZE;
  } else {
    ensure(first_page_metadata->type == page_metadata,
           msg("First page was not a metadata page?"));
  }
  ensure(first_page_metadata->file_header.last_tx_id ==
             state->last_recovered_tx_id,
         msg("The last recovered tx id does not match the header tx id"),
         with(first_page_metadata->file_header.last_tx_id, "%lu"),
         with(state->last_recovered_tx_id, "%lu"));

  state->db->state->default_read_tx->global_state =
      state->db->state->global_state;
  return success();
}
// end::wal_complete_recovery[]

// tag::wal_validate_recovered_pages[]
static result_t
wal_validate_recovered_pages(db_t *db, pages_hash_table_t *modified_pages) {
  size_t iter_state = 0;
  page_t *page_to_validate;
  while (hash_get_next(modified_pages, &iter_state, &page_to_validate)) {
    txn_t rtx;
    ensure(txn_create(db, TX_READ, &rtx));
    defer(txn_close, &rtx);
    page_t p = {.page_num = page_to_validate->page_num};
    ensure(txn_get_page(&rtx, &p));
  }
  return success();
}
// end::wal_validate_recovered_pages[]

// tag::wal_recover[]

static result_t wal_recover_tx(db_t *db, wal_tx_t *tx,
                               pages_hash_table_t **modified_pages) {
  void *input = (void *)tx + sizeof(wal_tx_t) +
                sizeof(wal_tx_page_t) * tx->number_of_modified_pages;
  txn_t recovery_tx;
  ensure(txn_create(db, TX_READ, &recovery_tx));
  defer(txn_close, &recovery_tx);

  for (size_t i = 0; i < tx->number_of_modified_pages; i++) {
    ensure(hash_try_add(modified_pages, tx->pages[i].page_num));
    page_t modified_page = {.page_num = tx->pages[i].page_num,
                            .overflow_size =
                                tx->pages[i].number_of_pages * PAGE_SIZE};
    ensure(pages_get(&recovery_tx, &modified_page));

    if (tx->pages[i].flags == wal_tx_page_flags_diff) {
      size_t end_offset = i + 1 < tx->number_of_modified_pages
                              ? tx->pages[i + 1].offset
                              : tx->tx_size;
      input = wal_apply_diff(input, (void *)tx + end_offset, &modified_page);
    } else {

      memcpy(modified_page.address, input, modified_page.overflow_size);
      input += modified_page.overflow_size;
    }
  }

  if (db->state->options.avoid_mmap_io) {
    size_t iter_state = 0;
    page_t *p;
    while (hash_get_next(recovery_tx.working_set, &iter_state, &p)) {
      ensure(palfs_write_file(db->state->handle, p->page_num * PAGE_SIZE,
                              p->address,
                              size_to_pages(p->overflow_size) * PAGE_SIZE));
    }
  }

  return success();
}

static result_t wal_recover(db_t *db, wal_state_t *wal) {
  if (!db->state->options.avoid_mmap_io) {
    ensure(palfs_enable_writes(db->state->global_state.mmap.address,
                               db->state->global_state.mmap.size));
  }
  defer(disable_db_mmap_writes, &db->state->global_state.mmap);
  struct wal_recovery_operation recovery_state;
  wal_init_recover_state(db, wal, &recovery_state);
  defer(free_wal_recovery, &recovery_state);
  pages_hash_table_t *modified_pages;
  ensure(hash_new(16, &modified_pages));
  defer(free_p, &modified_pages);

  while (true) {
    wal_tx_t *tx;
    ensure(wal_next_valid_transaction(&recovery_state, &tx));
    if (!tx)
      break;
    ensure(wal_recover_tx(db, tx, &modified_pages));
  }
  ensure(wal_complete_recovery(&recovery_state));
  ensure(wal_validate_recovered_pages(db, modified_pages));
  return success();
}
// end::wal_recover[]

// tag::wal_open_and_recover[]
static result_t wal_open_single_file(struct wal_file_state *file_state,
                                     db_t *db, char wal_code) {
  const char *db_file_name = palfs_get_filename(db->state->handle);
  size_t db_name_len = strlen(db_file_name);
  char *wal_file_name = malloc(db_name_len + 1 + 6); // \0 + -a.wal
  ensure(wal_file_name, msg("Unable to allocate WAL file name"),
         with(db_file_name, "%s"));
  defer(free, wal_file_name);

  memcpy(wal_file_name, db_file_name, db_name_len);
  wal_file_name[db_name_len++] = '-';
  wal_file_name[db_name_len++] = wal_code;
  memcpy(wal_file_name + db_name_len, ".wal", 5); // include \0

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
  ensure(
      palfs_set_file_minsize(file_state->handle, db->state->options.wal_size));
  ensure(palfs_get_filesize(file_state->handle, &file_state->map.size));
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

  ensure(wal_recover(db, wal));

  return success();
}
// end::wal_open_and_recover[]

result_t wal_close(db_state_t *db) {
  if (!db)
    return success();
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
static void *wal_setup_transaction_data(txn_state_t *tx, wal_tx_t *wt,
                                        void *output) {
  size_t iter_state = 0;
  page_t *entry;
  size_t index = 0;
  while (hash_get_next(tx->pages, &iter_state, &entry)) {
    wt->pages[index].number_of_pages = size_to_pages(entry->overflow_size);
    wt->pages[index].page_num = entry->page_num;

    size_t size = wt->pages[index].number_of_pages * PAGE_SIZE;
    void *end;
    if (tx->db->options.encrypted) {
      memcpy(output, entry->address, size);
      end = output + size;
    } else {
      end = wal_diff_page(entry->previous, entry->address,
                          size / sizeof(uint64_t), output);
    }

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

static result_t wal_prepare_txn_buffer(txn_state_t *tx, wal_tx_t **txn_buffer) {
  size_t tx_header_size =
      sizeof(wal_tx_t) + tx->pages->modified_pages * sizeof(wal_tx_page_t);
  uint32_t required_pages =
      (uint32_t)size_to_pages(tx_header_size) + tx->pages->modified_pages;

  // <2>
  void *tmp_buf;
  ensure(palmem_allocate_pages(&tmp_buf, required_pages));
  memset(tmp_buf, 0, required_pages * PAGE_SIZE);
  size_t cancel_defer = 0;
  try_defer(palmem_free_page, &tmp_buf, cancel_defer);
  wal_tx_t *wt = tmp_buf;
  wt->number_of_modified_pages = tx->pages->modified_pages;
  wt->tx_id = tx->global_state.header.last_tx_id;

  // <3>
  void *end = wal_setup_transaction_data(tx, wt, tmp_buf + tx_header_size);
  if (!tx->db->options.encrypted) {
    end = wal_compress_transaction(wt, tmp_buf + sizeof(wal_tx_t), end,
                                   tmp_buf + required_pages * PAGE_SIZE);
  }

  wt->tx_size = (uint64_t)(end - tmp_buf);
  wt->page_aligned_tx_size = wal_page_align_tx_size(wt);
  // <4>
  memset(end, 0, wt->page_aligned_tx_size - wt->tx_size);

  ensure(!crypto_generichash(
             wt->tx_hash_sodium, crypto_generichash_BYTES,
             (const uint8_t *)wt + crypto_generichash_BYTES,
             wt->page_aligned_tx_size - crypto_generichash_BYTES, 0, 0),
         msg("Unable to compute hash for transaction"));
  *txn_buffer = tmp_buf;
  cancel_defer = 1;
  return success();
}

// tag::wal_append[]
result_t wal_append(txn_state_t *tx) {
  wal_tx_t *txn_buffer;
  ensure(wal_prepare_txn_buffer(tx, &txn_buffer));
  defer(free, txn_buffer);

  wal_state_t *wal = &tx->db->wal_state;

  struct wal_file_state *cur_file = &wal->files[wal->current_append_file_index];
  if (cur_file->last_write_pos + txn_buffer->page_aligned_tx_size >
      cur_file->map.size) {
    // we need to increase the WAL size
    uint64_t next_size = MAX(next_power_of_two(cur_file->map.size / 10),
                             txn_buffer->page_aligned_tx_size * 2);
    ensure(palfs_set_file_minsize(cur_file->handle, next_size));
    cur_file->map.size = next_size;
  }
  ensure(palfs_write_file(cur_file->handle, cur_file->last_write_pos,
                          (char *)txn_buffer,
                          txn_buffer->page_aligned_tx_size));
  cur_file->last_write_pos += txn_buffer->page_aligned_tx_size;
  cur_file->last_tx_id = tx->global_state.header.last_tx_id;
  return success();
}
// end::wal_append[]

// tag::wal_checkpoint[]
bool wal_will_checkpoint(db_state_t *db, uint64_t tx_id) {
  if (!db)
    return false;

  size_t cur_file_index = db->wal_state.current_append_file_index;
  size_t other_file_index = (cur_file_index + 1) & 1;
  bool cur_full = db->wal_state.files[cur_file_index].last_write_pos >
                  db->options.wal_size / 2;
  bool other_ready = tx_id > db->wal_state.files[other_file_index].last_tx_id;

  return cur_full && other_ready;
}

static result_t wal_reset_file(db_state_t *db, struct wal_file_state *file) {
  void *zero;
  ensure(palmem_allocate_pages(&zero, 1),
         msg("Unable to allocate page for WAL reset"));
  defer(palmem_free_page, &zero);
  memset(zero, 0, PAGE_SIZE);
  wal_tx_t *wt = zero;
  wt->tx_id = file->last_tx_id;
  wt->tx_size = sizeof(wal_tx_t);
  wt->page_aligned_tx_size = PAGE_SIZE;
  ensure(!crypto_generichash(wt->tx_hash_sodium, crypto_generichash_BYTES,
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

  size_t other_index = (db->wal_state.current_append_file_index + 1) & 1;
  struct wal_file_state *other = &db->wal_state.files[other_index];

  struct wal_file_state *cur =
      &db->wal_state.files[db->wal_state.current_append_file_index];

  ensure(wal_reset_file(db, other));

  if (tx_id >= cur->last_tx_id) { // can reset the current WAL as well
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
