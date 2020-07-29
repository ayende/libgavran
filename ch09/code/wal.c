#include <gavran/db.h>
#include <gavran/internal.h>
#include <sodium.h>
#include <string.h>
#include <zstd.h>

// tag::wal_txn_t[]
enum wal_txn_page_flags {
  wal_txn_page_flags_none = 0,
  wal_txn_page_flags_diff = 1,
};

typedef struct wal_txn_page {
  uint64_t page_num;
  uint64_t offset;
  uint64_t number_of_pages;
  uint32_t flags;
  uint8_t padding[4];
} wal_txn_page_t;

enum wal_txn_flags {
  wal_txn_flags_none = 0,
  wal_txn_flags_compressed = 1,
};

typedef struct wal_txn {
  uint8_t hash_blake2b[32];
  uint64_t tx_id;
  uint64_t page_aligned_tx_size;
  uint64_t tx_size;
  uint64_t number_of_modified_pages;
  enum wal_txn_flags flags;
  uint8_t padding[4];
  struct wal_txn_page pages[];
} wal_txn_t;
// end::wal_txn_t[]

// tag::wal_page_diff[]
typedef struct wal_page_diff {
  uint32_t offset;
  int32_t length;  // negative means zero filled
} wal_page_diff_t;
// end::wal_page_diff[]

// tag::wal_apply_diff[]
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
// end::wal_apply_diff[]

// tag::wal_diff_page[]
static void *wal_diff_page(uint64_t *restrict origin,
                           uint64_t *restrict modified, size_t size,
                           void *output) {
  if (!origin) {  // no previous definition
    memcpy(output, modified, size * sizeof(uint64_t));
    return output + (size * sizeof(uint64_t));
  }
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
    void *required_write = current + sizeof(wal_page_diff_t);
    wal_page_diff_t diff = {
        .offset = (uint32_t)(diff_start * sizeof(uint64_t)),
        .length = (int32_t)((i - diff_start) * sizeof(uint64_t))};
    if (zeroes) {
      diff.length = -diff.length;  // indicates zero fill
    } else {
      required_write += diff.length;
    }
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

// tag::wal_setup_transaction_data[]
static void *wal_setup_transaction_data(txn_state_t *tx,
                                        wal_txn_t *wt, void *output) {
  size_t iter_state = 0;
  page_t *entry;
  size_t index = 0;

  while (hash_get_next(tx->modified_pages, &iter_state, &entry)) {
    wt->pages[index].number_of_pages = entry->number_of_pages;
    wt->pages[index].page_num = entry->page_num;
    size_t size = wt->pages[index].number_of_pages * PAGE_SIZE;
    // <1>
    void *end = wal_diff_page(entry->previous, entry->address,
                              size / sizeof(uint64_t), output);
    wt->pages[index].flags = (size == (size_t)(end - output))
                                 ? wal_txn_page_flags_none
                                 : wal_txn_page_flags_diff;
    wt->pages[index].offset = (uint64_t)(output - (void *)wt);
    output = end;
    index++;
  }
  return output;
}
// end::wal_setup_transaction_data[]

// tag::wal_compress_transaction[]
static void *wal_compress_transaction(wal_txn_t *wt, void *start,
                                      void *end) {
  size_t input_size = (size_t)(end - start);
  size_t required_size = ZSTD_compressBound(input_size);
  void *buffer;
  if (flopped(mem_alloc(&buffer, required_size))) {
    // no memory, we'll skip compression
    errors_clear();  // recoverable, so can clear it
    return end;
  }
  defer(free, buffer);

  size_t res =
      ZSTD_compress(buffer, required_size, start, input_size, 0);
  if (ZSTD_isError(res) || res >= input_size) {
    // * we got an error, let's just return uncompressed
    // * compressed bigger than input? skip it
    return end;
  }
  wt->flags = wal_txn_flags_compressed;
  memcpy(start, buffer, res);
  return start + res;
}
// end::wal_compress_transaction[]

// tag::wal_prepare_txn_buffer[]
static result_t wal_prepare_txn_buffer(txn_state_t *tx,
                                       wal_txn_t **txn_buffer) {
  uint64_t pages = tx->modified_pages->count;
  // <1>
  size_t tx_header_size =
      sizeof(wal_txn_t) + pages * sizeof(wal_txn_page_t);
  uint64_t total_size =
      (TO_PAGES(tx_header_size) + pages) * PAGE_SIZE;
  size_t cancel_defer = 0;
  wal_txn_t *wt;
  ensure(mem_alloc_page_aligned((void *)&wt, total_size));
  try_defer(free, wt, cancel_defer);
  memset(wt, 0, total_size);
  wt->number_of_modified_pages = pages;
  wt->tx_id = tx->tx_id;
  void *end = wal_setup_transaction_data(
      tx, wt, ((char *)wt) + tx_header_size);
  // <2>
  end = wal_compress_transaction(wt, (char *)wt + sizeof(wal_txn_t),
                                 end);
  // <3>
  wt->tx_size = (uint64_t)((char *)end - (char *)wt);
  wt->page_aligned_tx_size = TO_PAGES(wt->tx_size) * PAGE_SIZE;
  // <4>
  memset(((void *)wt) + wt->tx_size, 0,
         wt->page_aligned_tx_size - wt->tx_size);

  *txn_buffer = wt;
  cancel_defer = 1;
  return success();
}
// end::wal_prepare_txn_buffer[]

// tag::wal_append[]
result_t wal_append(txn_state_t *tx) {
  wal_txn_t *txn_buffer;
  ensure(wal_prepare_txn_buffer(tx, &txn_buffer));
  defer(free, txn_buffer);

  // <1>
  const size_t size = crypto_generichash_BYTES;
  ensure(!crypto_generichash(txn_buffer->hash_blake2b, size,
                             (uint8_t *)txn_buffer + size,
                             txn_buffer->page_aligned_tx_size - size,
                             0, 0),
         msg("Unable to compute hash for transaction"),
         with(txn_buffer->tx_id, "%lu"));

  wal_state_t *wal = &tx->db->wal_state;

  wal_file_state_t *cur_file = &wal->files[0];
  ensure(pal_write_file(cur_file->handle, cur_file->last_write_pos,
                        (char *)txn_buffer,
                        txn_buffer->page_aligned_tx_size));
  cur_file->last_write_pos += txn_buffer->page_aligned_tx_size;
  cur_file->last_tx_id = tx->tx_id;
  return success();
}
// end::wal_append[]

// tag::wal_recovery_operation[]
typedef struct wal_recovery_operation {
  db_t *db;
  wal_state_t *wal;
  wal_file_state_t *files[2];
  size_t current_recovery_file_index;
  void *start;
  void *end;
  uint64_t last_recovered_tx_id;
  reusable_buffer_t tmp_buffer;
} wal_recovery_operation_t;
// end::wal_recovery_operation[]

// tag::wal_init_recover_state[]
static void wal_init_recover_state(db_t *db, wal_state_t *wal,
                                   wal_recovery_operation_t *state) {
  memset(state, 0, sizeof(wal_recovery_operation_t));
  state->db = db;
  state->wal = wal;
  state->last_recovered_tx_id = 0;
  state->files[0] = &wal->files[0];
  state->files[1] = &wal->files[1];

  state->start = state->files[0]->span.address;
  state->end = state->start + state->files[0]->span.size;
}
// end::wal_init_recover_state[]

// tag::wal_validate_recovered_pages[]
static result_t wal_validate_recovered_pages(
    db_t *db, pages_hash_table_t *modified_pages) {
  size_t iter_state = 0;
  page_t *page_to_validate;
  while (
      hash_get_next(modified_pages, &iter_state, &page_to_validate)) {
    txn_t rtx;
    ensure(txn_create(db, TX_READ, &rtx));
    defer(txn_close, rtx);
    page_t p = {.page_num = page_to_validate->page_num};
    ensure(txn_get_page(&rtx, &p));
  }
  return success();
}
// end::wal_validate_recovered_pages[]

// tag::wal_range[]
static result_t wal_get_next_range(wal_recovery_operation_t *state,
                                   void **current, void **end) {
  *end = state->end;
  if (state->start >= state->end) {
    *current = 0;
    return success();
  }
  *current = state->start;
  return success();
}

static void wal_increment_next_range_start(
    wal_recovery_operation_t *state, size_t amount) {
  state->start += amount;
}
// end::wal_range[]

static result_t wal_validate_transaction(reusable_buffer_t *file,
                                         void *start, void *end,
                                         wal_txn_t **txn_p);

// tag::wal_validate_after_end_of_transactions[]
static result_t wal_ensure_last_tx_id_is_set(
    wal_recovery_operation_t *s) {
  if (s->last_recovered_tx_id) return success();
  txn_t rtx;  // nothing from WAL, load the db's last_tx_id
  ensure(txn_create(s->db, TX_READ, &rtx));
  defer(txn_close, rtx);
  page_t page = {.page_num = 0};  // maybe new db, have to use raw API
  ensure(txn_raw_get_page(&rtx, &page));
  page_metadata_t *metadata = page.address;
  s->last_recovered_tx_id = metadata->file_header.last_tx_id;
  return success();
}
static result_t wal_validate_after_end_of_transactions(
    wal_recovery_operation_t *s) {
  ensure(wal_ensure_last_tx_id_is_set(s));
  while (true) {
    void *cur, *end;
    ensure(wal_get_next_range(s, &cur, &end));
    if (!cur) break;
    wal_txn_t *tx;
    if (flopped(wal_validate_transaction(&s->tmp_buffer, cur, end,
                                         &tx)) ||
        !tx) {
      errors_clear();  // errors are expected here
      wal_increment_next_range_start(s, PAGE_SIZE);
      continue;
    }
    if (s->last_recovered_tx_id > tx->tx_id) {
      break;  // valid old tx, we had a WAL reset and can stop
    }
    ssize_t corrupted_pos = cur - s->files[0]->span.address;
    wal_txn_t *corrupted_tx = cur;
    failed(ENODATA, msg("Valid TX after invalid TX"),
           with(corrupted_pos, "%zd"), with(tx->tx_id, "%lu"),
           with(corrupted_tx->tx_id, "%lu"),
           with(s->db->state->last_tx_id, "%lu"));
  }
  return success();
}
// end::wal_validate_after_end_of_transactions[]

// tag::wal_decompress_transaction[]
static result_t wal_decompress_transaction(reusable_buffer_t *buffer,
                                           wal_txn_t *in,
                                           wal_txn_t **txp) {
  // <1>
  if (in->flags == wal_txn_flags_none) {
    *txp = in;
    return success();
  }
  // <2>
  size_t required_size =
      ZSTD_getDecompressedSize((void *)in + sizeof(wal_txn_t),
                               in->tx_size - sizeof(wal_txn_t)) +
      sizeof(wal_txn_t);
  if (required_size > buffer->size) {
    ensure(mem_realloc(&buffer->address, required_size));
    buffer->size = required_size;
  }
  // <3>
  size_t res = ZSTD_decompress(buffer->address + sizeof(wal_txn_t),
                               required_size - sizeof(wal_txn_t),
                               (void *)in + sizeof(wal_txn_t),
                               in->tx_size - sizeof(wal_txn_t));
  if (ZSTD_isError(res)) {
    const char *zstd_error = ZSTD_getErrorName(res);
    failed(ENODATA, msg("Failed to decompress transaction"),
           with(in->tx_id, "%lu"), with(zstd_error, "%s"));
  }
  // <4>
  memcpy(buffer->address, in, sizeof(wal_txn_t));
  *txp = buffer->address;
  (*txp)->tx_size = buffer->used = res + sizeof(wal_txn_t);
  return success();
}
// end::wal_decompress_transaction[]

// tag::wal_validate_transaction[]
static result_t wal_validate_transaction(reusable_buffer_t *buffer,
                                         void *start, void *end,
                                         wal_txn_t **txn_p) {
  *txn_p = 0;
  wal_txn_t *tx = start;
  if (!tx->tx_id || tx->page_aligned_tx_size + start > end) {
    *txn_p = 0;
    return success();
  }
  uint8_t hash[crypto_generichash_BYTES];
  const size_t size = crypto_generichash_BYTES;
  ensure(!crypto_generichash(hash, size, (uint8_t *)tx + size,
                             tx->page_aligned_tx_size - size, 0, 0),
         msg("Unable to compute hash for transaction on recover"),
         with(tx->tx_id, "%lu"));

  if (memcmp(hash, tx->hash_blake2b, size) != 0) {
    *txn_p = 0;  // not a match on the hash, failed
    return success();
  }
  // we got a valid hash, can go forward with this
  ensure(wal_decompress_transaction(buffer, tx, txn_p));
  return success();
}
// end::wal_validate_transaction[]

// tag::wal_next_valid_transaction[]
static result_t wal_next_valid_transaction(
    struct wal_recovery_operation *state, wal_txn_t **txp) {
  if (state->start >= state->end ||
      !wal_validate_transaction(&state->tmp_buffer, state->start,
                                state->end, txp) ||
      !*txp || state->last_recovered_tx_id >= (*txp)->tx_id) {
    *txp = 0;
    // <1>
    ensure(wal_validate_after_end_of_transactions(state));
  } else {
    state->last_recovered_tx_id = (*txp)->tx_id;
    state->start = state->start + (*txp)->page_aligned_tx_size;
  }
  return success();
}
// end::wal_next_valid_transaction[]

// tag::wal_recover_page[]
static result_t wal_recover_page(db_t *db, pages_hash_table_t **pages,
                                 wal_txn_page_t *page, void *end,
                                 const void *src, void **input) {
  size_t size = page->number_of_pages * PAGE_SIZE;
  page_t final = {.page_num = page->page_num,
                  .number_of_pages = page->number_of_pages};
  ensure(mem_alloc_page_aligned((void *)&final.address, size));
  size_t done = 0;
  try_defer(free, final.address, done);
  if (page->flags == wal_txn_page_flags_diff) {
    txn_t tx;
    ensure(txn_create(db, TX_READ, &tx));
    page_t before = {.page_num = page->page_num,
                     .number_of_pages = page->number_of_pages};
    ensure(pages_get(&tx, &before));
    memcpy(final.address, before.address,
           page->number_of_pages * PAGE_SIZE);
    *input = wal_apply_diff(*input, end, &final);
  } else {
    memcpy(final.address, src + page->offset, size);
    *input += size;
  }

  ensure(hash_put_new(pages, &final));
  done = 1;

  return success();
}
// end::wal_recover_page[]

// tag::wal_recover_tx[]
static result_t free_hash_table_and_contents(
    pages_hash_table_t **pages) {
  size_t iter_state = 0;
  page_t *p;
  while (hash_get_next(*pages, &iter_state, &p)) {
    free(p->address);
  }
  free(*pages);
  return success();
}
enable_defer(free_hash_table_and_contents);

static result_t wal_recover_tx(db_t *db, wal_txn_t *tx,
                               pages_hash_table_t **recovered_pages) {
  void *input = (void *)tx + sizeof(wal_txn_t) +
                sizeof(wal_txn_page_t) * tx->number_of_modified_pages;
  pages_hash_table_t *pages;
  ensure(hash_new(next_power_of_two(tx->number_of_modified_pages +
                                    tx->number_of_modified_pages / 2),
                  &pages));
  defer(free_hash_table_and_contents, pages);
  for (size_t i = 0; i < tx->number_of_modified_pages; i++) {
    size_t end_offset = i + 1 < tx->number_of_modified_pages
                            ? tx->pages[i + 1].offset
                            : tx->tx_size;
    ensure(wal_recover_page(db, &pages, tx->pages + i,
                            ((void *)tx) + end_offset, tx, &input));
  }
  size_t iter_state = 0;
  page_t *p;
  while (hash_get_next(pages, &iter_state, &p)) {
    page_t existing = {.page_num = p->page_num};
    if (!hash_lookup(*recovered_pages, &existing)) {
      ensure(hash_put_new(recovered_pages, p));
    }
    ensure(pal_write_file(db->state->handle, p->page_num * PAGE_SIZE,
                          p->address,
                          p->number_of_pages * PAGE_SIZE));
  }
  return success();
}
// end::wal_recover_tx[]

// tag::wal_complete_recovery[]
static result_t wal_complete_recovery(
    struct wal_recovery_operation *state) {
  txn_t recovery_tx;
  ensure(txn_create(state->db, TX_READ, &recovery_tx));
  defer(txn_close, recovery_tx);
  page_t header_page = {.page_num = 0};
  ensure(txn_raw_get_page(&recovery_tx, &header_page));
  page_metadata_t *header = header_page.address;

  state->db->state->number_of_pages =
      header->file_header.number_of_pages;
  state->db->state->last_tx_id = header->file_header.last_tx_id;
  if (state->last_recovered_tx_id == 0) {  // empty db / no recovery
    if (header->file_header.last_tx_id != 0) {  // no recovery needed
      state->last_recovered_tx_id = header->file_header.last_tx_id;
    }

    state->db->state->number_of_pages =
        state->db->state->map.size / PAGE_SIZE;
  } else {
    ensure(header->common.page_flags == page_flags_file_header,
           msg("First page was not a metadata page?"));
  }
  ensure(
      header->file_header.last_tx_id == state->last_recovered_tx_id,
      msg("The last recovered tx id does not match the header tx id"),
      with(header->file_header.last_tx_id, "%lu"),
      with(state->last_recovered_tx_id, "%lu"));

  state->db->state->default_read_tx->number_of_pages =
      state->db->state->number_of_pages;
  state->db->state->default_read_tx->map = state->db->state->map;
  return success();
}
// end::wal_complete_recovery[]

// tag::wal_recover[]
static result_t wal_recover(db_t *db, wal_state_t *wal) {
  wal_recovery_operation_t recovery_state;
  wal_init_recover_state(db, wal, &recovery_state);
  pages_hash_table_t *recovered_pages;
  ensure(hash_new(16, &recovered_pages));
  defer(free, recovered_pages);
  defer(free, recovery_state.tmp_buffer.address);

  while (true) {
    wal_txn_t *tx;
    ensure(wal_next_valid_transaction(&recovery_state, &tx));
    if (!tx) break;
    ensure(wal_recover_tx(db, tx, &recovered_pages));
  }
  ensure(wal_complete_recovery(&recovery_state));
  ensure(wal_validate_recovered_pages(db, recovered_pages));
  return success();
}
// end::wal_recover[]

// tag::wal_open_single_file[]
static result_t wal_get_wal_filename(const char *db_file_name,
                                     char wal_code,
                                     char **wal_file_name) {
  size_t db_name_len = strlen(db_file_name);  // \0 + -a.wal
  ensure(mem_alloc((void *)wal_file_name, db_name_len + 1 + 6));
  memcpy(*wal_file_name, db_file_name, db_name_len);
  (*wal_file_name)[db_name_len++] = '-';
  (*wal_file_name)[db_name_len++] = wal_code;
  memcpy((*wal_file_name) + db_name_len, ".wal", 5);  // include \0
  return success();
}
static result_t wal_open_single_file(
    struct wal_file_state *file_state, db_t *db, char wal_code) {
  char *wal_file_name;
  ensure(wal_get_wal_filename(db->state->handle->filename, wal_code,
                              &wal_file_name));
  defer(free, wal_file_name);
  ensure(pal_create_file(wal_file_name, &file_state->handle,
                         pal_file_creation_flags_durable));
  ensure(pal_set_file_size(file_state->handle,
                           db->state->options.wal_size, UINT64_MAX));
  file_state->span.size = file_state->handle->size;
  ensure(pal_mmap(file_state->handle, 0, &file_state->span));
  return success();
}
// end::wal_open_single_file[]

// tag::wal_open_and_recover[]
result_t wal_open_and_recover(db_t *db) {
  memset(&db->state->wal_state, 0, sizeof(wal_state_t));
  wal_state_t *wal = &db->state->wal_state;
  ensure(wal_open_single_file(&wal->files[0], db, 'a'));
  defer(pal_unmap, wal->files[0].span);
  ensure(wal_recover(db, wal));
  return success();
}
// end::wal_open_and_recover[]

result_t wal_close(db_state_t *db) {
  if (!db) return success();
  // need to proceed even if there are failures
  bool failure = false;
  for (size_t i = 0; i < 2; i++) {
    failure = !pal_unmap(&db->wal_state.files[i].span);
    failure |= !pal_close_file(db->wal_state.files[i].handle);
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

// tag::wal_will_checkpoint[]
bool wal_will_checkpoint(db_state_t *db, uint64_t tx_id) {
  if (!db) return false;

  bool full = db->wal_state.files[0].last_write_pos >
              db->options.wal_size / 2;
  bool at_end = tx_id >= db->wal_state.files[0].last_tx_id;

  return full && at_end;
}
// end::wal_will_checkpoint[]

// tag::wal_checkpoint[]
static result_t wal_reset_file(db_state_t *db,
                               wal_file_state_t *file) {
  (void)db;
  void *zero;
  ensure(mem_alloc_page_aligned(&zero, PAGE_SIZE));
  defer(free, zero);
  memset(zero, 0, PAGE_SIZE);
  // reset the start of the log, preventing recovery from proceeding
  ensure(pal_write_file(file->handle, 0, zero, PAGE_SIZE),
         msg("Unable to reset WAL first page"));
  file->last_write_pos = 0;
  return success();
}

result_t wal_checkpoint(db_state_t *db, uint64_t tx_id) {
  (void)tx_id;
  ensure(wal_reset_file(db, &db->wal_state.files[0]));
  return success();
}
// end::wal_checkpoint[]