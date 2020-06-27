#include <sodium.h>
#include <stdalign.h>
#include <string.h>

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
typedef struct __attribute__((packed)) wal_tx {
  uint8_t tx_hash_sodium[crypto_generichash_BYTES];
  uint64_t tx_id;
  uint64_t tx_size;
  uint32_t number_of_modified_pages;
  struct wal_tx_page pages[];
} wal_tx_t;
// end::wal_tx_t[]

typedef struct wal_page_diff {
  uint32_t offset;
  int32_t length; // negative means zero filled
} wal_page_diff_t;

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

static void *wal_diff_page(void *restrict origin, void *restrict modified,
                           size_t size, void *output) {

  size_t words = size / sizeof(uint64_t);
  uint64_t *origin64 = origin;
  uint64_t *modified64 = modified;

  void *current = output;
  void *end = current + size;
  for (size_t i = 0; i < words; i++) {
    if (origin64[i] == modified64[i]) {
      continue;
    }
    bool zeroes = true;
    size_t diff_start = i;
    for (; i < words &&
           // avoid possible overflow with wal_page_diff_t.length
           (i - diff_start) < (1024 * 1024);
         i++) { // find the range of modified words
      zeroes &= modified64[i] == 0;
      if (origin64[i] == modified64[i]) {
        break;
      }
    }

    int32_t len = (int32_t)((i - diff_start) * sizeof(uint64_t));
    wal_page_diff_t diff = {.offset = (uint32_t)(diff_start * sizeof(uint64_t)),
                            .length = len};
    if (zeroes) {
      diff.length = -diff.length; // indicates zero fill
    }
    void *required_write =
        current + sizeof(wal_page_diff_t) + (diff.length > 0 ? diff.length : 0);
    if (required_write > end) {
      memcpy(output, modified, size);
      return end;
    }
    memcpy(current, &diff, sizeof(wal_page_diff_t));
    current += sizeof(wal_page_diff_t);
    if (diff.length > 0) {
      memcpy(current, modified, (size_t)diff.length);
      current += diff.length;
    }
  }

  return current;
}

// tag::wal_validate_transaction_hash[]
static result_t wal_validate_transaction_hash(wal_tx_t *tx, bool *passed) {
  uint8_t hash[crypto_generichash_BYTES];
  size_t remaining =
      tx->tx_size % PAGE_SIZE ? PAGE_SIZE - (tx->tx_size % PAGE_SIZE) : 0;
  ensure(!crypto_generichash((uint8_t *)hash, crypto_generichash_BYTES,
                             (const uint8_t *)tx + crypto_generichash_BYTES,
                             tx->tx_size - crypto_generichash_BYTES + remaining,
                             0, 0),
         msg("Unable to compute hash for transaction"));

  *passed =
      sodium_compare(hash, tx->tx_hash_sodium, crypto_generichash_BYTES) == 0;
  return success();
}
// end::wal_validate_transaction_hash[]

static result_t wal_recover(db_t *db, wal_state_t *wal) {
  void *start = wal->map.address;
  void *end = (char *)start + wal->map.size;
  db->state->last_tx_id = 0;

  txn_t recovery_tx;
  ensure(txn_create(db, TX_WRITE, &recovery_tx));
  // tag::wal_recover_loop[]
  while (start < end) {
    wal_tx_t *tx = start;
    if ((char *)start + tx->tx_size > (char *)end)
      break;
    if (db->state->last_tx_id > tx->tx_id)
      break; // end of written txs
    if (tx->tx_size <= crypto_generichash_BYTES)
      break;
    bool passed;
    ensure(wal_validate_transaction_hash(tx, &passed));
    if (!passed)
      break;
    // end::wal_recover_loop[]

    db->state->last_tx_id = tx->tx_id;
    void *input = start + sizeof(wal_tx_t) +
                  sizeof(wal_tx_page_t) * tx->number_of_modified_pages;

    for (size_t i = 0; i < tx->number_of_modified_pages; i++) {
      page_t modified_page = {.page_num = tx->pages[i].page_num,
                              .overflow_size =
                                  tx->pages[i].number_of_pages * PAGE_SIZE};
      ensure(txn_modify_page_raw(&recovery_tx, &modified_page));

      size_t end_offset = i + 1 < tx->number_of_modified_pages
                              ? tx->pages[i + 1].offset
                              : tx->tx_size;
      input = wal_apply_diff(input, start + end_offset, &modified_page);
    }
    start = (char *)start + tx->tx_size;
  }
  wal->last_write_pos = (uint64_t)((char *)start - (char *)wal->map.address);
  // tag::wal_recover_validate_after[]
  // we need to check if there are valid transactions _after_ where we stopped
  for (char *cur = start; cur < (char *)end; cur += PAGE_SIZE) {
    wal_tx_t *maybe_tx = (void *)cur;
    if (maybe_tx->tx_size % PAGE_SIZE ||
        maybe_tx->tx_size <= crypto_generichash_BYTES ||
        (char *)cur + maybe_tx->tx_size > (char *)end)
      continue; // validate the sizes
    bool passed;
    ensure(wal_validate_transaction_hash(maybe_tx, &passed));
    if (!passed)
      continue;
    if (db->state->last_tx_id < maybe_tx->tx_id) { // probably old tx
      cur += maybe_tx->tx_size - PAGE_SIZE;        // skip current tx buffer
      continue;
    }

    ssize_t corrupted_pos = (char *)wal->map.address - (char *)start;
    wal_tx_t *corrupted_tx = start;

    failed(ENODATA,
           msg("Found valid transaction after rejecting (smaller) invalid "
               "transaction. WAL is probably corrupted"),
           with(corrupted_pos, "%zd"), with(maybe_tx->tx_id, "%lu"),
           with(corrupted_tx->tx_id, "%lu"),
           with(db->state->last_tx_id, "%lu"));
  }
  // end::wal_recover_validate_after[]
  ensure(txn_write_state_to_disk(recovery_tx.state));
  ensure(txn_close(&recovery_tx)); // discarding the tx

  return success();
}

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

  wal_state_t *wal = malloc(handle_len);
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

  cancel_defer = 1;
  db->state->wal_state = wal;

  return success();
}
// end::wal_open_and_recover[]

result_t wal_close(db_state_t *db) {
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

static void *wal_setup_transaction_for_file(txn_state_t *tx, wal_tx_t *wt,
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
    void *end = wal_diff_page(entry->previous, entry->address, size, output);

    wt->pages[index].offset = (uint64_t)(output - (void *)wt);
    wt->pages[index].flags = (size == (size_t)(end - output))
                                 ? wal_tx_page_flags_none
                                 : wal_tx_page_flags_diff;
    output = end;
    index++;
  }
  return output;
}

// tag::wal_hash_transaction_sodium[]
static result_t wal_hash_transaction(wal_tx_t *wt,
                                     struct palfs_io_buffer *io_buffers,
                                     size_t required_pages, size_t index) {
  crypto_generichash_state state;
  ensure(!crypto_generichash_init(&state, 0, 0, crypto_generichash_BYTES),
         msg("Failed to init hash"));
  ensure(!crypto_generichash_update(
             &state, (const uint8_t *)wt + crypto_generichash_BYTES,
             (required_pages * PAGE_SIZE) - crypto_generichash_BYTES),
         msg("Failed to hash tx data"));

  for (size_t i = 1; i < index + 1; i++) {
    ensure(!crypto_generichash_update(&state, io_buffers[i].address,
                                      io_buffers[i].size),
           msg("Failed to hash page data"), with(i, "%zu"));
  }

  ensure(!crypto_generichash_final(&state, (uint8_t *)wt->tx_hash_sodium,
                                   crypto_generichash_BYTES),
         msg("Unable to complete hash of transaction"));

  return success();
}
// end::wal_hash_transaction_sodium[]

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
  wt->tx_id = tx->tx_id;

  void *end = wal_setup_transaction_for_file(tx, wt, tmp_buf + tx_header_size);

  wt->tx_size = (uint64_t)(end - tmp_buf);
  size_t remaining =
      wt->tx_size % PAGE_SIZE ? PAGE_SIZE - (wt->tx_size % PAGE_SIZE) : 0;
  memset(end, 0, remaining);

  size_t page_aligned_tx_size = wt->tx_size + remaining;

  ensure(!crypto_generichash(wt->tx_hash_sodium, crypto_generichash_BYTES,
                             (const uint8_t *)wt + crypto_generichash_BYTES,
                             page_aligned_tx_size - crypto_generichash_BYTES, 0,
                             0),
         msg("Unable to compute hash for transaction"));

  ensure(palfs_write_file(wal->handle, wal->last_write_pos, tmp_buf,
                          page_aligned_tx_size));
  wal->last_write_pos += page_aligned_tx_size;
  return success();
}
// end::wal_append[]

bool wal_will_checkpoint(db_state_t *db, uint64_t tx_id) {
  return db->last_tx_id == tx_id;
}

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
  ensure(wal_hash_transaction(wt,
                              /*io_buffers*/ 0,
                              /*required_pages*/ 1,
                              /*index*/ 0));
  // reset the start of the log, preventing recovering from proceeding
  ensure(palfs_write_file(db->wal_state->handle, 0, zero, PAGE_SIZE),
         msg("Unable to reset WAL first page"));
  db->wal_state->last_write_pos = PAGE_SIZE;
  return success();
}
// end::wal_checkpoint[]
