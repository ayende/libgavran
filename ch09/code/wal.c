#include <sodium.h>
#include <string.h>

#include "impl.h"
#include "platform.mem.h"

// tag::structs[]
typedef struct wal_state {
  file_handle_t *handle;
  struct mmap_args map;
  uint64_t last_write_pos;
} wal_state_t;

typedef struct __attribute__((packed)) wal_tx_page {
  uint64_t page_num;
  uint64_t offset;
  uint32_t number_of_pages;
} wal_tx_page_t;

typedef struct __attribute__((packed)) wal_tx {
  uint8_t tx_hash[crypto_generichash_BYTES];
  uint64_t tx_id;
  uint64_t tx_size;
  uint32_t number_of_modified_pages;
  struct wal_tx_page pages[];
} wal_tx_t;
// end::structs[]

// tag::wal_recover[]
static result_t wal_recover(db_state_t *db, wal_state_t *wal) {
  void *start = wal->map.address;
  void *end = (char *)start + wal->map.size;
  db->last_tx_id = 1;
  while (start < end) {
    wal_tx_t *tx = start;
    if ((char *)start + tx->tx_size > (char *)end)
      break;
    if (db->last_tx_id > tx->tx_id)
      break; // end of written txs
    if (tx->tx_size <= crypto_generichash_BYTES)
      break;

    uint8_t hash[crypto_generichash_BYTES];
    ensure(
        !crypto_generichash((uint8_t *)hash, crypto_generichash_BYTES,
                            (const uint8_t *)start + crypto_generichash_BYTES,
                            tx->tx_size - crypto_generichash_BYTES, 0, 0),
        msg("Unable to compute hash for transaction"));

    if (sodium_compare(hash, tx->tx_hash, crypto_generichash_BYTES) != 0)
      break;

    db->last_tx_id = tx->tx_id;
    for (size_t i = 0; i < tx->number_of_modified_pages; i++) {
      void *src = (char *)start + tx->pages[i].offset;
      size_t len = tx->pages[i].number_of_pages * PAGE_SIZE;
      size_t file_offset = (tx->pages[i].page_num * PAGE_SIZE);
      ensure(palfs_write_file(db->handle, file_offset, src, len));
    }
    start = (char *)start + tx->tx_size;
  }
  wal->last_write_pos = (uint64_t)((char *)start - (char *)wal->map.address);
  return success();
}
// end::wal_recover[]

// tag::wal_open_and_recover[]
result_t wal_open_and_recover(db_state_t *db) {
  const char *db_file_name = palfs_get_filename(db->handle);
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
  ensure(palfs_set_file_minsize(wal->handle, db->options.wal_size));
  ensure(palfs_get_filesize(wal->handle, &wal->map.size));
  ensure(palfs_mmap(wal->handle, 0, &wal->map));
  defer(palfs_unmap, &wal->map);

  ensure(wal_recover(db, wal));

  cancel_defer = 1;
  db->wal_state = wal;

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

// tag::wal_append[]
result_t wal_append(txn_state_t *tx) {
  wal_state_t *wal = tx->db->wal_state;
  // <1>
  uint32_t required_pages = size_to_pages(
      sizeof(wal_tx_t) + tx->modified_pages * sizeof(wal_tx_page_t));
  // <2>
  void *tmp_buf;
  ensure(palmem_allocate_pages(&tmp_buf, required_pages));
  memset(tmp_buf, 0, required_pages * PAGE_SIZE);
  defer(palmem_free_page, &tmp_buf);
  wal_tx_t *wt = tmp_buf;

  struct palfs_io_buffer *io_buffers =
      calloc(tx->modified_pages + 1, sizeof(struct palfs_io_buffer));
  ensure(io_buffers, msg("Unable to allocate IO buffers for append"));
  defer(free, io_buffers);

  io_buffers[0].address = wt;
  wt->tx_size = io_buffers[0].size = required_pages * PAGE_SIZE;
  wt->number_of_modified_pages = tx->modified_pages;
  wt->tx_id = tx->tx_id;
  size_t index = 0;
  size_t number_of_buckets = get_number_of_buckets(tx);
  for (size_t i = 0; i < number_of_buckets; i++) {
    page_t *entry = &tx->entries[i];
    if (!entry->address)
      continue;
    wt->pages[index].offset = wt->tx_size;
    wt->pages[index].page_num = entry->page_num;
    wt->pages[index].number_of_pages =
        size_to_pages(entry->overflow_size / PAGE_SIZE);

    io_buffers[index + 1].address = entry->address;
    io_buffers[index + 1].size = wt->pages[index].number_of_pages * PAGE_SIZE;
    index++;

    wt->tx_size += io_buffers[index].size;
  }

  crypto_generichash_state state;
  ensure(!crypto_generichash_init(&state, 0, 0, crypto_generichash_BYTES),
         msg("Failed to init hash"));
  ensure(!crypto_generichash_update(
             &state, (const uint8_t *)wt + crypto_generichash_BYTES,
             sizeof(wal_tx_t) - crypto_generichash_BYTES),
         msg("Failed to hash tx data"));

  for (size_t i = 1; i < index + 1; i++) {
    ensure(!crypto_generichash_update(&state, io_buffers[i].address,
                                      io_buffers[i].size),
           msg("Failed to hash page data"), with(i, "%zu"));
  }

  ensure(!crypto_generichash_final(&state, (uint8_t *)wt->tx_hash,
                                   crypto_generichash_BYTES),
         msg("Unable to complete hash of transaction"));

  ensure(palfs_vectored_write_file(wal->handle, wal->last_write_pos, io_buffers,
                                   index + 1));
  wal->last_write_pos += wt->tx_size;
  return success();
}
// end::wal_append[]

// tag::wal_checkpoint[]
bool wal_will_checkpoint(db_state_t *db, uint64_t tx_id) {
  return db->last_tx_id == tx_id;
}
result_t wal_checkpoint(db_state_t *db, uint64_t tx_id) {
  if (!wal_will_checkpoint(db, tx_id)) {
    return success();
  }
  void *zero;
  ensure(palmem_allocate_pages(&zero, 1),
         msg("Unable to allocate page for WAL reset"));
  defer(palmem_free_page, &zero);
  memset(zero, 0, PAGE_SIZE);
  // reset the start of the log, preventing recovering from proceeding
  ensure(palfs_write_file(db->wal_state->handle, 0, zero, PAGE_SIZE),
         msg("Unable to reset WAL first page"));
  db->wal_state->last_write_pos = 0;
  return success();
}
// end::wal_checkpoint[]
