#include <errno.h>
#include <sodium.h>
#include <stdio.h>
#include <string.h>
#include <sys/param.h>

#include "db.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::txn_hash_page[]
result_t txn_hash_page(page_t *page, uint8_t hash[crypto_generichash_BYTES]) {
  bool is_metadata_page =
      (page->page_num & PAGES_IN_METADATA_MASK) == page->page_num;

  void *start = is_metadata_page ? page->address + crypto_generichash_BYTES
                                 : page->address;
  size_t size = is_metadata_page
                    ? PAGE_SIZE - sizeof(page_metadata_t)
                    : size_to_pages(page->overflow_size) * PAGE_SIZE;

  if (crypto_generichash(hash, crypto_generichash_BYTES, start, size, 0, 0)) {
    failed(ENODATA,
           msg("Unable to compute page hash for page, shouldn't happen"),
           with(page->page_num, "%lu"));
  }
  return success();
}
// end::txn_hash_page[]

// tag::txn_encrypt_page[]
static const char TxnKeyCtx[8] = "TxnPages";

static result_t txn_encrypt_page(txn_t *tx, uint64_t page_num, void *start,
                                 size_t size, page_metadata_t *metadata) {
  // <1>
  uint8_t subkey[crypto_aead_aes256gcm_KEYBYTES];
  if (crypto_kdf_derive_from_key(subkey, crypto_aead_aes256gcm_KEYBYTES,
                                 page_num, TxnKeyCtx,
                                 tx->state->db->options.encryption_key)) {
    failed(EINVAL, msg("Unable to derive key for page decryption"),
           with(page_num, "%ld"));
  }

  if (sodium_is_zero(metadata->aes_gcm.nonce,
                     crypto_aead_aes256gcm_NPUBBYTES)) {
    // <2>
    randombytes_buf(metadata->aes_gcm.nonce, crypto_aead_aes256gcm_NPUBBYTES);
  } else {
    // <3>
    sodium_increment(metadata->aes_gcm.nonce, crypto_aead_aes256gcm_NPUBBYTES);
  }

  // <4>
  int result = crypto_aead_aes256gcm_encrypt_detached(
      start, metadata->aes_gcm.mac, 0, start, size, 0, 0, 0,
      metadata->aes_gcm.nonce, subkey);
  sodium_memzero(subkey, crypto_aead_aes256gcm_KEYBYTES);
  if (result) {
    failed(EINVAL, msg("Unable to encrypt page"), with(page_num, "%ld"));
  }
  return success();
}
// end::txn_encrypt_page[]

// tag::tx_finalize_page[]
static result_t tx_finalize_page(txn_t *tx, page_t *page,
                                 page_metadata_t *metadata) {
  if (tx->state->db->options.encrypted) {
    if ((page->page_num & PAGES_IN_METADATA_MASK) == page->page_num) {
      return txn_encrypt_page(
          tx, page->page_num, page->address + PAGE_METADATA_CRYPTO_HEADER_SIZE,
          PAGE_SIZE - PAGE_METADATA_CRYPTO_HEADER_SIZE, metadata);
    }
    return txn_encrypt_page(tx, page->page_num, page->address,
                            size_to_pages(metadata->overflow_size) * PAGE_SIZE,
                            metadata);
  } else {
    return txn_hash_page(page, metadata->page_hash);
  }
}
// end::tx_finalize_page[]

// tag::txn_finalize_modified_pages[]
static result_t txn_finalize_modified_pages(txn_t *tx) {
  txn_state_t *state = tx->state;
  // <1>
  page_t *modified_pages = calloc(state->pages->modified_pages, sizeof(page_t));
  ensure(modified_pages, msg("Unable to allocate buffer for modified pages"));
  defer(free, modified_pages);

  size_t modified_pages_idx = 0;
  size_t iter_state = 0;
  page_t *current;
  while (hash_get_next(tx->state->pages, &iter_state, &current)) {
    // <2>
    // have to copy it here, might change during txn_modify_metadata
    memcpy(&modified_pages[modified_pages_idx++], current, sizeof(page_t));
  }
  // <3>
  for (size_t i = 0; i < modified_pages_idx; i++) {
    page_metadata_t *metadata;
    ensure(txn_modify_metadata(tx, modified_pages[i].page_num, &metadata));
    if ((modified_pages[i].page_num & PAGES_IN_METADATA_MASK) ==
        modified_pages[i].page_num)
      // we handle metadata page separately, metadata pages *must* be modified
      continue;

    ensure(tx_finalize_page(tx, &modified_pages[i], metadata));
  }
  // <4>
  iter_state = 0;
  while (hash_get_next(tx->state->pages, &iter_state, &current)) {
    if ((current->page_num & PAGES_IN_METADATA_MASK) != current->page_num)
      continue; // not a metadata page
    page_metadata_t *entries = current->address;

    ensure(tx_finalize_page(tx, current, entries));
  }
  return success();
}
// end::txn_finalize_modified_pages[]

// tag::txn_commit[]
result_t txn_commit(txn_t *tx) {
  errors_assert_empty();

  // no modified pages, nothing to do here
  if (!tx->state->pages->modified_pages)
    return success();

  page_metadata_t *first_metadata;
  ensure(txn_modify_metadata(tx, 0, &first_metadata));
  first_metadata->file_header.last_tx_id =
      tx->state->global_state.header.last_tx_id;

  ensure(txn_finalize_modified_pages(tx));

  ensure(wal_append(tx->state));

  tx->state->flags |= TX_COMMITED;
  tx->state->usages = 1;
  db_state_t *db = tx->state->db;

  // <1>
  // we will never rollback this tx, can free this memory
  while (tx->state->on_rollback) {
    struct cleanup_act *cur = tx->state->on_rollback;
    tx->state->on_rollback = cur->next;
    free(cur);
  }

  db->last_write_tx->next_tx = tx->state;
  db->last_write_tx = tx->state;
  // <2>
  // copy the committed state of the system
  db->global_state = tx->state->global_state;
  return success();
}
// end::txn_commit[]

// tag::txn_write_state_to_disk[]
result_t txn_write_state_to_disk(txn_state_t *state, bool can_checkpoint) {
  size_t iter_state = 0;
  page_t *current;
  while (hash_get_next(state->pages, &iter_state, &current)) {
    ensure(pages_write(state->db, current));
  }

  if (can_checkpoint &&
      wal_will_checkpoint(state->db, state->global_state.header.last_tx_id)) {
    ensure(palfs_fsync_file(state->db->handle));
    ensure(wal_checkpoint(state->db, state->global_state.header.last_tx_id));
  }
  return success();
}
// end::txn_write_state_to_disk[]

// tag::free_tx[]
void txn_free_single_tx(txn_state_t *state) {
  size_t iter_state = 0;
  page_t *current;
  while (hash_get_next(state->pages, &iter_state, &current)) {
    free(current->address);
    current->address = 0;
  }
  // <4>
  while (state->on_forget) {
    struct cleanup_act *cur = state->on_forget;
    cur->func(cur->state);
    state->on_forget = cur->next;
    free(cur);
  }
  free(state->pages);
  free(state);
}
// end::free_tx[]

// tag::txn_merge_unique_pages[]
static result_t txn_merge_unique_pages(txn_state_t *state) {
  // build a table of all the distinct pages that we want to write
  txn_state_t *prev = state->prev_tx;
  while (prev) {

    size_t iter_state = 0;
    page_t *entry;
    while (hash_get_next(prev->pages, &iter_state, &entry)) {
      page_t check = {.page_num = entry->page_num};
      if (hash_lookup(state->pages, &check))
        continue;

      ensure(hash_put_new(&state->pages, entry));
      entry->address = 0; // we moved ownership, detach from original tx
    }
    prev = prev->prev_tx;
  }

  return success();
}
// end::txn_merge_unique_pages[]

// tag::txn_try_reset_tx_chain[]
static void txn_try_reset_tx_chain(db_state_t *db, txn_state_t *state) {
  if (state != db->last_write_tx)
    return;

  // this is the last transaction, need to reset the tx chain to default
  db->last_write_tx = db->default_read_tx;
  db->default_read_tx->next_tx = 0;
  db->default_read_tx->global_state = state->global_state;
  // can be cleaned up immediately, nothing afterward
  state->can_free_after_tx_id = db->global_state.header.last_tx_id;
}
// end::txn_try_reset_tx_chain[]

// tag::txn_free_registered_transactions[]
static void txn_free_registered_transactions(db_state_t *state) {
  while (state->transactions_to_free) {
    txn_state_t *cur = state->transactions_to_free;

    if (cur->usages || cur->can_free_after_tx_id > state->oldest_active_tx)
      break;

    if (cur->next_tx)
      cur->next_tx->prev_tx = 0;

    state->transactions_to_free = cur->next_tx;
    state->default_read_tx->next_tx = cur->next_tx;

    txn_free_single_tx(cur);
  }
}
// end::txn_free_registered_transactions[]

// tag::txn_gc_tx[]
static result_t txn_gc_tx(txn_state_t *state) {
  // <1>
  db_state_t *db = state->db;
  state->can_free_after_tx_id = db->global_state.header.last_tx_id + 1;
  // <2>
  txn_state_t *latest_unused = state->db->default_read_tx;
  if (latest_unused->usages)
    // transactions looking at the file directly, cannot proceed
    return success();
  // <3>
  while (latest_unused->next_tx && latest_unused->next_tx->usages == 0) {
    latest_unused = latest_unused->next_tx;
  }
  if (latest_unused == db->default_read_tx) {
    return success(); // no work to be done
  }

  db->oldest_active_tx = latest_unused->global_state.header.last_tx_id + 1;
  // <4>
  ensure(txn_merge_unique_pages(latest_unused));
  // <5>
  ensure(txn_write_state_to_disk(latest_unused, /* can_checkpoint*/ true));

  // <6>
  txn_try_reset_tx_chain(db, latest_unused);
  // <7>
  txn_free_registered_transactions(db);

  return success();
}
// end::txn_gc_tx[]

// tag::txn_close[]
result_t txn_close(txn_t *tx) {
  txn_state_t *state = tx->state;
  tx->state = 0;
  if (!state)
    return success(); // probably double close?
  if (state->global_state.header.last_tx_id == state->db->active_write_tx) {
    state->db->active_write_tx = 0;
  }
  // tag::working_set_txn_close[]
  if (tx->working_set) {
    size_t iter_state = 0;
    page_t *p;
    while (hash_get_next(tx->working_set, &iter_state, &p)) {
      if (state->db->options.encrypted) {
        sodium_memzero(p->address, size_to_pages(p->overflow_size) * PAGE_SIZE);
      }
      free(p->address);
    }
    free(tx->working_set);
  }
  // end::working_set_txn_close[]

  // tag::txn_close_rollback[]
  // we have a rollback, no need to keep the memory
  if (!(state->flags & TX_COMMITED)) {
    // <1>
    while (state->on_rollback) {
      struct cleanup_act *cur = state->on_rollback;
      cur->func(cur->state);
      state->on_rollback = cur->next;
      free(cur);
    }
    // <2>
    while (state->on_forget) {
      // we didn't commit, so we can't execute it, just
      // discard it
      struct cleanup_act *cur = state->on_forget;
      state->on_forget = cur->next;
      free(cur);
    }
    // <3>
    txn_free_single_tx(state);
    return success();
  }
  // end::txn_close_rollback[]

  // <4>
  if (!state->db->transactions_to_free && state != state->db->default_read_tx)
    state->db->transactions_to_free = state;

  // <5>
  if (--state->usages == 0)
    ensure(txn_gc_tx(state));

  return success();
}
// end::txn_close[]

// tag::txn_create[]
result_t txn_create(db_t *db, uint32_t flags, txn_t *tx) {
  errors_assert_empty();
  if (db->state->options.encrypted) {
    ensure(hash_new(8, &tx->working_set));
  } else {
    tx->working_set = 0;
  }

  // <3>
  if (flags == TX_READ) {
    tx->state = db->state->last_write_tx;
    tx->state->usages++;
    return success();
  }
  // <4>
  ensure(flags == TX_WRITE,
         msg("tx_create(flags) must be either TX_WRITE or TX_READ"),
         with(flags, "%d"));
  ensure(!db->state->active_write_tx,
         msg("There can only be a single write transaction at a time, but "
             "asked for a write transaction when one is already opened"));

  txn_state_t *state = calloc(1, sizeof(txn_state_t));
  ensure(state, msg("Unable to allocate memory for transaction state"));
  size_t cancel_defer = 0;
  try_defer(free, state, cancel_defer);
  ensure(hash_new(8, &state->pages));

  state->flags = flags;
  state->db = db->state;
  state->global_state = db->state->global_state;
  // <5>
  state->prev_tx = db->state->last_write_tx;
  state->global_state.header.last_tx_id =
      state->global_state.header.last_tx_id + 1;
  tx->state = state;
  db->state->active_write_tx = state->global_state.header.last_tx_id;

  cancel_defer = 1;
  return success();
}
// end::txn_create[]

// end::expand_hash_table[]

static result_t set_page_overflow_size(txn_t *tx, page_t *p) {
  page_metadata_t *metadata;
  // need to avoid recursing into metadata if this _is_
  // the same page
  if ((p->page_num & PAGES_IN_METADATA_MASK) == p->page_num) {
    // a metadata page is always 1 page in size
    metadata = p->address;
  } else {
    ensure(txn_get_metadata(tx, p->page_num, &metadata));
  }
  p->overflow_size = metadata->overflow_size;
  return success();
}

static result_t get_metadata_entry(uint64_t page_num, page_t *metadata_page,
                                   page_metadata_t **metadata) {
  size_t index_in_page = page_num & ~PAGES_IN_METADATA_MASK;

  page_metadata_t *entries = metadata_page->address;

  ensure(entries->type == page_metadata,
         msg("Attempted to get metadata page, but wasn't marked as "
             "metadata"),
         with(metadata_page->page_num, "%lu"), with(entries->type, "%x"),
         with(page_num, "%lu"));

  *metadata = entries + index_in_page;
  return success();
}

result_t
txn_validate_page_hash(page_t *page,
                       uint8_t expected_hash[crypto_generichash_BYTES]) {
  // <1>
  uint8_t hash[crypto_generichash_BYTES];
  ensure(txn_hash_page(page, hash));

  // <2>
  if (!sodium_compare(hash, expected_hash, crypto_generichash_BYTES))
    return success();

  // <3>
  size_t size = size_to_pages(page->overflow_size) * PAGE_SIZE;
  if (sodium_is_zero(expected_hash, crypto_generichash_BYTES) &&
      sodium_is_zero(page->address, size))
    return success();

  char hash_base64[sodium_base64_ENCODED_LEN(crypto_generichash_BYTES,
                                             sodium_base64_VARIANT_ORIGINAL)];
  char expected_hash_base64[sodium_base64_ENCODED_LEN(
      crypto_generichash_BYTES, sodium_base64_VARIANT_ORIGINAL)];

  sodium_bin2base64(hash_base64,
                    sodium_base64_ENCODED_LEN(crypto_generichash_BYTES,
                                              sodium_base64_VARIANT_ORIGINAL),
                    hash, crypto_generichash_BYTES,
                    sodium_base64_VARIANT_ORIGINAL);
  sodium_bin2base64(expected_hash_base64,
                    sodium_base64_ENCODED_LEN(crypto_generichash_BYTES,
                                              sodium_base64_VARIANT_ORIGINAL),
                    expected_hash, crypto_generichash_BYTES,
                    sodium_base64_VARIANT_ORIGINAL);
  // <4>
  failed(ENODATA, msg("Unable to validate hash for page, data corruption?"),
         with(page->page_num, "%lu"), with(hash_base64, "%s"),
         with(expected_hash_base64, "%s"));
}

// tag::txn_ensure_page_is_valid[]
static result_t txn_ensure_page_is_valid(txn_t *tx, page_t *page) {
  // <1>
  if (tx->state->db->options.page_validation == page_validation_none)
    return success();

  // <2>
  if (tx->state->db->options.page_validation == page_validation_once) {
    // before the db init is completed, no checks or page that was
    // extended during this run or already checked
    if (!tx->state->db->first_read_bitmap ||
        page->page_num > tx->state->db->original_number_of_pages ||
        (tx->state->db->first_read_bitmap[page->page_num / 64] &
         (1UL << page->page_num % 64)))
      return success();
  }

  // <3>
  page_metadata_t *metadata;
  if ((page->page_num & PAGES_IN_METADATA_MASK) != page->page_num) {
    ensure(txn_get_metadata(tx, page->page_num, &metadata));
  } else {
    metadata = page->address;
  }
  ensure(txn_validate_page_hash(page, metadata->page_hash));

  // <4>
  if (tx->state->db->options.page_validation == page_validation_once) {
    // we only do it one, can skip it next time
    tx->state->db->first_read_bitmap[page->page_num / 64] |=
        (1UL << page->page_num % 64);
  }

  return success();
}
// end::txn_ensure_page_is_valid[]

// tag::txn_decrypt[]
result_t txn_decrypt(database_options_t *options, void *start, size_t size,
                     void *dest, page_metadata_t *metadata, uint64_t page_num) {
  uint8_t subkey[crypto_aead_aes256gcm_KEYBYTES];

  if (crypto_kdf_derive_from_key(subkey, crypto_aead_aes256gcm_KEYBYTES,
                                 page_num, TxnKeyCtx,
                                 options->encryption_key)) {
    failed(EINVAL, msg("Unable to derive key for page decryption"),
           with(page_num, "%ld"));
  }

  int result = crypto_aead_aes256gcm_decrypt_detached(
      dest, 0, start, size, metadata->aes_gcm.mac, 0, 0,
      metadata->aes_gcm.nonce, subkey);
  sodium_memzero(subkey, crypto_aead_aes256gcm_KEYBYTES);
  if (result) {
    if (!sodium_is_zero(start, size)) {
      failed(EINVAL, msg("Unable to decrypt page"), with(page_num, "%ld"));
    }
    memset(dest, 0, size);
  }

  return success();
}
// end::txn_decrypt[]

// tag::txn_decrypt_page[]
static result_t txn_decrypt_page(txn_t *tx, page_t *page) {
  void *buffer = 0;
  size_t cancel_defer = 0;
  try_defer(free, buffer, cancel_defer);

  if ((page->page_num & PAGES_IN_METADATA_MASK) == page->page_num) {
    ensure(palmem_allocate_pages(&buffer, 1));
    ensure(txn_decrypt(&tx->state->db->options,
                       page->address + PAGE_METADATA_CRYPTO_HEADER_SIZE,
                       PAGE_SIZE - PAGE_METADATA_CRYPTO_HEADER_SIZE,
                       buffer + PAGE_METADATA_CRYPTO_HEADER_SIZE, page->address,
                       page->page_num));
    memcpy(buffer, page->address, PAGE_METADATA_CRYPTO_HEADER_SIZE);
  } else {
    page_metadata_t *metadata;
    ensure(txn_get_metadata(tx, page->page_num, &metadata));
    uint32_t number_of_pages = size_to_pages(metadata->overflow_size);
    ensure(palmem_allocate_pages(&buffer, number_of_pages));
    ensure(txn_decrypt(&tx->state->db->options, page->address,
                       number_of_pages * PAGE_SIZE, buffer, metadata,
                       page->page_num));
  }

  page->address = buffer;
  ensure(hash_put_new(&tx->working_set, page));
  cancel_defer = 1;
  return success();
}
// end::txn_decrypt_page[]

// tag::txn_get_page[]
result_t txn_get_page(txn_t *tx, page_t *page) {
  errors_assert_empty();
  page->address = 0;
  if (hash_lookup(tx->state->pages, page) ||
      // <1>
      hash_lookup(tx->working_set, page))
    return success();

  txn_state_t *prev = tx->state->prev_tx;
  while (prev) {
    if (hash_lookup(prev->pages, page)) {
      break;
    }
    prev = prev->prev_tx;
  }

  if (!page->address)
    ensure(pages_get(&tx->state->global_state, page));

  if (tx->state->db->options.encrypted) {
    // <2>
    ensure(txn_decrypt_page(tx, page));
  } else {
    ensure(set_page_overflow_size(tx, page));
    ensure(txn_ensure_page_is_valid(tx, page));
  }

  return success();
}
// end::txn_get_page[]

// tag::txn_modify_page[]
result_t txn_modify_page(txn_t *tx, page_t *page) {
  errors_assert_empty();
  ensure(tx->state->flags & TX_WRITE,
         msg("Read transactions cannot modify the pages"),
         with(tx->state->flags, "%d"));

  ensure(tx->state->global_state.header.number_of_pages > page->page_num,
         msg("Cannot modify a page beyond the end of the file"),
         with(tx->state->global_state.header.number_of_pages, "%lu"),
         with(page->page_num, "%lu"));

  if (hash_lookup(tx->state->pages, page)) {
    return success();
  }

  page_t original = {.page_num = page->page_num};
  ensure(txn_get_page(tx, &original));
  if (!original.overflow_size) {
    original.overflow_size = page->overflow_size;
  }
  if (!original.overflow_size)
    original.overflow_size = PAGE_SIZE;

  uint32_t pages = original.overflow_size / PAGE_SIZE +
                   (original.overflow_size % PAGE_SIZE ? 1 : 0);

  ensure(palmem_allocate_pages(&page->address, pages),
         msg("Unable to allocate memory for a COW page"));

  size_t cancel_defer = 0;
  try_defer(free, page->address, cancel_defer);
  memcpy(page->address, original.address, PAGE_SIZE * pages);
  page->previous = original.address;
  page->overflow_size = original.overflow_size;

  ensure(hash_put_new(&tx->state->pages, page),
         msg("Failed to allocate entry"));

  cancel_defer = 1;
  return success();
}
// end::txn_modify_page[]

// tag::page_metadata[]
// <6>
result_t txn_get_metadata(txn_t *tx, uint64_t page_num,
                          page_metadata_t **metadata) {
  page_t metadata_page = {.page_num = page_num & PAGES_IN_METADATA_MASK};

  ensure(txn_get_page(tx, &metadata_page));

  return get_metadata_entry(page_num, &metadata_page, metadata);
}

result_t txn_modify_metadata(txn_t *tx, uint64_t page_num,
                             page_metadata_t **metadata) {
  page_t metadata_page = {.page_num = page_num & PAGES_IN_METADATA_MASK};

  ensure(txn_modify_page(tx, &metadata_page));

  return get_metadata_entry(page_num, &metadata_page, metadata);
}
// end::page_metadata[]

// tag::txn_add_action[]
static result_t txn_add_action(struct cleanup_act **head,
                               void (*action)(void *), void *state_to_copy,
                               size_t size_of_state) {
  struct cleanup_act *cur =
      calloc(1, sizeof(struct cleanup_act) + sizeof(struct mmap_args));
  if (!cur) {
    failed(ENOMEM, msg("Unable to allocate memory to register txn action"));
  }
  memcpy(cur->state, state_to_copy, size_of_state);
  cur->func = action;
  cur->next = *head;
  *head = cur;
  return success();
}

result_t txn_register_on_forget(txn_state_t *tx, void (*action)(void *),
                                void *state_to_copy, size_t size_of_state) {
  return txn_add_action(&tx->on_forget, action, state_to_copy, size_of_state);
}
result_t txn_register_on_rollback(txn_state_t *tx, void (*action)(void *),
                                  void *state_to_copy, size_t size_of_state) {
  return txn_add_action(&tx->on_rollback, action, state_to_copy, size_of_state);
}
// end::txn_add_action[]
