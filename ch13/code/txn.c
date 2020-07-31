#include <gavran/db.h>
#include <gavran/internal.h>
#include <string.h>

// tag::txn_create[]
// tag::txn_create_working_set[]
result_t txn_create(db_t *db, db_flags_t flags, txn_t *tx) {
  errors_assert_empty();
  if (db->state->options.flags & db_flags_page_need_txn_working_set) {
    ensure(hash_new(8, &tx->working_set));
  } else {
    tx->working_set = 0;
  }
  // end::txn_create_working_set[]
  // <1>
  if (flags == TX_READ) {
    tx->state = db->state->last_write_tx;
    tx->state->usages++;
    return success();
  }
  if ((db->state->options.flags & db_flags_log_shipping_target)) {
    ensure(flags & txn_flags_apply_log,
           msg("txn_create(flags) must have txn_flags_apply_log when "
               "running in log shipping mode"),
           with(flags, "%d"));
  }

  ensure(flags & TX_WRITE,
         msg("txn_create(flags) must be flagged with either TX_WRITE "
             "or TX_READ"),
         with(flags, "%d"));
  ensure(!db->state->active_write_tx,
         msg("Opening a second write transaction is forbidden"));

  size_t cancel_defer = 0;
  txn_state_t *state;
  ensure(mem_calloc((void *)&state, sizeof(txn_state_t)));
  try_defer(free, state, cancel_defer);

  ensure(hash_new(8, &state->modified_pages));

  state->flags = flags | db->state->options.flags;
  state->db = db->state;
  state->map = db->state->map;
  state->number_of_pages = db->state->number_of_pages;
  // <3>
  state->prev_tx = db->state->last_write_tx;
  state->tx_id = db->state->last_tx_id + 1;
  db->state->active_write_tx = state->tx_id;

  tx->state = state;
  cancel_defer = 1;
  return success();
}
// end::txn_create[]

static result_t txn_hash_page(page_t *page,
                              uint8_t hash[crypto_generichash_BYTES]);

// tag::txn_validate_page[]
static result_t txn_validate_page_hash(
    page_t *page, uint8_t expected_hash[crypto_generichash_BYTES]) {
  // <1>
  uint8_t hash[crypto_generichash_BYTES];
  ensure(txn_hash_page(page, hash));
  // <2>
  if (!memcmp(hash, expected_hash, crypto_generichash_BYTES))
    return success();
  // <3>
  if (sodium_is_zero(expected_hash, crypto_generichash_BYTES) &&
      sodium_is_zero(page->address,
                     page->number_of_pages * PAGE_SIZE))
    return success();
  // <4>
  failed(ENODATA,
         msg("Unable to validate hash for page, data corruption?"),
         with(page->page_num, "%lu"));
}
static result_t txn_validate_page(txn_t *tx, page_t *page) {
  page_metadata_t *metadata;
  if ((page->page_num & PAGES_IN_METADATA_MASK) != page->page_num) {
    ensure(txn_get_metadata(tx, page->page_num, &metadata));
  } else {
    metadata = page->address;
  }
  ensure(txn_validate_page_hash(page, metadata->cyrpto.hash_blake2b));
  return success();
}
// end::txn_validate_page[]

// tag::txn_ensure_page_is_valid[]
static result_t txn_ensure_page_is_valid(txn_t *tx, page_t *page) {
  if ((tx->state->flags & db_flags_page_validation_none) ==
      db_flags_page_validation_none)
    return success();
  if (tx->state->flags & db_flags_page_validation_always) {
    ensure(txn_validate_page(tx, page));
    return success();
  }
  if ((tx->state->flags & db_flags_page_validation_once) == 0)
    return success();

  db_state_t *db = tx->state->db;
  uint64_t *bitmap = db->first_read_bitmap;
  // before the db init is completed or extended during this run
  if (!bitmap || page->page_num >= db->original_number_of_pages ||
      // already checked
      (bitmap[page->page_num / 64] & (1UL << page->page_num % 64)))
    return success();
  ensure(txn_validate_page(tx, page));
  // we only do it one, can skip it next time
  bitmap[page->page_num / 64] |= (1UL << page->page_num % 64);
  return success();
}
// end::txn_ensure_page_is_valid[]

// tag::txn_generate_nonce[]
static void txn_generate_nonce(page_metadata_t *metadata) {
  if (sodium_is_zero(metadata->cyrpto.aead.nonce,
                     PAGE_METADATA_CRYPTO_NONCE_SIZE)) {
    randombytes_buf(metadata->cyrpto.aead.nonce,
                    PAGE_METADATA_CRYPTO_NONCE_SIZE);
  } else {
    sodium_increment(metadata->cyrpto.aead.nonce,
                     PAGE_METADATA_CRYPTO_NONCE_SIZE);
  }
}
static void txn_set_nonce(
    page_metadata_t *metadata,
    uint8_t nonce[crypto_aead_xchacha20poly1305_IETF_NPUBBYTES]) {
  memcpy(nonce, metadata->cyrpto.aead.nonce,
         PAGE_METADATA_CRYPTO_NONCE_SIZE);
  memset(nonce + PAGE_METADATA_CRYPTO_NONCE_SIZE, 0,
         crypto_aead_xchacha20poly1305_IETF_NPUBBYTES -
             PAGE_METADATA_CRYPTO_NONCE_SIZE);
}
// end::txn_generate_nonce[]

// tag::txn_encrypt_page[]
static const char TxnKeyCtx[8] = "TxnPages";
static result_t txn_encrypt_page(txn_t *tx, uint64_t page_num,
                                 void *start, size_t size,
                                 page_metadata_t *metadata) {
  // <1>
  uint8_t subkey[crypto_aead_xchacha20poly1305_IETF_KEYBYTES];
  if (crypto_kdf_derive_from_key(
          subkey, crypto_aead_xchacha20poly1305_IETF_KEYBYTES,
          page_num, TxnKeyCtx,
          tx->state->db->options.encryption_key)) {
    failed(EINVAL, msg("Unable to derive key for page decryption"),
           with(page_num, "%ld"));
  }
  uint8_t nonce[crypto_aead_xchacha20poly1305_IETF_NPUBBYTES];
  // <2>
  txn_generate_nonce(metadata);
  txn_set_nonce(metadata, nonce);
  // <3>
  int result = crypto_aead_xchacha20poly1305_ietf_encrypt_detached(
      start, metadata->cyrpto.aead.mac, 0, start, size, 0, 0, 0,
      nonce, subkey);
  sodium_memzero(subkey, crypto_aead_xchacha20poly1305_IETF_KEYBYTES);
  if (result) {
    failed(EINVAL, msg("Unable to encrypt page"),
           with(page_num, "%ld"));
  }
  return success();
}
// end::txn_encrypt_page[]

// tag::txn_decrypt[]
static result_t txn_decrypt(db_options_t *options, void *start,
                            size_t size, void *dest,
                            page_metadata_t *metadata,
                            uint64_t page_num) {
  uint8_t subkey[crypto_aead_xchacha20poly1305_ietf_KEYBYTES];

  if (crypto_kdf_derive_from_key(
          subkey, crypto_aead_xchacha20poly1305_ietf_KEYBYTES,
          page_num, TxnKeyCtx, options->encryption_key)) {
    failed(EINVAL, msg("Unable to derive key for page decryption"),
           with(page_num, "%ld"));
  }
  uint8_t nonce[crypto_aead_xchacha20poly1305_ietf_NPUBBYTES];
  txn_set_nonce(metadata, nonce);
  int result = crypto_aead_xchacha20poly1305_ietf_decrypt_detached(
      dest, 0, start, size, metadata->cyrpto.aead.mac, 0, 0, nonce,
      subkey);
  sodium_memzero(subkey, crypto_aead_xchacha20poly1305_ietf_KEYBYTES);
  if (result) {
    if (!sodium_is_zero(start, size) &&
        !sodium_is_zero(metadata->cyrpto.aead.mac,
                        crypto_aead_xchacha20poly1305_ietf_ABYTES)) {
      failed(EINVAL, msg("Unable to decrypt page"),
             with(page_num, "%ld"));
    }
    memset(dest, 0, size);
  }
  return success();
}
// end::txn_decrypt[]

// tag::txn_decrypt_page[]
static result_t txn_decrypt_page(txn_t *tx, page_t *page) {
  size_t cancel_defer = 0;
  void *buffer = 0;
  ensure(mem_alloc_page_aligned(&buffer,
                                page->number_of_pages * PAGE_SIZE));
  try_defer(free, buffer, cancel_defer);
  if ((page->page_num & PAGES_IN_METADATA_MASK) == page->page_num) {
    size_t shift = PAGE_METADATA_CRYPTO_HEADER_SIZE;
    ensure(txn_decrypt(&tx->state->db->options, page->address + shift,
                       PAGE_SIZE - shift, buffer + shift,
                       page->address, page->page_num));
    memcpy(buffer, page->address, shift);
  } else {
    page_metadata_t *metadata;
    ensure(txn_get_metadata(tx, page->page_num, &metadata));
    ensure(txn_decrypt(&tx->state->db->options, page->address,
                       page->number_of_pages * PAGE_SIZE, buffer,
                       metadata, page->page_num));
  }
  // <1>
  page_t existing = {.page_num = page->page_num};
  if (hash_lookup(tx->working_set, &existing)) {
    // this can happen if we are using encryption AND 32 bits mode
    // let's replace the encrypted content with the plain text one
    memcpy(existing.address, buffer,
           page->number_of_pages * PAGE_SIZE);
    sodium_memzero(buffer, page->number_of_pages * PAGE_SIZE);
    free(buffer);
    buffer = 0;
    memcpy(page, &existing, sizeof(page_t));
  } else {
    page->address = buffer;
    ensure(hash_put_new(&tx->working_set, page));
  }
  cancel_defer = 1;
  return success();
}
// end::txn_decrypt_page[]

// tag::txn_raw_get_page[]
result_t txn_raw_get_page(txn_t *tx, page_t *page) {
  errors_assert_empty();
  page->address = 0;
  if (!(tx->state->flags & TX_COMMITED) &&
      hash_lookup(tx->state->modified_pages, page))
    return success();
  if (hash_lookup(tx->working_set, page)) return success();
  txn_state_t *prev = tx->state;
  while (prev) {
    if (hash_lookup(prev->modified_pages, page)) break;
    prev = prev->prev_tx;
  }

  if (!page->address) {
    if (!page->number_of_pages) page->number_of_pages = 1;
    ensure(pages_get(tx, page));
  }

  // <1>
  if (!(tx->state->flags & txn_flags_apply_log)) {
    if (tx->state->flags & db_flags_encrypted) {
      ensure(txn_decrypt_page(tx, page));
    } else {
      ensure(txn_ensure_page_is_valid(tx, page));
    }
  }

  return success();
}
// end::txn_raw_get_page[]

// tag::txn_raw_modify_page[]
result_t txn_raw_modify_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  ensure(tx->state->flags & TX_WRITE,
         msg("Read transactions cannot modify the pages"),
         with(tx->state->flags, "%d"));

  if (hash_lookup(tx->state->modified_pages, page)) {
    return success();
  }
  // end::txn_raw_modify_page[]

  size_t done = 0;
  if (!page->number_of_pages) page->number_of_pages = 1;
  ensure(mem_alloc_page_aligned(&page->address,
                                PAGE_SIZE * page->number_of_pages));
  try_defer(free, page->address, done);
  page_t original = {.page_num = page->page_num};
  ensure(txn_raw_get_page(tx, &original));
  if (original.number_of_pages == page->number_of_pages) {
    memcpy(page->address, original.address,
           (PAGE_SIZE * page->number_of_pages));
    page->previous = original.address;
  } else {  // mismatch in size means that we consider to be new only
    memset(page->address, 0, (PAGE_SIZE * page->number_of_pages));
    page->previous = 0;
  }
  ensure(hash_put_new(&tx->state->modified_pages, page),
         msg("Failed to allocate entry"));
  done = 1;
  return success();
}

// tag::txn_hash_page[]
static result_t txn_hash_page(
    page_t *page, uint8_t hash[crypto_generichash_BYTES]) {
  bool is_metadata_page =
      (page->page_num & PAGES_IN_METADATA_MASK) == page->page_num;

  void *start = is_metadata_page
                    ? page->address + crypto_generichash_BYTES
                    : page->address;
  size_t size = is_metadata_page ? PAGE_SIZE - sizeof(page_metadata_t)
                                 : page->number_of_pages * PAGE_SIZE;

  if (crypto_generichash(hash, crypto_generichash_BYTES, start, size,
                         0, 0)) {
    failed(
        ENODATA,
        msg("Unable to compute page hash for page, shouldn't happen"),
        with(page->page_num, "%lu"));
  }
  return success();
}
// end::txn_hash_page[]

// tag::tx_finalize_page[]
static result_t tx_finalize_page(txn_t *tx, page_t *page,
                                 page_metadata_t *metadata) {
  if (tx->state->flags & db_flags_encrypted) {
    if ((page->page_num & PAGES_IN_METADATA_MASK) == page->page_num) {
      size_t shift = PAGE_METADATA_CRYPTO_HEADER_SIZE;
      return txn_encrypt_page(tx, page->page_num,
                              page->address + shift,
                              PAGE_SIZE - shift, metadata);
    }
    return txn_encrypt_page(tx, page->page_num, page->address,
                            page->number_of_pages * PAGE_SIZE,
                            metadata);
  } else {
    return txn_hash_page(page, metadata->cyrpto.hash_blake2b);
  }
}
// end::tx_finalize_page[]

// tag::txn_finalize_modified_pages[]
static result_t txn_finalize_modified_pages(txn_t *tx) {
  txn_state_t *state = tx->state;
  // <1>
  page_t *modified_pages;
  ensure(mem_calloc((void *)&modified_pages,
                    state->modified_pages->count * sizeof(page_t)));
  defer(free, modified_pages);
  size_t modified_pages_idx = 0;
  size_t iter_state = 0;
  page_t *current;
  while (hash_get_next(tx->state->modified_pages, &iter_state,
                       &current)) {
    // <2>
    // can't modify in place, the hash may change, need a copy
    memcpy(&modified_pages[modified_pages_idx++], current,
           sizeof(page_t));
  }
  // <3>
  for (size_t i = 0; i < modified_pages_idx; i++) {
    page_metadata_t *metadata;
    ensure(txn_modify_metadata(tx, modified_pages[i].page_num,
                               &metadata));
    if ((modified_pages[i].page_num & PAGES_IN_METADATA_MASK) ==
        modified_pages[i].page_num)
      // we handle metadata page separately, note that metadata pages
      // *must* be modified, that is why we call modify metadat first
      continue;

    ensure(tx_finalize_page(tx, &modified_pages[i], metadata));
  }
  // <4>
  iter_state = 0;
  while (hash_get_next(tx->state->modified_pages, &iter_state,
                       &current)) {
    if ((current->page_num & PAGES_IN_METADATA_MASK) !=
        current->page_num)
      continue;  // not a metadata page
    page_metadata_t *entries = current->address;

    ensure(tx_finalize_page(tx, current, entries));
  }
  return success();
}
// end::txn_finalize_modified_pages[]

// tag::txn_commit[]
result_t txn_commit(txn_t *tx) {
  errors_assert_empty();
  if (!tx->state->modified_pages->count) return success();

  // <1>
  if (!(tx->state->flags & txn_flags_apply_log)) {
    page_metadata_t *header;
    ensure(txn_modify_metadata(tx, 0, &header));
    header->file_header.last_tx_id = tx->state->tx_id;
    ensure(txn_finalize_modified_pages(tx));
  }

  ensure(wal_append(tx->state));
  // end::txn_commit[]

  tx->state->flags |= TX_COMMITED;
  tx->state->usages = 1;

  // <1>
  // Update global references to the current span on commit
  tx->state->db->last_write_tx->next_tx = tx->state;
  tx->state->db->last_write_tx = tx->state;
  tx->state->db->last_tx_id = tx->state->tx_id;
  tx->state->db->map = tx->state->map;
  tx->state->db->number_of_pages = tx->state->number_of_pages;

  // <2>
  while (tx->state->on_rollback) {
    cleanup_callback_t *cur = tx->state->on_rollback;
    tx->state->on_rollback = cur->next;
    free(cur);
  }

  return success();
}

// tag::txn_free_single_tx_state[]
implementation_detail void txn_free_single_tx_state(
    txn_state_t *state) {
  size_t iter_state = 0;
  page_t *p;
  while (hash_get_next(state->modified_pages, &iter_state, &p)) {
    free(p->address);
  }
  // <1>
  while (state->on_forget) {
    cleanup_callback_t *cur = state->on_forget;
    cur->func(cur->state);
    state->on_forget = cur->next;
    free(cur);
  }
  free(state->modified_pages);
  free(state);
}
// end::txn_free_single_tx_state[]

// tag::txn_free_registered_transactions[]
static void txn_free_registered_transactions(db_state_t *state) {
  while (state->transactions_to_free) {
    txn_state_t *cur = state->transactions_to_free;

    if (cur->usages ||
        cur->can_free_after_tx_id > state->oldest_active_tx)
      break;

    if (cur->next_tx) cur->next_tx->prev_tx = 0;

    state->transactions_to_free = cur->next_tx;
    state->default_read_tx->next_tx = cur->next_tx;
    state->default_read_tx->map = cur->map;
    state->default_read_tx->number_of_pages = cur->number_of_pages;
    if (state->last_write_tx == cur)
      state->last_write_tx = state->default_read_tx;

    txn_free_single_tx_state(cur);
  }
}
// end::txn_free_registered_transactions[]

// tag::txn_write_state_to_disk[]
static result_t txn_write_state_to_disk(txn_state_t *s) {
  size_t iter_state = 0;
  page_t *current;
  while (hash_get_next(s->modified_pages, &iter_state, &current)) {
    ensure(pages_write(s->db, current));
  }
  // <1>
  if (wal_will_checkpoint(s->db, s->tx_id)) {
    ensure(pal_fsync(s->db->handle));
    ensure(wal_checkpoint(s->db, s->tx_id));
  }
  return success();
}
// end::txn_write_state_to_disk[]

// tag::txn_merge_unique_pages[]
static result_t txn_merge_unique_pages(txn_state_t *state) {
  // state-modified_pages will have distinct set of the latest pages
  // that we want to write
  txn_state_t *prev = state->prev_tx;
  while (prev) {
    size_t iter_state = 0;
    page_t *entry;
    while (hash_get_next(prev->modified_pages, &iter_state, &entry)) {
      page_t check = {.page_num = entry->page_num};
      if (hash_lookup(state->modified_pages, &check)) continue;

      ensure(hash_put_new(&state->modified_pages, entry));
      entry->address = 0;  // ownership changed, avoid double free
    }
    prev = prev->prev_tx;
  }
  return success();
}
// end::txn_merge_unique_pages[]

// tag::txn_gc[]
static result_t txn_gc(txn_state_t *state) {
  // <1>
  db_state_t *db = state->db;
  state->can_free_after_tx_id = db->last_tx_id + 1;
  // <2>
  txn_state_t *latest_unused = state->db->default_read_tx;
  if (latest_unused->usages)  // tx using the file directly
    return success();
  // <3>
  while (latest_unused->next_tx &&
         latest_unused->next_tx->usages == 0) {
    latest_unused = latest_unused->next_tx;
  }
  if (latest_unused == db->default_read_tx) {
    return success();  // no work to be done
  }
  // <4>
  db->oldest_active_tx = latest_unused->tx_id + 1;
  // no one is looking, can release immediately
  if (latest_unused == db->last_write_tx) {
    latest_unused->can_free_after_tx_id = db->last_tx_id;
  }
  // <5>
  ensure(txn_merge_unique_pages(latest_unused));
  ensure(txn_write_state_to_disk(latest_unused));
  txn_free_registered_transactions(db);
  return success();
}
// end::txn_gc[]

// tag::txn_close[]
// tag::working_set_txn_close[]
implementation_detail void txn_clear_working_set(txn_t *tx) {
  if (tx->working_set) {
    size_t iter_state = 0;
    page_t *p;
    while (hash_get_next(tx->working_set, &iter_state, &p)) {
      if (tx->state->flags & db_flags_encrypted) {
        sodium_memzero(p->address, p->number_of_pages * PAGE_SIZE);
      }
      free(p->address);
    }
    free(tx->working_set);
  }
}
result_t txn_close(txn_t *tx) {
  if (!tx || !tx->state) return success();
  db_state_t *db = tx->state->db;
  if (tx->state->tx_id == db->active_write_tx) {
    db->active_write_tx = 0;
  }
  txn_clear_working_set(tx);
  // end::working_set_txn_close[]
  if (!(tx->state->flags & TX_COMMITED)) {  // rollback
    // <1>
    while (tx->state->on_rollback) {
      cleanup_callback_t *cur = tx->state->on_rollback;
      cur->func(cur->state);
      tx->state->on_rollback = cur->next;
      free(cur);
    }
    // <2>
    while (tx->state->on_forget) {
      // we didn't commit, can just discard this
      cleanup_callback_t *cur = tx->state->on_forget;
      tx->state->on_forget = cur->next;
      free(cur);
    }
    txn_free_single_tx_state(tx->state);
    tx->state = 0;
    return success();
  }
  if (!db->transactions_to_free && tx->state != db->default_read_tx)
    db->transactions_to_free = tx->state;

  if (--tx->state->usages == 0) {
    ensure(txn_gc(tx->state));
  }

  tx->state = 0;
  return success();
}
// end::txn_close[]

// tag::txn_register_cleanup_action[]
result_t txn_register_cleanup_action(cleanup_callback_t **head,
                                     void (*action)(void *),
                                     void *state_to_copy,
                                     size_t size_of_state) {
  cleanup_callback_t *cur;
  ensure(mem_calloc((void *)&cur,
                    sizeof(cleanup_callback_t) + size_of_state));
  memcpy(cur->state, state_to_copy, size_of_state);
  cur->func = action;
  cur->next = *head;
  *head = cur;
  return success();
}
// end::txn_register_cleanup_action[]
