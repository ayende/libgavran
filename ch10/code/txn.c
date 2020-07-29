#include <gavran/db.h>
#include <gavran/internal.h>
#include <string.h>

// tag::txn_create[]
result_t txn_create(db_t *db, uint32_t flags, txn_t *tx) {
  errors_assert_empty();
  // <1>
  if (flags == TX_READ) {
    tx->state = db->state->last_write_tx;
    tx->state->usages++;
    return success();
  }
  // <2>
  ensure(flags == TX_WRITE,
         msg("txn_create(flags) must be either TX_WRITE or TX_READ"),
         with(flags, "%d"));
  ensure(!db->state->active_write_tx,
         msg("Opening a second write transaction is forbidden"));

  size_t cancel_defer = 0;
  txn_state_t *state;
  ensure(mem_calloc((void *)&state, sizeof(txn_state_t)));
  try_defer(free, state, cancel_defer);

  ensure(hash_new(8, &state->modified_pages));

  state->flags = flags;
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

// tag::txn_raw_get_page[]
result_t txn_raw_get_page(txn_t *tx, page_t *page) {
  errors_assert_empty();
  page->address = 0;
  if (hash_lookup(tx->state->modified_pages, page)) return success();
  // <1>
  txn_state_t *prev = tx->state->prev_tx;
  while (prev) {
    if (hash_lookup(prev->modified_pages, page)) return success();
    prev = prev->prev_tx;
  }

  if (!page->address) {
    if (!page->number_of_pages) page->number_of_pages = 1;
    ensure(pages_get(tx, page));
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

// tag::txn_commit[]
result_t txn_commit(txn_t *tx) {
  errors_assert_empty();
  if (!tx->state->modified_pages->count) return success();
  page_metadata_t *header;
  ensure(txn_modify_metadata(tx, 0, &header));
  header->file_header.last_tx_id = tx->state->tx_id;

  ensure(wal_append(tx->state));

  tx->state->flags = TX_COMMITED;
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
// end::txn_commit[]

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
result_t txn_close(txn_t *tx) {
  if (!tx || !tx->state) return success();
  db_state_t *db = tx->state->db;
  if (tx->state->tx_id == db->active_write_tx) {
    db->active_write_tx = 0;
  }
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
