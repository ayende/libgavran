#include <errno.h>
#include <string.h>

#include "db.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::lookup_entry_in_tx[]
#define get_number_of_buckets(state)                                           \
  ((state->allocated_size - sizeof(txn_state_t)) / sizeof(page_t))

static result_t allocate_entry_in_tx(txn_state_t **state_ptr, page_t *page);

static bool lookup_entry_in_tx(txn_state_t *state, page_t *page) {
  uint64_t page_num = page->page_num;
  size_t number_of_buckets = get_number_of_buckets(state);
  if (!number_of_buckets)
    return false;
  size_t starting_pos = (size_t)(page_num % number_of_buckets);
  // we use linear probing to find a value in case of collisions
  for (size_t i = 0; i < number_of_buckets; i++) {
    size_t index = (i + starting_pos) % number_of_buckets;
    if (!state->entries[index].address) {
      // empty value, so there is no match
      return false;
    }
    if (state->entries[index].page_num == page_num) {
      memcpy(page, &state->entries[index], sizeof(page_t));
      return true;
    }
  }
  return false;
}
// end::lookup_entry_in_tx[]

// tag::txn_commit[]
result_t txn_commit(txn_t *tx) {
  errors_assert_empty();

  // no modified pages, nothing to do here
  if (!tx->state->modified_pages)
    return success();

  tx->state->flags |= TX_COMMITED;
  tx->state->usages = 1;
  tx->state->db->last_write_tx->next_transaction = tx->state;
  tx->state->db->last_write_tx = tx->state;
  return success();
}
// end::txn_commit[]

static result_t txn_apply(txn_state_t *state) {
  size_t number_of_buckets = get_number_of_buckets(state);

  for (size_t i = 0; i < number_of_buckets; i++) {
    page_t *entry = &state->entries[i];
    if (!entry->address)
      continue;

    ensure(pages_write(state->db, entry));
  }

  return success();
}

static void txn_cleanup(txn_state_t *state) {
  size_t number_of_buckets = get_number_of_buckets(state);
  for (size_t i = 0; i < number_of_buckets; i++) {
    page_t *entry = &state->entries[i];
    if (!entry->address)
      continue;

    free(entry->address);
  }

  free(state);
}

static void txn_cleanup_list(txn_state_t *state) {
  if (!state)
    return;
  txn_state_t *default_read_tx = state->db->default_read_tx;
  while (state != default_read_tx) {
    txn_state_t *cur = state;
    state = state->previous_write_tx;
    txn_cleanup(cur);
  }
}

static result_t txn_try_apply(txn_state_t *state) {
  txn_state_t *latest_unused = state->db->default_read_tx;
  if (latest_unused->usages)
    // transactions looking at the file directly, cannot proceed
    return success();

  while (latest_unused->next_transaction &&
         latest_unused->next_transaction->usages == 0) {
    latest_unused = latest_unused->next_transaction;
  }
  if (latest_unused == state->db->default_read_tx)
    return success(); // no work to be done

  // build a table of all the pages that we want to write
  txn_state_t *prev_unused = latest_unused->previous_write_tx;
  while (prev_unused) {
    size_t number_of_buckets = get_number_of_buckets(prev_unused);
    for (size_t i = 0; i < number_of_buckets; i++) {
      page_t *entry = &state->entries[i];
      if (!entry->address)
        continue;

      if (lookup_entry_in_tx(latest_unused, entry))
        continue; // newer value exists, skip

      ensure(allocate_entry_in_tx(&latest_unused, entry));
    }

    prev_unused = prev_unused->previous_write_tx;
  }

  // write all these pages to disk
  ensure(txn_apply(latest_unused));

  // free memory from other transactions that became unreachable
  txn_cleanup_list(latest_unused->transactions_to_free);

  // update the chain
  if (latest_unused->next_transaction) {
    state->db->default_read_tx->next_transaction =
        latest_unused->next_transaction;

    // we can only free when all the current transactions are closed
    latest_unused->transactions_to_free =
        state->db->last_write_tx->transactions_to_free;
    state->db->last_write_tx->transactions_to_free = latest_unused;

  } else {
    state->db->last_write_tx = state->db->default_read_tx;
    state->db->default_read_tx->next_transaction = 0;
    // no one is watching, we can clear this
    txn_cleanup_list(latest_unused);
  }

  return success();
}

// tag::txn_close[]

result_t txn_close(txn_t *tx) {
  txn_state_t *state = tx->state;
  tx->state = 0;
  if (!state)
    return success(); // probably double close?

  // we have a rollback, no need to keep the memory
  if (!(state->flags & TX_COMMITED)) {
    txn_cleanup(state);
    return success();
  }

  state->usages--;
  ensure(txn_try_apply(state));

  return success();
}
// end::txn_close[]

// tag::txn_create[]
result_t txn_create(db_t *db, uint32_t flags, txn_t *tx) {
  errors_assert_empty();

  // <3>
  if (flags & TX_READ) {
    tx->state = db->state->last_write_tx;
    tx->state->usages++;
    return success();
  }
  ensure(!db->state->current_write_tx,
         msg("There can only be a single write transaction at a time, but "
             "asked for a write transaction when one is already opened"));

  size_t initial_size = sizeof(txn_state_t) + sizeof(page_t) * 8;
  txn_state_t *state = calloc(1, initial_size);
  ensure(state, msg("Unable to allocate memory for transaction state"));

  state->allocated_size = initial_size;
  state->flags = flags;
  state->db = db->state;
  // <4>
  state->previous_write_tx = db->state->last_write_tx;
  state->tx_id = db->state->last_write_tx->tx_id + 1;
  tx->state = state;
  return success();
}
// end::txn_create[]

enum hash_resize_status {
  hash_resize_success,
  hash_resize_err_no_mem,
  hash_resize_err_failure,
};

// tag::expand_hash_table[]
static enum hash_resize_status expand_hash_table(txn_state_t **state_ptr,
                                                 size_t number_of_buckets) {
  size_t new_number_of_buckets = number_of_buckets * 2;
  size_t new_size =
      sizeof(txn_state_t) + (new_number_of_buckets * sizeof(page_t));
  txn_state_t *state = *state_ptr;
  txn_state_t *new_state = calloc(1, new_size);
  if (!new_state) {
    // we are OOM, but we'll accept that and let the hash
    // table fill to higher capacity, caller may decide to
    // error
    return hash_resize_err_no_mem;
  }
  memcpy(new_state, state, sizeof(txn_state_t));
  new_state->allocated_size = new_size;

  for (size_t i = 0; i < number_of_buckets; i++) {
    page_t *entry = &state->entries[i];
    if (!entry->address)
      continue;
    size_t starting_pos = entry->page_num % new_number_of_buckets;
    bool located = false;
    for (size_t j = 0; j < new_number_of_buckets; j++) {
      size_t index = (j + starting_pos) % new_number_of_buckets;
      if (!new_state->entries[index].address) { // empty
        new_state->entries[index] = state->entries[i];
        located = true;
        break;
      }
    }
    if (!located) {
      errors_push(EINVAL,
                  msg("Failed to find spot for page after hash table resize"),
                  with(entry->page_num, "%lu"));
      free(new_state);
      return hash_resize_err_failure;
    }
  }

  *state_ptr = new_state; // update caller's reference
  free(state);
  return hash_resize_success;
}
// end::expand_hash_table[]

// tag::allocate_entry_in_tx[]
static result_t allocate_entry_in_tx(txn_state_t **state_ptr, page_t *page) {
  uint64_t page_num = page->page_num;
  txn_state_t *state = *state_ptr;
  size_t number_of_buckets = get_number_of_buckets(state);
  size_t starting_pos = (size_t)(page_num % number_of_buckets);
  bool placed_successfully = false;
  // we use linear probing to find a value in case of collisions
  for (size_t i = 0; i < number_of_buckets; i++) {
    size_t index = (i + starting_pos) % number_of_buckets;
    if (state->entries[index].page_num == page_num &&
        state->entries[index].address) {
      failed(EINVAL,
             msg("Attempted to allocate entry for page "
                 "which already exist in the table"),
             with(page_num, "%lu"));
    }

    if (!state->entries[index].address) {
      state->entries[index].page_num = page_num;
      memcpy(&state->entries[index], page, sizeof(page_t));
      state->modified_pages++;

      // check the load factor
      size_t max_pages = (number_of_buckets * 3 / 4);
      if (state->modified_pages + 1 < max_pages) {
        return success();
      }
      placed_successfully = true;
      break;
    }
  }

  switch (expand_hash_table(state_ptr, number_of_buckets)) {
  case hash_resize_success:
    if (placed_successfully) {
      return success();
    }
    // try again after expansion
    return allocate_entry_in_tx(state_ptr, page);
  case hash_resize_err_no_mem:
    // we already placed the current value, so can ignore
    if (placed_successfully) {
      return success();
    }
    // we are at 100% capacity, can't recover, will error now
    failed(ENOMEM,
           msg("Can't allocate to add page to the "
               "transaction hash table"),
           with(page_num, "%lu"));
  case hash_resize_err_failure:
    failed(EINVAL,
           msg("Failed to add page to the transaction "
               "hash table"),
           with(page_num, "%lu"));
  }
}
// end::allocate_entry_in_tx[]

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

// tag::txn_get_page[]
result_t txn_get_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  txn_state_t *cur = tx->state;
  // <1>
  while (cur) {
    if (lookup_entry_in_tx(cur, page)) {
      return success();
    }
    cur = cur->previous_write_tx;
  }

  ensure(pages_get(tx->state->db, page));

  ensure(set_page_overflow_size(tx, page));

  return success();
}
// end::txn_get_page[]

// tag::txn_modify_page[]
result_t txn_modify_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  // <1>
  ensure(tx->state->flags & TX_WRITE,
         msg("Read transactions cannot modify the pages"));

  // <2>
  if (lookup_entry_in_tx(tx->state, page)) {
    return success();
  }

  // <3>
  page_t original = {.page_num = page->page_num};
  txn_state_t *cur = tx->state->previous_write_tx;
  while (cur) {
    if (lookup_entry_in_tx(cur, &original)) {
      break;
    }
    cur = cur->previous_write_tx;
  }

  // <4>
  if (!page->address) {
    if (!page->overflow_size)
      page->overflow_size = PAGE_SIZE;

    ensure(pages_get(tx->state->db, &original));
    ensure(set_page_overflow_size(tx, &original));
  }

  // <5>
  uint32_t pages = page->overflow_size / PAGE_SIZE +
                   (page->overflow_size % PAGE_SIZE ? 1 : 0);

  ensure(palmem_allocate_pages(&page->address, pages),
         msg("Unable to allocate memory for a COW page"));

  size_t cancel_defer = 0;
  try_defer(free, page->address, cancel_defer);
  memcpy(page->address, original.address, PAGE_SIZE * pages);

  ensure(allocate_entry_in_tx(&tx->state, page),
         msg("Failed to allocate entry"));

  cancel_defer = 1;
  return success();
}
// end::txn_modify_page[]

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
