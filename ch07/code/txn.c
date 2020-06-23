#include <errno.h>
#include <stdio.h>
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

  // <1>
  tx->state->flags |= TX_COMMITED;
  tx->state->usages = 1;
  tx->state->db->last_write_tx->next_tx = tx->state;
  tx->state->db->last_write_tx = tx->state;
  tx->state->db->last_tx_id = tx->state->tx_id;
  return success();
}
// end::txn_commit[]

// tag::txn_write_state_to_disk[]
static result_t txn_write_state_to_disk(txn_state_t *state) {
  size_t number_of_buckets = get_number_of_buckets(state);

  for (size_t i = 0; i < number_of_buckets; i++) {
    page_t *entry = &state->entries[i];
    if (!entry->address)
      continue;

    ensure(pages_write(state->db, entry));
  }

  return success();
}
// end::txn_write_state_to_disk[]

// tag::free_tx[]
void txn_free_single_tx(txn_state_t *state) {
  size_t number_of_buckets = get_number_of_buckets(state);
  for (size_t i = 0; i < number_of_buckets; i++) {
    page_t *entry = &state->entries[i];
    if (!entry->address)
      continue;

    free(entry->address);
    entry->address = 0;
  }
  free(state);
}

static void txn_free_transactions(txn_state_t *state) {
  if (!state)
    return;
  txn_state_t *default_read_tx = state->db->default_read_tx;
  while (state != default_read_tx) {
    txn_state_t *cur = state;
    state = state->previous_tx;
    txn_free_single_tx(cur);
  }
}
// end::free_tx[]

// tag::txn_merge_unique_pages[]
static result_t txn_merge_unique_pages(txn_state_t *state) {
  // build a table of all the distinct pages that we want to write
  txn_state_t *default_read_tx = state->db->default_read_tx;
  txn_state_t *prev = state->previous_tx;
  while (prev != default_read_tx) {
    size_t number_of_buckets = get_number_of_buckets(prev);
    for (size_t i = 0; i < number_of_buckets; i++) {
      page_t *entry = &prev->entries[i];
      if (!entry->address)
        continue;
      page_t check = {.page_num = entry->page_num};
      if (lookup_entry_in_tx(state, &check))
        continue;

      ensure(allocate_entry_in_tx(&state, entry));
      entry->address = 0; // we moved ownership, detached from original tx
    }

    prev = prev->previous_tx;
  }

  return success();
}
// end::txn_merge_unique_pages[]

// tag::txn_cleanup_now_or_register[]
static void txn_place_in_free_tx_list(db_state_t *db, txn_state_t *state) {
  if (state == db->default_read_tx)
    return;

  state->can_free_after_tx_id = db->last_tx_id + 1;

  txn_state_t **cur = &db->transactions_to_free;

  while (*cur && (*cur)->tx_id < state->tx_id) {
    cur = &(*cur)->next_tx_in_free_list;
  }
  if (*cur && (*cur)->tx_id == state->tx_id) {
    return; // already registered
  }
  state->next_tx_in_free_list = *cur;
  *cur = state;
}

static void txn_cleanup_now_or_register(db_state_t *db, txn_state_t *state) {
  // update the chain
  if (state->next_tx) {

    db->default_read_tx->next_tx = state->next_tx;
    state->next_tx->previous_tx = db->default_read_tx;

    txn_place_in_free_tx_list(db, state);

  } else {
    db->last_write_tx = db->default_read_tx;
    db->default_read_tx->next_tx = 0;
    // no one is watching, we can clear this
    txn_free_transactions(state);
  }
}
// end::txn_cleanup_now_or_register[]

// tag::txn_free_registered_transactions[]
static void txn_free_registered_transactions(db_state_t *state) {
  txn_state_t *oldest_tx = state->default_read_tx->next_tx;
  while (state->transactions_to_free) {
    txn_state_t *cur = state->transactions_to_free;

    if (oldest_tx && cur->can_free_after_tx_id > oldest_tx->tx_id)
      break;

    state->transactions_to_free = cur->next_tx_in_free_list;

    txn_free_single_tx(cur);
  }
}
// end::txn_free_registered_transactions[]

// tag::txn_gc_tx[]
static result_t txn_gc_tx(txn_state_t *state) {
  // <1>
  txn_state_t *latest_unused = state->db->default_read_tx;
  if (latest_unused->usages)
    // transactions looking at the file directly, cannot proceed
    return success();

  db_state_t *db = state->db;
  // <2>
  while (latest_unused->next_tx && latest_unused->next_tx->usages == 0) {
    latest_unused = latest_unused->next_tx;
  }
  if (latest_unused == db->default_read_tx) {
    txn_place_in_free_tx_list(db, state);
    return success(); // no work to be done
  }

  // <3>
  ensure(txn_merge_unique_pages(latest_unused));

  // <4>
  ensure(txn_write_state_to_disk(latest_unused));

  // a later transaction closed before this one
  // but we need to account for the current close tx
  if (latest_unused->tx_id > state->tx_id)
    latest_unused = state;

  // <6>
  txn_cleanup_now_or_register(db, latest_unused);

  // <5>
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

  if (state == state->db->active_write_tx) {
    state->db->active_write_tx = 0;
  }

  // <2>
  // we have a rollback, no need to keep the memory
  if (!(state->flags & TX_COMMITED)) {
    txn_free_single_tx(state);
    return success();
  }

  // <3>
  state->usages--;
  ensure(txn_gc_tx(state));

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
  ensure(!db->state->active_write_tx,
         msg("There can only be a single write transaction at a time, but "
             "asked for a write transaction when one is already opened"));

  size_t initial_size = sizeof(txn_state_t) + sizeof(page_t) * 8;
  txn_state_t *state = calloc(1, initial_size);
  ensure(state, msg("Unable to allocate memory for transaction state"));

  state->allocated_size = initial_size;
  state->flags = flags;
  state->db = db->state;
  // <4>
  state->previous_tx = db->state->last_write_tx;
  state->tx_id = db->state->last_tx_id + 1;
  tx->state = state;
  db->state->active_write_tx = state;
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
    cur = cur->previous_tx;
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
         msg("Read transactions cannot modify the pages"),
         with(tx->state->flags, "%d"));

  // <2>
  if (lookup_entry_in_tx(tx->state, page)) {
    return success();
  }

  // <3>
  page_t original = {.page_num = page->page_num};
  txn_state_t *cur = tx->state->previous_tx;
  while (cur) {
    if (lookup_entry_in_tx(cur, &original)) {
      break;
    }
    cur = cur->previous_tx;
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
