#include <errno.h>
#include <string.h>

#include "db.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::lookup_entry_in_tx[]
#define get_number_of_buckets(state) \
  ((state->allocated_size - sizeof(txn_state_t)) / sizeof(page_t))

static bool lookup_entry_in_tx(txn_state_t *state, page_t *page) {
  uint64_t page_num = page->page_num;
  size_t number_of_buckets = get_number_of_buckets(state);
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
  txn_state_t *state = tx->state;
  size_t number_of_buckets = get_number_of_buckets(state);

  for (size_t i = 0; i < number_of_buckets; i++) {
    page_t *entry = &state->entries[i];
    if (!entry->address) continue;

    ensure(pages_write(state->db, entry));

    free(entry->address);
    entry->address = 0;
  }
  return success();
}
// end::txn_commit[]

// tag::txn_close[]
result_t txn_close(txn_t *tx) {
  if (!tx->state) return success();  // probably double close?
  txn_state_t *state = tx->state;
  size_t number_of_buckets = get_number_of_buckets(state);
  for (size_t i = 0; i < number_of_buckets; i++) {
    page_t *entry = &state->entries[i];
    if (!entry->address) continue;

    free(entry->address);
    entry->address = 0;
  }

  free(tx->state);

  tx->state = 0;
  return success();
}
// end::txn_close[]

// tag::txn_create[]
result_t txn_create(db_t *db, uint32_t flags, txn_t *tx) {
  errors_assert_empty();

  size_t initial_size = sizeof(txn_state_t) + sizeof(page_t) * 8;
  txn_state_t *state = calloc(1, initial_size);
  ensure(state,
         msg("Unable to allocate memory for transaction state"));

  state->allocated_size = initial_size;
  state->flags = flags;
  state->db = db->state;

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
static enum hash_resize_status expand_hash_table(
    txn_state_t **state_ptr, size_t number_of_buckets) {
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
    if (!entry->address) continue;
    size_t starting_pos = entry->page_num % new_number_of_buckets;
    bool located = false;
    for (size_t j = 0; j < new_number_of_buckets; j++) {
      size_t index = (j + starting_pos) % new_number_of_buckets;
      if (!new_state->entries[index].address) {  // empty
        new_state->entries[index] = state->entries[i];
        located = true;
        break;
      }
    }
    if (!located) {
      errors_push(
          EINVAL,
          msg("Failed to find spot for page after hash table resize"),
          with(entry->page_num, "%lu"));
      free(new_state);
      return hash_resize_err_failure;
    }
  }

  *state_ptr = new_state;  // update caller's reference
  free(state);
  return hash_resize_success;
}
// end::expand_hash_table[]

// tag::allocate_entry_in_tx[]
static result_t allocate_entry_in_tx(txn_state_t **state_ptr,
                                     page_t *page) {
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

// tag::txn_modify_page[]
result_t txn_modify_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  if (lookup_entry_in_tx(tx->state, page)) {
    return success();
  }

  page_t original = {.page_num = page->page_num};
  ensure(pages_get(tx->state->db, &original));

  ensure(palmem_allocate_pages(1, &page->address),
         msg("Unable to allocate memory for a COW page"));

  size_t cancel_defer = 0;
  try_defer(free, page->address, cancel_defer);
  memcpy(page->address, original.address, PAGE_SIZE);

  ensure(allocate_entry_in_tx(&tx->state, page),
         msg("Failed to allocate entry"));

  cancel_defer = 1;
  return success();
}
// end::txn_modify_page[]

// tag::txn_get_page[]
result_t txn_get_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  if (lookup_entry_in_tx(tx->state, page)) {
    return success();
  }

  ensure(pages_get(tx->state->db, page));

  return success();
}
// end::txn_get_page[]

// tag::txn_free_page[]
result_t txn_free_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  ensure(txn_modify_page(tx, page));

  memset(page->address, 0, PAGE_SIZE);

  page_t free_space_page = {.page_num =
                                page->page_num / BITS_IN_PAGE};

  ensure(txn_modify_page(tx, &free_space_page));
  clear_bit(free_space_page.address, page->page_num % BITS_IN_PAGE);

  return success();
}
// end::txn_free_page[]