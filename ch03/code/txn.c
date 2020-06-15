#include <errno.h>
#include <string.h>

#include "db.h"
#include "errors.h"
#include "impl.h"
#include "pal.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::transaction_state[]
typedef struct page_hash_entry {
  uint64_t page_num;
  void *address;
} page_hash_entry_t;

struct transaction_state {
  db_state_t *db;
  size_t allocated_size;
  uint32_t flags;
  uint32_t modified_pages;
  page_hash_entry_t entries[];
};
// end::transaction_state[]

result_t txn_commit(txn_t *tx) {
  errors_assert_empty();
  txn_state_t *state = tx->state;
  size_t number_of_buckets = get_number_of_buckets(state);

  for (size_t i = 0; i < number_of_buckets; i++) {
    page_hash_entry_t *entry = &state->entries[i];
    if (!entry->address) continue;

    ensure(palfs_write_file(state->db->handle,
                            entry->page_num * PAGE_SIZE,
                            entry->address, PAGE_SIZE),
           msg("Unable to write page"), with(entry->page_num, "%lu"));

    free(entry->address);
    entry->address = 0;
  }
  return true;
}

result_t txn_close(txn_t *tx) {
  if (!tx->state) success();  // probably double close?
  txn_state_t *state = tx->state;
  size_t number_of_buckets = get_number_of_buckets(state);
  for (size_t i = 0; i < number_of_buckets; i++) {
    page_hash_entry_t *entry = &state->entries[i];
    if (!entry->address) continue;

    free(entry->address);
    entry->address = 0;
  }

  free(tx->state);

  tx->state = 0;
  success();
}

// tag::txn_create[]
result_t txn_create(db_t *db, uint32_t flags, txn_t *tx) {
  errors_assert_empty();

  size_t initial_size =
      sizeof(txn_state_t) + sizeof(page_hash_entry_t) * 8;
  txn_state_t *state = calloc(1, initial_size);
  failed(!state,
         msg("Unable to allocate memory for transaction state"));

  memset(state, 0, initial_size);
  state->allocated_size = initial_size;
  state->flags = flags;
  state->db = db->state;

  tx->state = state;
  return true;
}
// end::txn_create[]

// tag::lookup_entry_in_tx[]
#define get_number_of_buckets(state)               \
  ((state->allocated_size - sizeof(txn_state_t)) / \
   sizeof(page_hash_entry_t))

static bool lookup_entry_in_tx(txn_state_t *state, uint64_t page_num,
                               page_hash_entry_t **entry) {
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
      *entry = &state->entries[index];
      return true;
    }
  }
  return false;
}
// end::lookup_entry_in_tx[]

enum hash_resize_status {
  hash_resize_success,
  hash_resize_err_no_mem,
  hash_resize_err_failure,
};

static enum hash_resize_status expand_hash_table(
    txn_state_t **state_ptr, size_t number_of_buckets) {
  size_t new_number_of_buckets = number_of_buckets * 2;
  size_t new_size = sizeof(txn_state_t) + (new_number_of_buckets *
                                           sizeof(page_hash_entry_t));
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
    page_hash_entry_t *entry = &state->entries[i];
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
      push_error(
          EINVAL,
          "Failed to find spot for %lu after hash table resize",
          entry->page_num);
      free(new_state);
      return hash_resize_err_failure;
    }
  }

  *state_ptr = new_state;  // update caller's reference
  free(state);
  return hash_resize_success;
}

// tag::allocate_entry_in_tx[]
static bool allocate_entry_in_tx(txn_state_t **state_ptr,
                                 uint64_t page_num,
                                 page_hash_entry_t **entry) {
  txn_state_t *state = *state_ptr;
  size_t number_of_buckets = get_number_of_buckets(state);
  size_t starting_pos = (size_t)(page_num % number_of_buckets);
  // we use linear probing to find a value in case of collisions
  for (size_t i = 0; i < number_of_buckets; i++) {
    size_t index = (i + starting_pos) % number_of_buckets;
    if (state->entries[index].page_num == page_num &&
        state->entries[index].address) {
      fault(EINVAL,
            "Attempted to allocate entry for page_header %lu "
            "which already exist "
            "in the table",
            page_num);
    }

    if (!state->entries[index].address) {
      size_t max_pages = (number_of_buckets * 3 / 4);
      // check the load factor
      if (state->modified_pages + 1 < max_pages) {
        state->modified_pages++;
        state->entries[index].page_num = page_num;
        *entry = &state->entries[index];
        return true;
      }
      switch (expand_hash_table(state_ptr, number_of_buckets)) {
        case hash_resize_success:
          // try again, now we'll have enough room
          return allocate_entry_in_tx(state_ptr, page_num, entry);
        case hash_resize_err_no_mem:
          // we'll accept it here and just have higher
          // load factor
          break;
        case hash_resize_err_failure:
          push_error(
              EINVAL,
              "Failed to add page_header %lu to the transaction "
              "hash table",
              page_num);
          return false;
      }
    }
  }

  switch (expand_hash_table(state_ptr, number_of_buckets)) {
    case hash_resize_success:
      // try again, now we'll have enough room
      return allocate_entry_in_tx(state_ptr, page_num, entry);
    case hash_resize_err_no_mem:
      // we are at 100% capacity, can't recover, will error now
      fault(ENOMEM,
            "Can't allocate to add page_header %lu to the "
            "transaction hash table",
            page_num);
    case hash_resize_err_failure:
      fault(EINVAL,
            "Failed to add page_header %lu to the transaction "
            "hash table",
            page_num);
  }
}
// end::allocate_entry_in_tx[]

// tag::txn_modify_page[]
result_t txn_modify_page(txn_t *tx, page_t *page) {
  errors_assert_empty();

  page_hash_entry_t *entry;
  if (lookup_entry_in_tx(tx->state, page->page_num, &entry)) {
    page->address = entry->address;
    return true;
  }

  uint64_t offset = page->page_num * PAGE_SIZE;
  if (offset + PAGE_SIZE > tx->state->db->mmap.size) {
    failed(ERANGE,
           msg("Requests for a page that is outside of the bounds of "
               "the file"),
           with(page->page_num, "%lu"),
           with(tx->state->db->mmap.size, "%lu"));
  }
  void *original = ((char *)tx->state->db->mmap.address + offset);
  void *modified;
  ensure(palmem_allocate_pages(1, &modified),
         msg("Unable to allocate memory for a COW page"));
  size_t cancel_defer = 0;
  try_defer(free, modified, cancel_defer);
  memcpy(modified, original, PAGE_SIZE);

  ensure(allocate_entry_in_tx(&tx->state, page->page_num, &entry),
         msg("Failed to allocate entry"));

  entry->address = modified;
  page->address = modified;
  cancel_defer = 1;
  return true;
}
// end::txn_modify_page[]