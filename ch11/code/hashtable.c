#include <string.h>

#include "impl.h"

#define get_number_of_buckets(state)                                           \
  ((state->allocated_size - sizeof(pages_hash_table_t)) / sizeof(page_t))

enum hash_resize_status {
  hash_resize_success,
  hash_resize_err_no_mem,
  hash_resize_err_failure,
};

static enum hash_resize_status hash_expand_table(pages_hash_table_t **state_ptr,
                                                 size_t number_of_buckets) {
  size_t new_number_of_buckets = number_of_buckets * 2;
  size_t new_size =
      sizeof(pages_hash_table_t) + (new_number_of_buckets * sizeof(page_t));
  pages_hash_table_t *state = *state_ptr;
  pages_hash_table_t *new_state = calloc(1, new_size);
  if (!new_state) {
    // we are OOM, but we'll accept that and let the hash
    // table fill to higher capacity, caller may decide to
    // error
    return hash_resize_err_no_mem;
  }
  memcpy(new_state, state, sizeof(pages_hash_table_t));
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

result_t hash_put_new(pages_hash_table_t **table_p, page_t *page) {
  uint64_t page_num = page->page_num;
  pages_hash_table_t *state = *table_p;
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

  switch (hash_expand_table(table_p, number_of_buckets)) {
  case hash_resize_success:
    if (placed_successfully) {
      return success();
    }
    // try again after expansion
    return hash_put_new(table_p, page);
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

bool hash_lookup(pages_hash_table_t *table, page_t *page) {
  if (!table)
    return false;
  uint64_t page_num = page->page_num;
  size_t number_of_buckets = get_number_of_buckets(table);
  if (!number_of_buckets)
    return false;
  size_t starting_pos = (size_t)(page_num % number_of_buckets);
  // we use linear probing to find a value in case of collisions
  for (size_t i = 0; i < number_of_buckets; i++) {
    size_t index = (i + starting_pos) % number_of_buckets;
    if (!table->entries[index].address) {
      // empty value, so there is no match
      return false;
    }
    if (table->entries[index].page_num == page_num) {
      memcpy(page, &table->entries[index], sizeof(page_t));
      return true;
    }
  }
  return false;
}

bool hash_get_next(pages_hash_table_t *table, size_t *state, page_t **page) {
  if (!table)
    return false;
  size_t number_of_buckets = get_number_of_buckets(table);
  for (size_t i = *state; i < number_of_buckets; i++) {
    if (!table->entries[i].address)
      continue;
    *page = &table->entries[i];
    *state = i + 1;
    return true;
  }
  *state = number_of_buckets;
  return false;
}

result_t hash_try_add(pages_hash_table_t **table_p, uint64_t page_num) {
  page_t cur = {.page_num = page_num, .address = (void *)-1};
  if (hash_lookup(*table_p, &cur))
    return success();
  return hash_put_new(table_p, &cur);
}

result_t hash_new(size_t initial_number_of_elements,
                  pages_hash_table_t **table) {
  size_t initial_size =
      sizeof(pages_hash_table_t) + initial_number_of_elements * 8;
  pages_hash_table_t *t = calloc(1, initial_size);
  if (!t) {
    failed(ENOMEM, msg("Unable to allocate hash table"));
  }
  t->allocated_size = initial_size;
  *table = t;
  return success();
}
