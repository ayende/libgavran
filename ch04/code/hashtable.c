#include <gavran/internal.h>
#include <string.h>

// tag::hash_expand_table[]
static result_t hash_expand_table(pages_hash_table_t **state_ptr) {
  pages_hash_table_t *state = *state_ptr;
  size_t new_number_of_entries = state->number_of_buckets * 2;
  size_t new_size = sizeof(pages_hash_table_t) +
                    (new_number_of_entries * sizeof(page_t));
  pages_hash_table_t *new_state;
  ensure(mem_calloc((void *)&new_state, new_size));
  size_t done = 0;
  try_defer(free, new_state, done);
  memcpy(new_state, state, sizeof(pages_hash_table_t));
  new_state->number_of_buckets = new_number_of_entries;
  size_t iter_state = 0;
  page_t *p;
  while (hash_get_next(state, &iter_state, &p)) {
    ensure(hash_put_new(&new_state, p));
  }
  *state_ptr = new_state;  // update caller's reference
  free(state);
  done = 1;
  return success();
}
// end::hash_expand_table[]

// tag::hash_put_new[]
result_t hash_put_new(pages_hash_table_t **table_p, page_t *page) {
  if ((*table_p)->resize_required) {
    ensure(hash_expand_table(table_p));
  }
  pages_hash_table_t *state = *table_p;
  uint64_t page_num = page->page_num;
  size_t starting_pos = (size_t)(page_num % state->number_of_buckets);
  for (size_t i = 0; i < state->number_of_buckets; i++) {
    size_t index = (i + starting_pos) % state->number_of_buckets;
    if (state->entries[index].page_num == page_num &&
        state->entries[index].address) {
      failed(EINVAL, msg("Page already exists in table"),
             with(page_num, "%lu"));
    }

    if (!state->entries[index].address) {
      state->entries[index].page_num = page_num;
      memcpy(&state->entries[index], page, sizeof(page_t));
      state->count++;
      size_t load_factor = (state->number_of_buckets * 3 / 4);
      state->resize_required = (state->count > load_factor);
      return success();
    }
  }
  failed(ENOSPC, msg("No room for entry, should not happen"));
}
// end::hash_put_new[]

// tag::hash_get_next[]
bool hash_get_next(pages_hash_table_t *table, size_t *state,
                   page_t **page) {
  if (!table) return false;
  for (; *state < table->number_of_buckets; (*state)++) {
    if (!table->entries[*state].address) continue;
    *page = &table->entries[*state];
    (*state)++;
    return true;
  }
  *page = 0;
  return false;
}
// end::hash_get_next[]

// tag::hash_new_and_lookup[]
result_t hash_new(size_t initial_number_of_elements,
                  pages_hash_table_t **table) {
  size_t initial_size = sizeof(pages_hash_table_t) +
                        initial_number_of_elements * sizeof(page_t);
  ensure(mem_calloc((void *)table, initial_size));
  (*table)->number_of_buckets = initial_number_of_elements;
  return success();
}

bool hash_lookup(pages_hash_table_t *table, page_t *page) {
  if (!table) return false;
  uint64_t page_num = page->page_num;
  if (!table->number_of_buckets) return false;
  size_t starting_pos = (size_t)(page_num % table->number_of_buckets);
  for (size_t i = 0; i < table->number_of_buckets; i++) {
    size_t index = (i + starting_pos) % table->number_of_buckets;
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
// end::hash_new_and_lookup[]
