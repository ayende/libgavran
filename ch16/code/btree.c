#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

result_t btree_create(txn_t* tx, uint64_t* tree_id) {
  page_t p = {.number_of_pages = 1};
  page_metadata_t* metadata;
  ensure(txn_allocate_page(tx, &p, &metadata, 0));
  metadata->tree.page_flags = page_flags_tree_leaf;
  metadata->tree.floor      = sizeof(uint16_t);
  metadata->tree.ceiling    = PAGE_SIZE;

  *tree_id = p.page_num;
  return success();
}

// tag::btree_search_pos_in_page[]
ssize_t btree_search_pos_in_page(
    page_t* p, page_metadata_t* metadata, span_t* key) {
  uint16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
  size_t high = max_pos - 1, low = 0;
  uint16_t* positions = p->address;
  int last_match      = 0;
  size_t last_pos     = 0;
  while (low <= high) {
    last_pos = (low + high) >> 1;
    uint64_t ks;
    char* cur  = varint_decode(p->address + positions[last_pos], &ks);
    last_match = memcmp(key->address, cur, MIN(key->size, ks));
    if (last_match == 0) return last_pos;
    if (last_match < 0) {
      low = last_pos + 1;
    } else {
      high = last_pos - 1;
    }
  }
  // if last pos is smaller than key, shift it to where it should go
  if (last_match < 0) last_pos++;
  return ~last_pos;  // not an exact match
}
// end::btree_search_pos_in_page[]

// tag::btree_set_in_page[]
void btree_set_in_page(txn_t* tx, page_t* p,
    page_metadata_t* metadata, btree_val_t* set, btree_val_t* old) {
  ssize_t pos         = btree_search_pos_in_page(p, metadata, set);
  uint16_t* positions = p->address;
  size_t total_size   = varint_get_length(set->key.size) +
                      set->key.size + varint_get_length(set->val);
  if (pos >= 0) {
    uint64_t key_size, v;
    char* start   = p->address + positions[pos];
    char* key_end = varint_decode(start, &key_size) + key_size;
    char* end     = varint_decode(key_end, &v);
    if (old) {
      old->has_val = true;
      old->val     = v;
    }
    if (total_size <= (end - start)) {  // can fit old location
      char* val_end = varint_encode(set->val, key_end);
      memset(val_end, 0, end - val_end);
      metadata->tree.free_space += (uint16_t)(end - val_end);
      return;
    }
    memset(start, 0, end - start);  // reset value
  } else {
    if (old) old->has_val = false;
    pos            = ~pos;
    size_t max_pos = metadata->tree.floor / sizeof(uint16_t);
    metadata->tree.floor += sizeof(uint16_t);
    metadata->tree.free_space -= sizeof(uint16_t);
    // create a gap for the new value in the right place
    memmove(positions + pos + 1, positions + pos,
        (max_pos - pos) * sizeof(uint16_t));
  }
  metadata->tree.ceiling += total_size;
  char* key_start =
      varint_encode(set->key.size, metadata->tree.ceiling);
  memcpy(key_start, set->key.address, set->key.size);
  varint_encode(set->val, key_start + set->key.size);
  positions[pos] = metadata->tree.ceiling;
  metadata->tree.free_space -= total_size;
}
// end::btree_set_in_page[]