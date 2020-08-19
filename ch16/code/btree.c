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

ssize_t btree_search_pos_in_page(
    page_t* p, page_metadata_t* metadata, btree_val_t* kvp) {
  uint16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
  size_t high = max_pos - 1, low = 0;
  uint16_t* positions = p->address;
  int last_match      = 0;
  size_t last_pos     = 0;
  while (low <= high) {
    last_pos = (low + high) / 2;
    uint64_t key_size;
    char* key =
        varint_decode(p->address + positions[last_pos], &key_size);
    last_match =
        memcmp(kvp->key.address, key, MIN(kvp->key.size, key_size));
    if (last_match == 0) break;
    if (last_match < 0) {
      low = last_pos + 1;
    } else {
      high = last_pos - 1;
    }
  }
  if (last_match < 0) {
    last_pos++;  // last pos is smaller than key, shift it
  }
  if (last_match) {
    last_pos = ~last_pos;  // not an exact match
  }
  return last_pos;
}

void btree_set_in_page(txn_t* tx, page_t* p,
    page_metadata_t* metadata, btree_val_t* set, btree_val_t* old) {
  ssize_t pos         = btree_search_pos_in_page(p, metadata, set);
  uint16_t* positions = p->address;
  size_t total_size   = varint_get_length(set->key.size) +
                      set->key.size + varint_get_length(set->val);
  if (pos >= 0) {
    uint64_t ks, v;
    char* start   = p->address + positions[pos];
    char* key_end = varint_decode(start, &ks) + ks;
    char* end     = varint_decode(key_end, &v);
    if (old) {
      old->has_val = true;
      old->val     = v;
    } else {
      old->has_val = false;
    }

    if (total_size <= (end - start)) {  // can fit old location
      char* val_end = varint_encode(set->val, key_end);
      memset(val_end, 0, end - val_end);
      metadata->tree.free_space += (uint16_t)(end - val_end);
      return;
    }
    memset(start, 0, end - start);  // reset value
  } else {
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