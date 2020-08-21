#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

result_t btree_create(txn_t* tx, uint64_t* tree_id) {
  page_t p = {.number_of_pages = 1};
  page_metadata_t* metadata;
  ensure(txn_allocate_page(tx, &p, &metadata, 0));
  metadata->tree.page_flags = page_flags_tree_leaf;
  metadata->tree.floor      = 0;
  metadata->tree.ceiling    = PAGE_SIZE;
  metadata->tree.free_space = PAGE_SIZE;

  *tree_id = p.page_num;
  return success();
}

// tag::btree_search_pos_in_page[]
static void btree_search_pos_in_page(
    page_t* p, page_metadata_t* metadata, btree_val_t* kvp) {
  int16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
  int16_t high = max_pos - 1, low = 0;
  uint16_t* positions = p->address;
  kvp->position       = 0;  // to handle empty pages (after split)
  while (low <= high) {
    kvp->position = (low + high) >> 1;
    uint64_t ks;
    uint8_t* cur =
        varint_decode(p->address + positions[kvp->position], &ks);
    int match = memcmp(kvp->key.address, cur, MIN(kvp->key.size, ks));
    if (match == 0) {
      kvp->last_match = 0;
      return;  // found it
    }

    if (match > 0) {
      low             = kvp->position + 1;
      kvp->last_match = 1;
    } else {
      high            = kvp->position - 1;
      kvp->last_match = -1;
    }
  }
  // adjust position to where we _should_ be
  if (kvp->last_match > 0) {
    kvp->position++;
  } else {
    kvp->position = MAX(0, kvp->position - 1);
  }
  kvp->position = ~kvp->position;
}
// end::btree_search_pos_in_page[]

// tag::btree_write_in_page[]
static void btree_write_in_page(page_t* p, page_metadata_t* metadata,
    uint16_t req_size, btree_val_t* set, btree_val_t* old) {
  uint16_t* positions = p->address;
  if (set->position >= 0) {
    uint64_t key_size, v;
    uint8_t* start   = p->address + positions[set->position];
    uint8_t* key_end = varint_decode(start, &key_size) + key_size;
    uint8_t* end     = varint_decode(key_end, &v);
    if (old) {
      old->has_val = true;
      old->val     = v;
    }
    if (req_size <= (end - start)) {  // can fit old location
      uint8_t* val_end = varint_encode(set->val, key_end);
      memset(val_end, 0, (size_t)(end - val_end));
      metadata->tree.free_space += (uint16_t)(end - val_end);
      return;
    }
    memset(start, 0, (size_t)(end - start));  // reset value
  } else {
    if (old) old->has_val = false;
    metadata->tree.number_of_entries++;
    set->position  = ~set->position;
    size_t max_pos = metadata->tree.floor / sizeof(uint16_t);
    metadata->tree.floor += sizeof(uint16_t);
    metadata->tree.free_space -= sizeof(uint16_t);
    // create a gap for the new value in the right place
    memmove(positions + set->position + 1, positions + set->position,
        ((max_pos - (uint16_t)set->position) * sizeof(uint16_t)));
  }
  metadata->tree.ceiling -= req_size;
  uint8_t* key_start = varint_encode(
      set->key.size, p->address + metadata->tree.ceiling);
  memcpy(key_start, set->key.address, set->key.size);
  varint_encode(set->val, key_start + set->key.size);
  positions[set->position] = metadata->tree.ceiling;
  metadata->tree.free_space -= req_size;
}
// end::btree_write_in_page[]

static result_t btree_defrag(page_t* p, page_metadata_t* metadata) {
  uint8_t* tmp;
  ensure(mem_alloc_page_aligned((void*)&tmp, PAGE_SIZE));
  defer(free, tmp);
  memcpy(tmp, p->address, PAGE_SIZE);
  memset(p->address + metadata->tree.floor, 0,
      PAGE_SIZE - metadata->tree.floor);
  metadata->tree.ceiling = PAGE_SIZE;
  int16_t* positions     = p->address;
  size_t max_pos         = metadata->tree.floor / sizeof(int16_t);
  for (size_t i = 0; i < max_pos; i++) {
    uint64_t size;
    int16_t cur_pos = positions[i];
    uint8_t* end    = varint_decode(
        varint_decode(tmp + cur_pos, &size) + size, &size);
    positions[i] = (int16_t)(end - (tmp + cur_pos));
    metadata->tree.ceiling -= positions[i];
    memcpy(p->address + metadata->tree.ceiling, tmp + cur_pos,
        (size_t)(end - (tmp + cur_pos)));
  }
  return success();
}

static result_t btree_set_in_page(
    txn_t* tx, uint64_t page_num, btree_val_t* set, btree_val_t* old);

static result_t btree_create_root_page(txn_t* tx, page_t* p,
    page_metadata_t* metadata, btree_val_t* set) {
  page_t root = {.number_of_pages = 1};
  page_metadata_t* root_metadata;
  ensure(txn_allocate_page(tx, &root, &root_metadata, p->page_num));
  root_metadata->tree.page_flags = page_flags_tree_branch;
  root_metadata->tree.ceiling    = PAGE_SIZE;
  root_metadata->tree.free_space = PAGE_SIZE;

  set->tree_id_changed = true;
  set->tree_id         = root.page_num;
  uint64_t key_size;
  uint8_t* key_start =
      varint_decode(p->address + *(int16_t*)p->address, &key_size);
  btree_val_t ref = {.val = p->page_num,
      .key                = {.address = key_start, .size = key_size},
      .position           = ~0};  // new entry
  size_t req_size = varint_get_length(ref.key.size) + ref.key.size +
                    varint_get_length(ref.val);
  btree_write_in_page(
      &root, root_metadata, (uint16_t)req_size, &ref, 0);
  metadata->tree.parent_page = root.page_num;
  return success();
}

static result_t btree_split_page(txn_t* tx, page_t* p,
    page_metadata_t* metadata, btree_val_t* set) {
  if (metadata->tree.parent_page == 0) {
    ensure(btree_create_root_page(tx, p, metadata, set));
  }
  page_t other = {.number_of_pages = 1};
  page_metadata_t* o_metadata;
  ensure(txn_allocate_page(tx, &other, &o_metadata, p->page_num));
  o_metadata->tree.parent_page = metadata->tree.parent_page;
  o_metadata->tree.page_flags  = metadata->tree.page_flags;
  o_metadata->tree.ceiling     = PAGE_SIZE;
  o_metadata->tree.free_space  = PAGE_SIZE;

  uint16_t max_pos = metadata->tree.floor / sizeof(int16_t);
  bool seq_write_up =
      max_pos == (uint16_t)(~set->position) && set->last_match > 0;
  bool seq_write_down = set->position == 0 && set->last_match < 0;
  btree_val_t ref = {.tree_id = set->tree_id, .val = other.page_num};
  if (seq_write_down || seq_write_up) {  // optimization: no split req
    ref.key = set->key;
  } else {
    int16_t* positions = p->address;
    size_t base        = set->last_match < 0 ? 0 : max_pos / 2;
    for (size_t i = 0; i < max_pos / 2; i++) {
      size_t idx = i + base;
      uint64_t size, val_size;
      uint8_t* key_start =
          varint_decode(p->address + positions[idx], &size);
      uint8_t* end = varint_decode(key_start + size, &val_size);
      if (i == 0) {
        ref.key.address = key_start;
        ref.key.size    = size;
      }
      uint16_t entry_size =
          (uint16_t)(end - (uint8_t*)p->address + positions[idx]);
      o_metadata->tree.ceiling -= entry_size;
      memcpy(p->address + positions[idx],
          other.address + o_metadata->tree.ceiling, entry_size);
      *(int16_t*)(other.address + o_metadata->tree.floor) =
          (int16_t)o_metadata->tree.ceiling;
      o_metadata->tree.floor += sizeof(int16_t);
      memset(p->address + positions[idx], 0, entry_size);
      positions[idx] = 0;
    }
  }
  page_t parent = {.page_num = metadata->tree.parent_page};
  page_metadata_t* parent_metadata;
  ensure(txn_get_page_and_metadata(tx, &parent, &parent_metadata));
  btree_search_pos_in_page(&parent, parent_metadata, &ref);
  ensure(btree_set_in_page(tx, metadata->tree.parent_page, &ref, 0));
  return success();
}

static result_t btree_set_in_page(txn_t* tx, uint64_t page_num,
    btree_val_t* set, btree_val_t* old) {
  page_t p = {.page_num = page_num};
  page_metadata_t* metadata;
  ensure(txn_modify_page(tx, &p));
  ensure(txn_modify_metadata(tx, page_num, &metadata));
  size_t req_size = varint_get_length(set->key.size) + set->key.size +
                    varint_get_length(set->val);
  if (req_size > metadata->tree.free_space) {
    ensure(btree_split_page(tx, &p, metadata, set));
    return btree_set(tx, set, old);  // now try again
  }
  if (req_size > (metadata->tree.ceiling - metadata->tree.floor)) {
    ensure(btree_defrag(&p, metadata));  // let's see if this helps
    if (req_size > (metadata->tree.ceiling - metadata->tree.floor)) {
      ensure(btree_split_page(tx, &p, metadata, set));
      return btree_set(tx, set, old);  // now try again
    }
  }
  btree_write_in_page(&p, metadata, (uint16_t)req_size, set, old);
  return success();
}

static result_t btree_get_leaf_page_for(txn_t* tx, btree_val_t* kvp,
    page_t* p, page_metadata_t** metadata) {
  p->page_num = kvp->tree_id;
  ensure(txn_get_page_and_metadata(tx, p, metadata));
  while ((*metadata)->tree.page_flags == page_flags_tree_branch) {
    btree_search_pos_in_page(p, *metadata, kvp);
    if (kvp->position < 0) kvp->position = ~kvp->position;
    int16_t max_pos    = (*metadata)->tree.floor / sizeof(uint16_t);
    int16_t pos        = MIN(max_pos - 1, kvp->position);
    int16_t* positions = p->address;
    size_t key_size;
    uint8_t* start     = p->address + positions[pos];
    uint8_t* val_start = varint_decode(start, &key_size) + key_size;
    varint_decode(val_start, &p->page_num);
    ensure(txn_get_page_and_metadata(tx, p, metadata));
  }
  assert((*metadata)->tree.page_flags == page_flags_tree_leaf);
  btree_search_pos_in_page(p, *metadata, kvp);
  return success();
}

result_t btree_set(txn_t* tx, btree_val_t* set, btree_val_t* old) {
  page_t p;
  page_metadata_t* metadata;
  ensure(btree_get_leaf_page_for(tx, set, &p, &metadata));
  ensure(btree_set_in_page(tx, p.page_num, set, old));
  return success();
}

result_t btree_get(txn_t* tx, btree_val_t* kvp) {
  page_t p;
  page_metadata_t* metadata;
  ensure(btree_get_leaf_page_for(tx, kvp, &p, &metadata));
  if (kvp->last_match != 0) {
    kvp->has_val = false;
    return success();
  }
  size_t key_size;
  uint8_t* start = p.address + ((int16_t*)p.address)[kvp->position];
  varint_decode(
      varint_decode(start, &key_size) + key_size, &kvp->val);
  kvp->has_val = true;
  return success();
}