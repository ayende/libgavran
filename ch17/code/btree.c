#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

// tag::btree_validate_key[]
static result_t btree_validate_key(span_t* key) {
  ensure(key->size > 0);
  ensure(key->size <= 512);
  ensure(key->address, msg("Key cannot have a NULL address"));
  return success();
}
// end::btree_validate_key[]

// tag::btree_create[]
static void btree_init_metadata(
    page_metadata_t* m, page_flags_t page_flags) {
  m->tree.page_flags = page_flags;
  m->tree.floor      = 0;
  m->tree.ceiling    = PAGE_SIZE;
  m->tree.free_space = PAGE_SIZE;
}
result_t btree_create(txn_t* tx, uint64_t* tree_id) {
  page_t p = {.number_of_pages = 1};
  page_metadata_t* metadata;
  ensure(txn_allocate_page(tx, &p, &metadata, 0));
  btree_init_metadata(metadata, page_flags_tree_leaf);
  *tree_id = p.page_num;
  return success();
}
// end::btree_create[]

// tag::btree_search_pos_in_page[]
static void btree_search_pos_in_page(
    page_t* p, page_metadata_t* metadata, btree_val_t* kvp) {
  assert(kvp->key.size && kvp->key.address);
  int16_t max_pos =
      (int16_t)(metadata->tree.floor / sizeof(uint16_t));
  int16_t high = max_pos - 1, low = 0;
  uint16_t* positions = p->address;
  kvp->position       = 0;  // to handle empty pages (after split)
  kvp->last_match     = 0;
  while (low <= high) {
    kvp->position = (low + high) >> 1;
    uint64_t ks;
    uint8_t* cur =
        varint_decode(p->address + positions[kvp->position], &ks);
    int match;
    if (!ks) {  // the leftmost key can be empty, smaller than all
      assert(kvp->position == 0 &&
             metadata->tree.page_flags == page_flags_tree_branch);
      match = 1;
    } else {
      match = memcmp(kvp->key.address, cur, MIN(kvp->key.size, ks));
    }
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
  if (kvp->last_match > 0) {
    kvp->position++;  // adjust position to where we _should_ be
  }
  kvp->position = ~kvp->position;
}
// end::btree_search_pos_in_page[]

// tag::btree_insert_to_page[]
static void* btree_insert_to_page(page_t* p,
    page_metadata_t* metadata, int16_t pos, uint16_t req_size) {
  uint16_t* positions = p->address;
  size_t max_pos      = metadata->tree.floor / sizeof(uint16_t);
  metadata->tree.floor += sizeof(uint16_t);
  metadata->tree.free_space -= sizeof(uint16_t);
  if (pos < 0) {  // need to allocate space in positions
    pos = ~pos;
    memmove(positions + pos + 1, positions + pos,
        ((max_pos - (size_t)pos) * sizeof(uint16_t)));
  }
  metadata->tree.ceiling -= req_size;
  metadata->tree.free_space -= req_size;
  positions[pos] = metadata->tree.ceiling;
  return p->address + metadata->tree.ceiling;
}
// end::btree_insert_to_page[]

// tag::btree_defrag[]
static result_t btree_defrag(page_t* p, page_metadata_t* metadata) {
  uint8_t* tmp;
  ensure(mem_alloc_page_aligned((void*)&tmp, PAGE_SIZE));
  defer(free, tmp);
  memcpy(tmp, p->address, PAGE_SIZE);
  memset(p->address + metadata->tree.floor, 0,
      PAGE_SIZE - metadata->tree.floor);
  metadata->tree.ceiling = PAGE_SIZE;
  uint16_t* positions    = p->address;
  size_t max_pos         = metadata->tree.floor / sizeof(uint16_t);
  for (size_t i = 0; i < max_pos; i++) {
    uint64_t size;
    uint16_t cur_pos = positions[i];
    uint8_t* end     = varint_decode(
        varint_decode(tmp + cur_pos, &size) + size, &size);
    if (metadata->tree.page_flags == page_flags_tree_leaf) {
      end++;  // flags
    }
    uint16_t entry_size = (uint16_t)(end - (tmp + cur_pos));
    metadata->tree.ceiling -= entry_size;
    positions[i] = metadata->tree.ceiling;
    memcpy(p->address + metadata->tree.ceiling, tmp + cur_pos,
        entry_size);
  }
  return success();
}
// end::btree_defrag[]

static result_t btree_set_in_page(
    txn_t* tx, uint64_t page_num, btree_val_t* set, btree_val_t* old);

static void* btree_insert_to_page(page_t* p,
    page_metadata_t* metadata, int16_t pos, uint16_t req_size);

// tag::btree_create_root_page[]
static result_t btree_create_root_page(
    txn_t* tx, page_t* p, page_metadata_t** metadata) {
  page_t new = {.number_of_pages = 1};
  page_metadata_t* new_metadata;
  ensure(txn_allocate_page(tx, &new, &new_metadata, p->page_num));
  memcpy(new.address, p->address, PAGE_SIZE);
  memcpy(new_metadata, *metadata, sizeof(page_metadata_t));

  memset(p->address, 0, PAGE_SIZE);
  btree_init_metadata(*metadata, page_flags_tree_branch);

  size_t req_size =
      varint_get_length(0) + 0 + varint_get_length(new.page_num);
  uint8_t* val_p =
      btree_insert_to_page(p, *metadata, 0, (uint16_t)req_size);
  varint_encode(new.page_num, varint_encode(0, val_p));
  ensure(btree_stack_push(&tx->state->tmp.stack, p->page_num, 0));

  memcpy(p, &new, sizeof(page_t));
  *metadata = new_metadata;

  return success();
}
// end::btree_create_root_page[]

// tag::btree_get_entry_at[]
static void btree_get_entry_at(page_t* p, page_metadata_t* metadata,
    uint16_t pos, span_t* key, uint64_t* val, span_t* entry,
    uint8_t* flags) {
  uint16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
  assert(pos < max_pos);
  uint16_t* positions = p->address;
  entry->address      = p->address + positions[pos];
  key->address        = varint_decode(entry->address, &key->size);
  uint8_t* end        = varint_decode(key->address + key->size, val);
  if (metadata->tree.page_flags == page_flags_tree_leaf) {
    *flags = *end++;
  }
  entry->size = (size_t)(end - (uint8_t*)entry->address);
}
static uint64_t btree_get_val_at(
    page_t* p, page_metadata_t* metadata, uint16_t pos) {
  span_t key, entry;
  uint64_t val;
  uint8_t flags;
  btree_get_entry_at(p, metadata, pos, &key, &val, &entry, &flags);
  return val;
}
// end::btree_get_entry_at[]

// tag::btree_get_leftmost_key[]
static result_t btree_get_leftmost_key(txn_t* tx, page_t* p,
    page_metadata_t* metadata, span_t* leftmost_key) {
  while (metadata->tree.page_flags == page_flags_tree_branch) {
    p->page_num = btree_get_val_at(p, metadata, 0);
    ensure(txn_get_page_and_metadata(tx, p, &metadata));
  }
  span_t entry;
  uint64_t val;
  uint8_t flags;
  btree_get_entry_at(
      p, metadata, 0, leftmost_key, &val, &entry, &flags);
  return success();
}
// end::btree_get_leftmost_key[]

// tag::btree_split_page_in_half[]
static result_t btree_split_page_in_half(page_t* p,
    page_metadata_t** metadata, page_t* other,
    page_metadata_t* o_metadata, btree_val_t* ref, btree_val_t* set,
    uint16_t max_pos) {
  uint16_t* positions   = p->address;
  uint16_t* o_positions = other->address;
  uint64_t val;
  uint8_t flags;
  span_t key, entry;
  for (uint16_t idx = max_pos / 2, o_idx = 0; idx < max_pos;
       idx++, o_idx++) {
    btree_get_entry_at(p, *metadata, idx, &key, &val, &entry, &flags);
    o_metadata->tree.ceiling -= entry.size;
    memcpy(other->address + o_metadata->tree.ceiling, entry.address,
        entry.size);
    o_positions[o_idx] = o_metadata->tree.ceiling;
    o_metadata->tree.floor += sizeof(uint16_t);
    o_metadata->tree.free_space -= sizeof(uint16_t) + entry.size;
    memset(entry.address, 0, entry.size);
    (*metadata)->tree.free_space += sizeof(uint16_t) + entry.size;
  }
  size_t removed = (max_pos - (max_pos / 2));
  memset(positions + (max_pos / 2), 0, removed * sizeof(uint16_t));
  (*metadata)->tree.floor -= removed * sizeof(uint16_t);
  btree_get_entry_at(
      other, o_metadata, 0, &ref->key, &val, &entry, &flags);
  if (memcmp(ref->key.address, set->key.address,
          MIN(set->key.size, ref->key.size)) < 0) {
    memcpy(p, other, sizeof(page_t));
    *metadata = o_metadata;
  }
  return success();
}
// end::btree_split_page_in_half[]

// tag::btree_append_to_parent[]
static result_t btree_append_to_parent(
    txn_t* tx, btree_stack_t* stack, btree_val_t* ref) {
  page_t parent = {0};
  page_metadata_t* p_metadata;
  int16_t _pos;
  ensure(btree_stack_pop(stack, &parent.page_num, &_pos));
  ensure(txn_modify_page(tx, &parent));
  ensure(txn_modify_metadata(tx, parent.page_num, &p_metadata));
  btree_search_pos_in_page(&parent, p_metadata, ref);
  ensure(btree_set_in_page(tx, parent.page_num, ref, 0));
  return success();
}
// end::btree_append_to_parent[]

// tag::btree_split_page[]
static result_t btree_split_page(txn_t* tx, page_t* p,
    page_metadata_t** metadata, btree_val_t* set) {
  btree_stack_t* stack = &tx->state->tmp.stack;
  if (stack->index == 0) {  // at root
    ensure(btree_create_root_page(tx, p, metadata));
  }
  page_t other = {.number_of_pages = 1};
  page_metadata_t* o_metadata;
  ensure(txn_allocate_page(tx, &other, &o_metadata, p->page_num));
  btree_init_metadata(o_metadata, (*metadata)->tree.page_flags);
  uint16_t max_pos = (*metadata)->tree.floor / sizeof(uint16_t);
  bool seq_write_up =
      max_pos == (uint16_t)(~set->position) && set->last_match > 0;
  bool seq_write_down = (~set->position == 0) && set->last_match < 0;
  btree_val_t ref = {.tree_id = set->tree_id, .val = other.page_num};
  if (seq_write_up) {  // optimization: no split req
    ref.key = set->key;
    memcpy(p, &other, sizeof(page_t));
    *metadata = o_metadata;
  } else if (seq_write_down) {
    memcpy(other.address, p->address, PAGE_SIZE);
    memset(p->address, 0, PAGE_SIZE);
    memcpy(o_metadata, (*metadata), sizeof(page_metadata_t));
    btree_init_metadata((*metadata), o_metadata->tree.page_flags);
    ensure(btree_get_leftmost_key(tx, &other, o_metadata, &ref.key));
  } else {
    ensure(btree_split_page_in_half(
        p, metadata, &other, o_metadata, &ref, set, max_pos));
  }
  ensure(btree_append_to_parent(tx, stack, &ref));
  return success();
}
// end::btree_split_page[]

// tag::btree_append_to_page[]
static result_t btree_append_to_page(txn_t* tx, page_t* p,
    page_metadata_t* metadata, size_t req_size, btree_val_t* set) {
  if (req_size + sizeof(uint16_t) >  // not enough space?
      (metadata->tree.ceiling - metadata->tree.floor)) {
    if (req_size + sizeof(uint16_t) < metadata->tree.free_space) {
      ensure(btree_defrag(p, metadata));  // let's see if this helps
    }
    if (req_size + sizeof(uint16_t) >  // check again, defrag helped?
        (metadata->tree.ceiling - metadata->tree.floor)) {
      if (set->position >= 0) {  // remove existing entry in page
        uint16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
        metadata->tree.floor -= sizeof(uint16_t);
        uint16_t pos        = (uint16_t)(~set->position);
        uint16_t* positions = p->address;
        memmove(positions + pos, positions + pos + 1,
            ((max_pos - pos - 1) * sizeof(uint16_t)));
        positions[max_pos - 1] = 0;
      }
      ensure(btree_split_page(tx, p, &metadata, set));
      btree_search_pos_in_page(p, metadata, set);  // adjust pos
    }
  }
  void* dst = btree_insert_to_page(
      p, metadata, set->position, (uint16_t)req_size);
  uint8_t* key_start = varint_encode(set->key.size, dst);
  memcpy(key_start, set->key.address, set->key.size);
  uint8_t* end = varint_encode(set->val, key_start + set->key.size);
  if (metadata->tree.page_flags == page_flags_tree_leaf) {
    *end = set->flags;
  }
  return success();
}
// end::btree_append_to_page[]

// tag::btree_try_update_in_place[]
static result_t btree_try_update_in_place(page_t* p,
    page_metadata_t* metadata, size_t req_size, btree_val_t* set,
    btree_val_t* old, bool* updated) {
  span_t key, entry;
  uint8_t flags;
  uint64_t old_val;
  btree_get_entry_at(p, metadata, (uint16_t)set->position, &key,
      &old_val, &entry, &flags);
  if (old) {
    old->has_val = true;
    old->val     = old_val;
    old->flags   = flags;
  }
  if (req_size <= entry.size) {  // can fit old location
    uint8_t* val_end =
        varint_encode(set->val, key.address + key.size);
    if (metadata->tree.page_flags == page_flags_tree_leaf) {
      *val_end++ = set->flags;
    }
    size_t diff =
        (size_t)(((uint8_t*)entry.address + entry.size) - val_end);
    memset(val_end, 0, diff);
    metadata->tree.free_space += (uint16_t)diff;
    *updated = true;
  } else {
    memset(entry.address, 0, entry.size);  // reset value
    metadata->tree.free_space -= (uint16_t)entry.size;
  }
  return success();
}
// end::btree_try_update_in_place[]

// tag::btree_set_in_page[]
static result_t btree_set_in_page(txn_t* tx, uint64_t page_num,
    btree_val_t* set, btree_val_t* old) {
  page_t p = {.page_num = page_num};
  page_metadata_t* metadata;
  ensure(txn_modify_page(tx, &p));
  ensure(txn_modify_metadata(tx, page_num, &metadata));
  size_t req_size = varint_get_length(set->key.size) + set->key.size +
                    varint_get_length(set->val) + 1 /*flags*/;
  if (set->position >= 0) {  // update
    bool updated = false;
    ensure(btree_try_update_in_place(
        &p, metadata, req_size, set, old, &updated));
    if (updated) return success();
    // need to insert this again...
  } else {  // insert
    if (old) old->has_val = false;
  }
  ensure(btree_append_to_page(tx, &p, metadata, req_size, set));
  return success();
}
// end::btree_set_in_page[]

// tag::btree_get_leaf_page_for[]
static result_t btree_get_leaf_page_for(txn_t* tx, btree_val_t* kvp,
    page_t* p, page_metadata_t** metadata) {
  p->page_num = kvp->tree_id;
  ensure(txn_get_page_and_metadata(tx, p, metadata));
  assert((*metadata)->common.page_flags == page_flags_tree_branch ||
         (*metadata)->common.page_flags == page_flags_tree_leaf);
  btree_stack_clear(&tx->state->tmp.stack);
  while ((*metadata)->tree.page_flags == page_flags_tree_branch) {
    btree_search_pos_in_page(p, *metadata, kvp);
    if (kvp->position < 0) kvp->position = ~kvp->position;
    if (kvp->last_match) kvp->position--;  // went too far
    ensure(btree_stack_push(
        &tx->state->tmp.stack, p->page_num, kvp->position));
    uint16_t max_pos = (*metadata)->tree.floor / sizeof(uint16_t);
    uint16_t pos     = MIN(max_pos - 1, (uint16_t)kvp->position);
    p->page_num      = btree_get_val_at(p, *metadata, pos);
    ensure(txn_get_page_and_metadata(tx, p, metadata));
  }
  assert((*metadata)->tree.page_flags == page_flags_tree_leaf);
  btree_search_pos_in_page(p, *metadata, kvp);
  return success();
}
// end::btree_get_leaf_page_for[]

// tag::btree_drop[]
static result_t btree_free_page_recursive(
    txn_t* tx, uint64_t page_num) {
  page_t p = {.page_num = page_num};
  page_metadata_t* metadata;
  ensure(txn_get_page_and_metadata(tx, &p, &metadata));
  if (metadata->tree.page_flags != page_flags_tree_leaf) {
    uint16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
    for (uint16_t i = 0; i < max_pos; i++) {
      uint64_t child = btree_get_val_at(&p, metadata, i);
      ensure(btree_free_page_recursive(tx, child));
    }
  }
  ensure(txn_free_page(tx, &p));
  return success();
}
// tag::btree_drop_only[]
result_t btree_drop(txn_t* tx, uint64_t tree_id) {
  page_metadata_t* metadata;
  ensure(txn_get_metadata(tx, tree_id, &metadata));
  uint64_t nested = metadata->tree.next_nested;
  while (nested) {
    ensure(txn_get_metadata(tx, nested, &metadata));
    uint64_t old_nested = nested;
    nested              = metadata->tree.next_nested;
    ensure(btree_free_page_recursive(tx, old_nested));
  }
  return btree_free_page_recursive(tx, tree_id);
}
// end::btree_drop_only[]
// end::btree_drop[]

// tag::btree_set[]
result_t btree_set(txn_t* tx, btree_val_t* set, btree_val_t* old) {
  assert(btree_validate_key(&set->key));
  page_t p;
  page_metadata_t* metadata;
  ensure(btree_get_leaf_page_for(tx, set, &p, &metadata));
  ensure(btree_set_in_page(tx, p.page_num, set, old));
  return success();
}
// end::btree_set[]

// tag::btree_get[]
result_t btree_get(txn_t* tx, btree_val_t* kvp) {
  assert(btree_validate_key(&kvp->key));
  page_t p;
  page_metadata_t* metadata;
  ensure(btree_get_leaf_page_for(tx, kvp, &p, &metadata));
  if (kvp->last_match != 0) {
    kvp->has_val = false;
    return success();
  }
  span_t key, entry;
  btree_get_entry_at(&p, metadata, (uint16_t)kvp->position, &key,
      &kvp->val, &entry, &kvp->flags);
  kvp->has_val = true;
  return success();
}
// end::btree_get[]

// tag::btree_cursor_at[]
static result_t btree_cursor_at(btree_cursor_t* c, bool start) {
  page_t p = {.page_num = c->tree_id};
  page_metadata_t* metadata;
  btree_stack_t* stack = &c->tx->state->tmp.stack;
  ensure(txn_get_page_and_metadata(c->tx, &p, &metadata));
  // handle cursor reuse for multiple queries
  ensure(btree_free_cursor(c));
  while (metadata->tree.page_flags == page_flags_tree_branch) {
    uint16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
    int16_t pos      = start ? 0 : (int16_t)max_pos - 1;
    ensure(btree_stack_push(stack, p.page_num, pos));
    uint16_t* positions = p.address;
    size_t key_size;
    uint8_t* entry     = p.address + positions[pos];
    uint8_t* val_start = varint_decode(entry, &key_size) + key_size;
    varint_decode(val_start, &p.page_num);
    ensure(txn_get_page_and_metadata(c->tx, &p, &metadata));
  }
  assert(metadata->tree.page_flags == page_flags_tree_leaf);
  int16_t leaf_max_pos = metadata->tree.floor / sizeof(uint16_t);
  ensure(btree_stack_push(&c->tx->state->tmp.stack, p.page_num,
      ~(start ? 0 : leaf_max_pos)));
  c->has_val = metadata->tree.floor > 0;
  memcpy(&c->stack, stack, sizeof(btree_stack_t));
  memset(stack, 0, sizeof(btree_stack_t));
  return success();
}
result_t btree_cursor_at_start(btree_cursor_t* cursor) {
  return btree_cursor_at(cursor, true);
}
result_t btree_cursor_at_end(btree_cursor_t* cursor) {
  return btree_cursor_at(cursor, false);
}
// end::btree_cursor_at[]

// tag::btree_iterate_next_page[]
static result_t btree_iterate_next_page(btree_cursor_t* c, page_t* p,
    page_metadata_t** metadata, int16_t* pos, int16_t step,
    bool* done) {
  while (c->stack.index > 0) {
    ensure(btree_stack_pop(&c->stack, &p->page_num, pos));
    ensure(txn_get_page_and_metadata(c->tx, p, metadata));
    assert((*metadata)->tree.page_flags == page_flags_tree_branch);
    uint16_t max_pos = (*metadata)->tree.floor / sizeof(uint16_t);
    *pos += step;
    if (*pos < 0 || *pos >= max_pos) continue;  // go up...
    ensure(btree_stack_push(&c->stack, p->page_num, *pos));
    p->page_num = btree_get_val_at(p, *metadata, (uint16_t)*pos);
    ensure(txn_get_page_and_metadata(c->tx, p, metadata));
    // go down all branches
    while ((*metadata)->tree.page_flags == page_flags_tree_branch) {
      max_pos       = (*metadata)->tree.floor / sizeof(uint16_t);
      uint16_t next = step > 0 ? 0 : (max_pos - 1);
      ensure(btree_stack_push(&c->stack, p->page_num, (int16_t)next));
      p->page_num = btree_get_val_at(p, *metadata, next);
      ensure(txn_get_page_and_metadata(c->tx, p, metadata));
    }
    if (step > 0) {
      *pos = ~0;
    } else {
      *pos = ~(int16_t)((*metadata)->tree.floor / sizeof(uint16_t));
    }
    return success();
  }
  *done = true;
  return success();
}
// end::btree_iterate_next_page[]
// tag::btree_iterate[]
static result_t btree_iterate(btree_cursor_t* c, int8_t step) {
  page_metadata_t* metadata;
  int16_t pos;
  page_t p = {0};
  ensure(btree_stack_pop(&c->stack, &p.page_num, &pos));
  ensure(txn_get_page_and_metadata(c->tx, &p, &metadata));
  while (true) {
    assert(metadata->tree.page_flags == page_flags_tree_leaf);
    uint16_t* positions = p.address;
    uint16_t max_pos    = metadata->tree.floor / sizeof(uint16_t);
    if (pos < 0) {
      pos = ~pos;
      if (step < 0) pos--;  // moving to prev, but was on > item
    }
    if (pos >= 0 && pos < max_pos) {  // still same page
      c->key.address =
          varint_decode(p.address + positions[pos], &c->key.size);
      uint8_t* end =
          varint_decode(c->key.address + c->key.size, &c->val);
      c->flags   = *end++;
      c->has_val = true;
      ensure(btree_stack_push(&c->stack, p.page_num, pos + step));
      return success();
    }
    bool d = false;
    ensure(btree_iterate_next_page(c, &p, &metadata, &pos, step, &d));
    if (d) {
      c->has_val = false;
      break;
    }
  }
  return success();
}
// end::btree_iterate[]

// tag::btree_cursor_search[]
result_t btree_cursor_search(btree_cursor_t* c) {
  assert(btree_validate_key(&c->key));
  btree_val_t kvp = {.key = c->key, .tree_id = c->tree_id};
  // handle cursor reuse for multiple queries
  ensure(btree_free_cursor(c));
  page_t p;
  page_metadata_t* metadata;
  ensure(btree_get_leaf_page_for(c->tx, &kvp, &p, &metadata));
  ensure(btree_stack_push(
      &c->tx->state->tmp.stack, p.page_num, kvp.position));

  // <1>
  memcpy(&c->stack, &c->tx->state->tmp.stack, sizeof(btree_stack_t));
  memset(&c->tx->state->tmp.stack, 0, sizeof(btree_stack_t));

  return success();
}
result_t btree_get_next(btree_cursor_t* cursor) {
  return btree_iterate(cursor, 1);
}
result_t btree_get_prev(btree_cursor_t* cursor) {
  return btree_iterate(cursor, -1);
}
// end::btree_cursor_search[]

// tag::btree_free_cursor[]
result_t btree_free_cursor(btree_cursor_t* cursor) {
  if (cursor->stack.size == 0) return success();  // already freed
  if (cursor->tx->state->tmp.stack.size == 0) {
    // can reuse memory
    btree_stack_clear(&cursor->stack);
    memcpy(&cursor->tx->state->tmp.stack, &cursor->stack,
        sizeof(btree_stack_t));
    memset(&cursor->stack, 0, sizeof(btree_stack_t));
    return success();
  }
  return btree_stack_free(&cursor->stack);
}
// end::btree_free_cursor[]

// tag::btree_remove_entry[]
static uint64_t btree_remove_entry(
    page_t* p, page_metadata_t* metadata, uint16_t pos) {
  span_t key, entry;
  uint64_t val;
  uint8_t flags;
  btree_get_entry_at(p, metadata, pos, &key, &val, &entry, &flags);
  memset(entry.address, 0, entry.size);
  uint16_t* positions = p->address;
  memmove(positions + pos, positions + pos + 1,
      metadata->tree.floor - (pos + 1) * sizeof(uint16_t));
  metadata->tree.floor -= (uint16_t)sizeof(uint16_t);
  positions[metadata->tree.floor / sizeof(uint16_t)] = 0;
  metadata->tree.free_space += sizeof(uint16_t) + entry.size;
  return val;
}
// end::btree_remove_entry[]

// tag::btree_balance_entries[]
static result_t btree_balance_entries(page_t* p1, page_metadata_t* m1,
    page_t* p2, page_metadata_t* m2) {
  uint64_t val;
  uint16_t p1_base    = m1->tree.floor / sizeof(uint16_t);
  uint16_t max_p2_pos = m2->tree.floor / sizeof(uint16_t);
  uint16_t p2_pos     = 0;
  size_t total_moved  = 0;
  for (; p2_pos < max_p2_pos; p2_pos++) {
    span_t key, entry;
    uint8_t flags;
    btree_get_entry_at(p2, m2, p2_pos, &key, &val, &entry, &flags);
    if (m1->tree.free_space < entry.size + sizeof(uint16_t)) {
      break;  // no more room
    }
    if (entry.size + sizeof(uint16_t) >
        m1->tree.ceiling - m1->tree.floor) {
      ensure(btree_defrag(p1, m1));
      if (entry.size + sizeof(uint16_t) >
          m1->tree.ceiling - m1->tree.floor)
        break;  // still can't find room? abort
    }
    void* dst = btree_insert_to_page(
        p1, m1, (int16_t)(p2_pos + p1_base), (uint16_t)entry.size);
    memcpy(dst, entry.address, entry.size);
    memset(entry.address, 0, entry.size);
    total_moved += entry.size + sizeof(uint16_t);
  }
  m2->tree.free_space += total_moved;
  m2->tree.floor -= p2_pos * sizeof(uint16_t);
  memmove(p2->address, p2->address + p2_pos * sizeof(uint16_t),
      (max_p2_pos - p2_pos) * sizeof(uint16_t));
  memset(p2->address + m2->tree.floor * sizeof(uint16_t), 0,
      (max_p2_pos - m2->tree.floor) * sizeof(uint16_t));
  return success();
}
// end::btree_balance_entries[]

static result_t btree_maybe_merge_pages(
    txn_t* tx, page_t* p, page_metadata_t* metadata);

// tag::btree_remove_from_parent[]
static result_t btree_remove_from_parent(txn_t* tx, page_t* parent,
    page_metadata_t* parent_metadata, page_t* remove,
    uint16_t remove_pos) {
  ensure(txn_free_page(tx, remove));
  btree_remove_entry(parent, parent_metadata, remove_pos);
  if (remove_pos == 0) {  // ensure leftmost branch key is empty
    uint64_t val = btree_get_val_at(parent, parent_metadata, 0);
    btree_remove_entry(parent, parent_metadata, 0);
    uint8_t* dst = btree_insert_to_page(parent, parent_metadata,
        ~0 /*insert new*/, (uint16_t)(1 + varint_get_length(val)));
    *dst++       = 0;  // empty key size
    varint_encode(val, dst);
  }
  ensure(btree_maybe_merge_pages(tx, parent, parent_metadata));
  if (parent_metadata->tree.floor != sizeof(uint16_t))
    return success();
  page_t p = {// only remaining item, replace the parent page
      .page_num = btree_get_val_at(parent, parent_metadata, 0)};
  page_metadata_t* metadata;
  ensure(txn_get_page_and_metadata(tx, &p, &metadata));
  memcpy(parent_metadata, metadata, sizeof(page_metadata_t));
  memcpy(parent->address, p.address, PAGE_SIZE);
  ensure(txn_free_page(tx, &p));
  return success();
}
// end::btree_remove_from_parent[]

// tag::btree_maybe_free_empty_page[]
static result_t btree_maybe_free_empty_page(txn_t* tx, page_t* p,
    page_metadata_t* metadata, page_t* parent,
    page_metadata_t* parent_metadata, uint16_t position) {
  if (metadata->tree.floor != 0) return success();
  ensure(txn_modify_page(tx, parent));  // emptied the page
  ensure(txn_modify_metadata(tx, parent->page_num, &parent_metadata));
  ensure(btree_remove_from_parent(
      tx, parent, parent_metadata, p, position));
  return success();
}
// end::btree_maybe_free_empty_page[]

// tag::btree_merge_pages[]
static result_t btree_merge_pages(txn_t* tx, page_t* p,
    page_metadata_t* metadata, page_t* parent,
    page_metadata_t* parent_metadata, page_t* sibling,
    uint16_t sibling_pos) {
  page_metadata_t* s_metadata;
  ensure(txn_modify_page(tx, sibling));
  ensure(txn_modify_metadata(tx, sibling->page_num, &s_metadata));

  ensure(btree_balance_entries(p, metadata, sibling, s_metadata));

  if (s_metadata->tree.floor == 0) {  // completely emptied sibling
    ensure(btree_remove_from_parent(
        tx, parent, parent_metadata, sibling, sibling_pos));
    return success();
  }
  uint64_t val;
  span_t entry;
  btree_val_t ref = {.val = sibling->page_num};
  uint8_t flags;
  ensure(txn_modify_page(tx, parent));
  ensure(txn_modify_metadata(tx, parent->page_num, &parent_metadata));

  btree_remove_entry(parent, parent_metadata, sibling_pos);
  btree_get_entry_at(
      sibling, s_metadata, 0, &ref.key, &val, &entry, &flags);
  btree_search_pos_in_page(parent, parent_metadata, &ref);
  ensure(btree_set_in_page(tx, parent->page_num, &ref, 0));
  return success();
}
// end::btree_merge_pages[]

// tag::btree_maybe_merge_pages[]
static result_t btree_maybe_merge_pages(
    txn_t* tx, page_t* p, page_metadata_t* metadata) {
  // if page is over 2/3 full, we'll do nothing
  if (metadata->tree.free_space < (PAGE_SIZE / 3) * 2 ||
      tx->state->tmp.stack.index == 0)  // nothing to merge with
    return success();
  int16_t cur_pos;
  page_t parent = {0};
  ensure(btree_stack_pop(
      &tx->state->tmp.stack, &parent.page_num, &cur_pos));
  page_metadata_t* parent_metadata;
  ensure(txn_get_page_and_metadata(tx, &parent, &parent_metadata));
  uint16_t max_pos = parent_metadata->tree.floor / sizeof(uint16_t);
  if (cur_pos == 0 || cur_pos == max_pos - 1) {
    return btree_maybe_free_empty_page(  // not merging at start / end
        tx, p, metadata, &parent, parent_metadata, (uint16_t)cur_pos);
  }
  uint16_t sibling_pos = (uint16_t)cur_pos + 1;
  page_t sibling       = {.page_num = btree_get_val_at(
                        &parent, parent_metadata, sibling_pos)};
  page_metadata_t* s_metadata;
  ensure(txn_get_metadata(tx, sibling.page_num, &s_metadata));
  if (s_metadata->tree.page_flags != metadata->tree.page_flags) {
    return btree_maybe_free_empty_page(  // cannot merge leaf & branch
        tx, p, metadata, &parent, parent_metadata, (uint16_t)cur_pos);
  }
  ensure(btree_merge_pages(tx, p, metadata, &parent, parent_metadata,
      &sibling, sibling_pos));
  return success();
}
// end::btree_maybe_merge_pages[]

// tag::btree_del[]
result_t btree_del(txn_t* tx, btree_val_t* del) {
  assert(btree_validate_key(&del->key));
  page_t p;
  page_metadata_t* metadata;
  ensure(btree_get_leaf_page_for(tx, del, &p, &metadata));
  if (del->last_match != 0) {
    del->has_val = false;
    return success();
  }
  del->has_val = true;
  ensure(txn_modify_metadata(tx, p.page_num, &metadata));
  ensure(txn_modify_page(tx, &p));
  del->val =
      btree_remove_entry(&p, metadata, (uint16_t)del->position);
  ensure(btree_maybe_merge_pages(tx, &p, metadata));
  return success();
}
// end::btree_del[]