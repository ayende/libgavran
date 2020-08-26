#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

static result_t btree_validate_key(span_t* key) {
  ensure(key->size > 0);
  ensure(key->size <= 512);
  ensure(key->address, msg("Key cannot have a NULL address"));
  return success();
}

// tag::btree_create[]
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
      assert(kvp->position == 0);
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

static void* btree_insert_to_page(page_t* p,
    page_metadata_t* metadata, uint16_t pos, uint16_t req_size) {
  uint16_t* positions = p->address;
  size_t max_pos      = metadata->tree.floor / sizeof(uint16_t);
  metadata->tree.floor += sizeof(uint16_t);
  metadata->tree.free_space -= sizeof(uint16_t);
  // create a gap for the new value in the right place
  memmove(positions + pos + 1, positions + pos,
      ((max_pos - pos) * sizeof(uint16_t)));
  metadata->tree.ceiling -= req_size;
  metadata->tree.free_space -= req_size;
  positions[pos] = metadata->tree.ceiling;
  return p->address + metadata->tree.ceiling;
}

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
      memset(val_end, 0, (size_t)(val_end - end));
      metadata->tree.free_space += (uint16_t)(end - val_end);
      return;
    }
    memset(start, 0, (size_t)(end - start));  // reset value
  } else {
    if (old) old->has_val = false;
    set->position = ~set->position;
  }
  void* dst = btree_insert_to_page(
      p, metadata, (uint16_t)set->position, req_size);
  uint8_t* key_start = varint_encode(set->key.size, dst);
  memcpy(key_start, set->key.address, set->key.size);
  varint_encode(set->val, key_start + set->key.size);
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
  uint16_t* positions    = p->address;
  size_t max_pos         = metadata->tree.floor / sizeof(uint16_t);
  for (size_t i = 0; i < max_pos; i++) {
    uint64_t size;
    uint16_t cur_pos = positions[i];
    uint8_t* end     = varint_decode(
        varint_decode(tmp + cur_pos, &size) + size, &size);
    uint16_t entry_size = (uint16_t)(end - (tmp + cur_pos));
    metadata->tree.ceiling -= entry_size;
    positions[i] = metadata->tree.ceiling;
    memcpy(p->address + metadata->tree.ceiling, tmp + cur_pos,
        entry_size);
  }
  return success();
}

static result_t btree_set_in_page(
    txn_t* tx, uint64_t page_num, btree_val_t* set, btree_val_t* old);

static result_t btree_create_root_page(
    txn_t* tx, page_t* p, btree_val_t* set) {
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
      varint_decode(p->address + *(uint16_t*)p->address, &key_size);
  btree_val_t ref = {.val = p->page_num,
      .key                = {.address = key_start, .size = key_size},
      .position           = ~0};  // new entry
  size_t req_size = varint_get_length(ref.key.size) + ref.key.size +
                    varint_get_length(ref.val);
  btree_write_in_page(
      &root, root_metadata, (uint16_t)req_size, &ref, 0);
  ensure(btree_stack_push(&tx->state->tmp_stack, root.page_num, 0));
  return success();
}
static uint64_t btree_get_val_at(page_t* p, uint16_t pos);

static result_t btree_get_leftmost_key(txn_t* tx, page_t* p,
    page_metadata_t* metadata, span_t* leftmost_key) {
  while (metadata->tree.page_flags == page_flags_tree_branch) {
    p->page_num = btree_get_val_at(p, 0);
    ensure(txn_get_page_and_metadata(tx, p, &metadata));
  }
  span_t entry;
  ensure(btree_get_at(p, metadata, 0, leftmost_key, &entry);
  return success();
}

static result_t btree_split_page(txn_t* tx, page_t* p,
    page_metadata_t** metadata, btree_val_t* set) {
  btree_stack_t* stack = &tx->state->tmp_stack;
  if (stack->index == 0) {  // at root
    ensure(btree_create_root_page(tx, p, set));
  }
  page_metadata_t* cur_metadata = *metadata;
  page_t cur_page               = *p;
  page_t other                  = {.number_of_pages = 1};
  page_metadata_t* o_metadata;
  ensure(txn_allocate_page(tx, &other, &o_metadata, p->page_num));
  o_metadata->tree.page_flags = cur_metadata->tree.page_flags;
  o_metadata->tree.ceiling    = PAGE_SIZE;
  o_metadata->tree.free_space = PAGE_SIZE;

  uint16_t max_pos = cur_metadata->tree.floor / sizeof(uint16_t);
  bool seq_write_up =
      max_pos == (uint16_t)(~set->position) && set->last_match > 0;
  bool seq_write_down = (~set->position == 0) && set->last_match < 0;
  btree_val_t ref = {.tree_id = set->tree_id, .val = other.page_num};
  if (seq_write_up || seq_write_down) {  // optimization: no split req
    ref.key = set->key;
    memcpy(p, &other, sizeof(page_t));
    *metadata = o_metadata;
  } else {
    uint16_t* positions   = p->address;
    uint16_t* o_positions = other.address;
    for (size_t idx = max_pos / 2, o_idx = 0; idx < max_pos;
         idx++, o_idx++) {
      uint64_t size, val_size;
      uint8_t* key_start =
          varint_decode(p->address + positions[idx], &size);
      void* end = varint_decode(key_start + size, &val_size);
      uint16_t entry_size =
          (uint16_t)(end - (p->address + positions[idx]));
      o_metadata->tree.ceiling -= entry_size;
      memcpy(other.address + o_metadata->tree.ceiling,
          p->address + positions[idx], entry_size);
      if (ref.key.address == 0) {  // first item in new page
        ref.key.address = other.address + o_metadata->tree.ceiling +
                          varint_get_length(size);
        ref.key.size = size;
      }
      o_positions[o_idx] = o_metadata->tree.ceiling;
      o_metadata->tree.floor += sizeof(uint16_t);
      o_metadata->tree.free_space -= sizeof(uint16_t) + entry_size;
      memset(p->address + positions[idx], 0, entry_size);
      cur_metadata->tree.free_space += sizeof(uint16_t) + entry_size;
    }
    memset(positions + (max_pos / 2), 0,
        (max_pos - (max_pos / 2)) * sizeof(uint16_t));
    cur_metadata->tree.floor -=
        (max_pos - (max_pos / 2)) * sizeof(uint16_t);
    if (memcmp(ref.key.address, set->key.address,
            MIN(set->key.size, ref.key.size)) < 0) {
      memcpy(p, &other, sizeof(page_t));
      *metadata = o_metadata;
    }
  }
  page_t parent;
  page_metadata_t* p_metadata;
  ensure(btree_stack_pop(stack, &parent.page_num, &ref.position));
  ensure(txn_get_page_and_metadata(tx, &parent, &p_metadata));
  btree_search_pos_in_page(&parent, p_metadata, &ref);
  assert(~ref.position > 0 && ref.position < 0);
  if (~ref.position == 1) {
    span_t leftmost_key;
    ensure(btree_get_leftmost_key(
        tx, &cur_page, cur_metadata, &leftmost_key));
  }
  ensure(btree_set_in_page(tx, parent.page_num, &ref, 0));
  if (ref.tree_id_changed) {
    set->tree_id_changed = true;
    set->tree_id         = ref.tree_id;
  }
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
  if (req_size + sizeof(uint16_t) >  // not enough space?
      (metadata->tree.ceiling - metadata->tree.floor)) {
    if (req_size + sizeof(uint16_t) < metadata->tree.free_space) {
      // there should be enough _space_, let's defrag
      ensure(btree_defrag(&p, metadata));  // let's see if this helps
    }
    // check again...
    if (req_size + sizeof(uint16_t) >
        (metadata->tree.ceiling - metadata->tree.floor)) {
      ensure(btree_split_page(tx, &p, &metadata, set));
      // need to adjust position after the changes...
      btree_search_pos_in_page(&p, metadata, set);
    }
  }

  btree_write_in_page(&p, metadata, (uint16_t)req_size, set, old);
  return success();
}

// tag::btree_get_leaf_page_for[]
static result_t btree_get_leaf_page_for(txn_t* tx, btree_val_t* kvp,
    page_t* p, page_metadata_t** metadata) {
  p->page_num = kvp->tree_id;
  ensure(txn_get_page_and_metadata(tx, p, metadata));
  assert((*metadata)->common.page_flags == page_flags_tree_branch ||
         (*metadata)->common.page_flags == page_flags_tree_leaf);
  btree_stack_clear(&tx->state->tmp_stack);
  while ((*metadata)->tree.page_flags == page_flags_tree_branch) {
    btree_search_pos_in_page(p, *metadata, kvp);
    if (kvp->position < 0) kvp->position = ~kvp->position;
    if (kvp->last_match) kvp->position--;  // went too far
    ensure(btree_stack_push(
        &tx->state->tmp_stack, p->page_num, kvp->position));
    uint16_t max_pos    = (*metadata)->tree.floor / sizeof(uint16_t);
    uint16_t pos        = MIN(max_pos - 1, (uint16_t)kvp->position);
    uint16_t* positions = p->address;
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
// end::btree_get_leaf_page_for[]

static result_t btree_free_page_recursive(
    txn_t* tx, uint64_t page_num) {
  page_t p = {.page_num = page_num};
  page_metadata_t* metadata;
  ensure(txn_get_page_and_metadata(tx, &p, &metadata));
  if (metadata->tree.page_flags != page_flags_tree_leaf) {
    uint16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
    for (uint16_t i = 0; i < max_pos; i++) {
      uint64_t child = btree_get_val_at(&p, i);
      ensure(btree_free_page_recursive(tx, child));
    }
  }
  ensure(txn_free_page(tx, &p));
  return success();
}

result_t btree_drop(txn_t* tx, uint64_t tree_id) {
  return btree_free_page_recursive(tx, tree_id);
}

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

result_t btree_get(txn_t* tx, btree_val_t* kvp) {
  assert(btree_validate_key(&kvp->key));
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

static uint64_t btree_get_val_at(page_t* p, uint16_t pos) {
  uint16_t* positions = p->address;
  uint64_t k, v;
  uint8_t* key_start = varint_decode(p->address + positions[pos], &k);
  varint_decode(key_start + k, &v);
  return v;
}

static result_t btree_cursor_at(btree_cursor_t* c, bool start) {
  page_t p = {.page_num = c->tree_id};
  page_metadata_t* metadata;
  ensure(txn_get_page_and_metadata(c->tx, &p, &metadata));
  assert(metadata->common.page_flags == page_flags_tree_branch ||
         metadata->common.page_flags == page_flags_tree_leaf);
  btree_stack_clear(&c->tx->state->tmp_stack);
  int16_t pos = 0;
  while (metadata->tree.page_flags == page_flags_tree_branch) {
    uint16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
    pos              = start ? 0 : (int16_t)max_pos - 1;
    ensure(
        btree_stack_push(&c->tx->state->tmp_stack, p.page_num, pos));
    uint16_t* positions = p.address;
    size_t key_size;
    uint8_t* entry     = p.address + positions[pos];
    uint8_t* val_start = varint_decode(entry, &key_size) + key_size;
    varint_decode(val_start, &p.page_num);
    ensure(txn_get_page_and_metadata(c->tx, &p, &metadata));
  }
  assert(metadata->tree.page_flags == page_flags_tree_leaf);
  int16_t leaf_max_pos = metadata->tree.floor / sizeof(uint16_t);
  ensure(btree_stack_push(&c->tx->state->tmp_stack, p.page_num,
      ~(start ? 0 : leaf_max_pos)));

  memcpy(&c->stack, &c->tx->state->tmp_stack, sizeof(btree_stack_t));
  memset(&c->tx->state->tmp_stack, 0, sizeof(btree_stack_t));

  return success();
}

result_t btree_cursor_at_start(btree_cursor_t* cursor) {
  return btree_cursor_at(cursor, true);
}
result_t btree_cursor_at_end(btree_cursor_t* cursor) {
  return btree_cursor_at(cursor, false);
}

result_t btree_cursor_search(btree_cursor_t* c) {
  assert(btree_validate_key(&c->key));
  btree_val_t kvp = {.key = c->key, .tree_id = c->tree_id};
  // handle cursor reuse for multiple queries
  ensure(btree_free_cursor(c));
  page_t p;
  page_metadata_t* metadata;
  ensure(btree_get_leaf_page_for(c->tx, &kvp, &p, &metadata));
  ensure(btree_stack_push(
      &c->tx->state->tmp_stack, p.page_num, kvp.position));

  memcpy(&c->stack, &c->tx->state->tmp_stack, sizeof(btree_stack_t));
  memset(&c->tx->state->tmp_stack, 0, sizeof(btree_stack_t));

  return success();
}

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
      // we are moving to previous, but we were on greater than search
      if (step < 0) pos--;
    } else {
      pos += step;
    }
    if (pos >= 0 && pos < max_pos) {  // still same page
      c->key.address =
          varint_decode(p.address + positions[pos], &c->key.size);
      varint_decode(c->key.address + c->key.size, &c->val);
      c->has_val = true;
      ensure(btree_stack_push(&c->stack, p.page_num, pos));
      return success();
    }
    while (true) {
      if (c->stack.index == 0) {
        c->has_val = false;
        return success();  // done
      }
      ensure(btree_stack_pop(&c->stack, &p.page_num, &pos));
      ensure(txn_get_page_and_metadata(c->tx, &p, &metadata));
      assert(metadata->tree.page_flags == page_flags_tree_branch);
      max_pos = metadata->tree.floor / sizeof(uint16_t);
      pos += step;
      if (pos < 0 || pos >= max_pos) {
        continue;  // go up...
      }
      ensure(btree_stack_push(&c->stack, p.page_num, pos));
      p.page_num = btree_get_val_at(&p, (uint16_t)pos);
      ensure(txn_get_page_and_metadata(c->tx, &p, &metadata));
      // go down all branches
      while (metadata->tree.page_flags == page_flags_tree_branch) {
        max_pos           = metadata->tree.floor / sizeof(uint16_t);
        uint16_t edge_pos = step > 0 ? 0 : (max_pos - 1);
        ensure(btree_stack_push(
            &c->stack, p.page_num, (int16_t)edge_pos));
        p.page_num = btree_get_val_at(&p, edge_pos);
        ensure(txn_get_page_and_metadata(c->tx, &p, &metadata));
      }
      if (step > 0) {
        pos = ~0;
      } else {
        pos = ~(int16_t)(metadata->tree.floor / sizeof(uint16_t));
      }
      break;
    }
  }
}

result_t btree_get_next(btree_cursor_t* cursor) {
  return btree_iterate(cursor, 1);
}
result_t btree_get_prev(btree_cursor_t* cursor) {
  return btree_iterate(cursor, -1);
}
result_t btree_free_cursor(btree_cursor_t* cursor) {
  if (cursor->tx->state->tmp_stack.size == 0) {
    // can reuse memory
    memcpy(&cursor->tx->state->tmp_stack, &cursor->stack,
        sizeof(btree_stack_t));
    return success();
  }
  return btree_stack_free(&cursor->stack);
}

static uint64_t btree_remove_entry(
    page_t* p, page_metadata_t* metadata, uint16_t pos) {
  uint16_t* positions = p->address;
  uint64_t ks, v;
  uint8_t* start = p->address + positions[pos];
  uint8_t* end   = varint_decode(varint_decode(start, &ks) + ks, &v);
  memset(start, 0, (size_t)(end - start));
  memmove(positions + pos, positions + pos + 1,
      metadata->tree.floor - (pos + 1) * sizeof(uint16_t));
  metadata->tree.floor -= (uint16_t)sizeof(uint16_t);
  positions[metadata->tree.floor / sizeof(uint16_t)] = 0;
  metadata->tree.free_space +=
      sizeof(uint16_t) + (uint16_t)(end - start);
  return v;
}

static result_t btree_find_best_sibling_to_merge(txn_t* tx,
    page_t* parent, page_metadata_t* parent_metadata,
    uint16_t cur_pos, uint16_t* sibling_pos) {
  uint16_t siblings_count =
      parent_metadata->tree.floor / sizeof(uint16_t);
  assert(siblings_count >= 2);
  if (cur_pos == siblings_count - 1) {
    *sibling_pos = (uint16_t)(cur_pos - 1);
    return success();
  }
  if (cur_pos == 0) {
    *sibling_pos = 1;
    return success();
  }
  // choose sibling with most free space
  page_metadata_t *right, *left;
  uint64_t page = btree_get_val_at(parent, (uint16_t)(cur_pos - 1));
  ensure(txn_get_metadata(tx, page, &left));
  page = btree_get_val_at(parent, (uint16_t)(cur_pos + 1));
  ensure(txn_get_metadata(tx, page, &right));
  if (left->tree.free_space > right->tree.free_space)
    *sibling_pos = (uint16_t)(cur_pos - 1);
  else
    *sibling_pos = (uint16_t)(cur_pos + 1);
  return success();
}

static result_t btree_get_at(page_t* p, page_metadata_t* metadata,
    uint16_t pos, span_t* key, span_t* entry) {
  uint16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
  ensure(pos < max_pos, msg("pos out of range"), with(pos, "%d"),
      with(max_pos, "%d"));
  uint64_t val;
  uint16_t* positions = p->address;
  entry->address      = p->address + positions[pos];
  key->address        = varint_decode(entry->address, &key->size);
  void* end           = varint_decode(key->address + key->size, &val);
  entry->size         = (size_t)(end - entry->address);
  return success();
}

static result_t btree_balance_entries(page_t* p1, page_metadata_t* m1,
    page_t* p2, page_metadata_t* m2, int8_t* step) {
  span_t k1, k2, entry;
  ensure(btree_get_at(p1, m1, 0, &k1, &entry));
  ensure(btree_get_at(p2, m2, 0, &k2, &entry));
  int res = memcmp(k1.address, k2.address, MIN(k1.size, k2.size));
  uint16_t p1_pos, p2_pos;

  if (res < 0) {
    p1_pos = m1->tree.floor / sizeof(uint16_t);
    p2_pos = 0;
    *step  = 0;
  } else {
    p1_pos = 0;
    p2_pos = m2->tree.floor / sizeof(uint16_t) - 1;
    *step  = -1;
  }

  while (m2->tree.floor) {
    span_t key;
    ensure(btree_get_at(p2, m2, p2_pos, &key, &entry));
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
    void* dst =
        btree_insert_to_page(p1, m1, p1_pos, (uint16_t)entry.size);
    memcpy(dst, entry.address, entry.size);
    btree_remove_entry(p2, m2, p2_pos);
    p2_pos += *step;
    p1_pos++;
  }
  return success();
}

static result_t btree_remove_from_parent(txn_t* tx, page_t* parent,
    page_metadata_t* parent_metadata, page_t* remove,
    uint16_t remove_pos, page_t* sibling,
    page_metadata_t* s_metadata) {
  ensure(txn_free_page(tx, remove));
  btree_remove_entry(parent, parent_metadata, remove_pos);
  if (parent_metadata->tree.floor == sizeof(uint16_t)) {
    // only remaining item, replace
    memcpy(parent_metadata, s_metadata, sizeof(page_metadata_t));
    memcpy(parent->address, sibling->address, PAGE_SIZE);
    ensure(txn_free_page(tx, sibling));
  }
  return success();
}

static result_t btree_maybe_merge_pages(txn_t* tx, page_t* p,
    page_metadata_t* metadata, btree_val_t* del) {
  // if page is over 2/3 full, we'll do nothing
  if (metadata->tree.free_space < (PAGE_SIZE / 3) * 2)
    return success();
  if (tx->state->tmp_stack.index == 0)
    return success();  // nothing to merge with

  int16_t cur_pos;
  page_t parent = {0};
  ensure(btree_stack_pop(
      &tx->state->tmp_stack, &parent.page_num, &cur_pos));
  page_metadata_t* parent_metadata;
  ensure(txn_get_page_and_metadata(tx, &parent, &parent_metadata));
  uint16_t sibling_pos;
  ensure(btree_find_best_sibling_to_merge(
      tx, &parent, parent_metadata, (uint16_t)cur_pos, &sibling_pos));
  page_t sibling = {
      .page_num = btree_get_val_at(&parent, sibling_pos)};
  page_metadata_t* s_metadata;
  ensure(txn_modify_page(tx, &sibling));
  ensure(txn_modify_metadata(tx, sibling.page_num, &s_metadata));
  ensure(txn_modify_page(tx, &parent));
  ensure(txn_modify_metadata(tx, parent.page_num, &parent_metadata));

  // cannot merge leaf & branch pages
  if (s_metadata->tree.page_flags != metadata->tree.page_flags) {
    if (metadata->tree.floor == 0) {  // emptied the page
      ensure(btree_remove_from_parent(tx, &parent, parent_metadata, p,
          (uint16_t)cur_pos, &sibling, s_metadata));
    }
    return success();
  }

  int8_t step;
  ensure(btree_balance_entries(
      p, metadata, &sibling, s_metadata, &step));
  if (s_metadata->tree.floor == 0) {  // completed emptied sibling...
    ensure(btree_remove_from_parent(tx, &parent, parent_metadata,
        &sibling, sibling_pos, p, metadata));
    return success();
  }
  span_t entry;
  btree_val_t ref = {0};
  if (step != -1) {
    ref.val = sibling.page_num;
    btree_remove_entry(&parent, parent_metadata, sibling_pos);
    ensure(btree_get_at(&sibling, s_metadata, 0, &ref.key, &entry));
  } else {
    ref.val = p->page_num;
    btree_remove_entry(&parent, parent_metadata, (uint16_t)cur_pos);
    ensure(btree_get_at(p, metadata, 0, &ref.key, &entry));
  }
  btree_search_pos_in_page(&parent, parent_metadata, &ref);
  ensure(btree_set_in_page(tx, parent.page_num, &ref, 0));
  if (ref.tree_id_changed) {
    del->tree_id_changed = true;
    del->tree_id         = ref.tree_id;
  }
  return success();
}

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
  ensure(btree_maybe_merge_pages(tx, &p, metadata, del));
  return success();
}