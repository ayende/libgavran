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
  int16_t max_pos =
      (int16_t)(metadata->tree.floor / sizeof(uint16_t));
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
      memset(val_end, 0, (size_t)(val_end - end));
      metadata->tree.free_space += (uint16_t)(end - val_end);
      return;
    }
    memset(start, 0, (size_t)(end - start));  // reset value
  } else {
    if (old) old->has_val = false;
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
  uint16_t* positions    = p->address;
  size_t max_pos         = metadata->tree.floor / sizeof(uint16_t);
  for (size_t i = 0; i < max_pos; i++) {
    uint64_t size;
    uint16_t cur_pos = positions[i];
    uint8_t* end     = varint_decode(
        varint_decode(tmp + cur_pos, &size) + size, &size);
    positions[i] = (uint16_t)(end - (tmp + cur_pos));
    metadata->tree.ceiling -= positions[i];
    memcpy(p->address + metadata->tree.ceiling, tmp + cur_pos,
        (size_t)(end - (tmp + cur_pos)));
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

static result_t btree_split_page(txn_t* tx, page_t* p,
    page_metadata_t* metadata, btree_val_t* set) {
  btree_stack_t* stack = &tx->state->tmp_stack;
  if (stack->index == 0) {  // at root
    ensure(btree_create_root_page(tx, p, set));
  }

  page_t other = {.number_of_pages = 1};
  page_metadata_t* o_metadata;
  ensure(txn_allocate_page(tx, &other, &o_metadata, p->page_num));
  o_metadata->tree.page_flags = metadata->tree.page_flags;
  o_metadata->tree.ceiling    = PAGE_SIZE;
  o_metadata->tree.free_space = PAGE_SIZE;

  uint16_t max_pos = metadata->tree.floor / sizeof(uint16_t);
  bool seq_write_up =
      max_pos == (uint16_t)(~set->position) && set->last_match > 0;
  bool seq_write_down = set->position == 0 && set->last_match < 0;
  btree_val_t ref = {.tree_id = set->tree_id, .val = other.page_num};
  if (seq_write_down || seq_write_up) {  // optimization: no split req
    ref.key = set->key;
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
      o_positions[o_idx] = o_metadata->tree.ceiling;
      o_metadata->tree.floor += sizeof(uint16_t);
      o_metadata->tree.free_space -= sizeof(uint16_t) + entry_size;
      memset(p->address + positions[idx], 0, entry_size);
      metadata->tree.free_space += sizeof(uint16_t) + entry_size;
    }
    memset(positions + (max_pos / 2), 0,
        (max_pos - (max_pos / 2)) * sizeof(uint16_t));
    metadata->tree.floor -=
        (max_pos - (max_pos / 2)) * sizeof(uint16_t);
  }

  page_t parent;
  page_metadata_t* p_metadata;
  ensure(btree_stack_pop(stack, &parent.page_num, &ref.position));
  ensure(txn_get_page_and_metadata(tx, &parent, &p_metadata));
  btree_search_pos_in_page(&parent, p_metadata, &ref);
  ensure(btree_set_in_page(tx, parent.page_num, &ref, 0));
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
  if (req_size + sizeof(uint16_t) > metadata->tree.free_space) {
    ensure(btree_split_page(tx, &p, metadata, set));
    return btree_set(tx, set, old);  // now try again
  }
  if (req_size + sizeof(uint16_t) >
      (metadata->tree.ceiling - metadata->tree.floor)) {
    ensure(btree_defrag(&p, metadata));  // let's see if this helps
    if (req_size + sizeof(uint16_t) >
        (metadata->tree.ceiling - metadata->tree.floor)) {
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

static uint64_t btree_get_val_at(page_t* p, uint16_t pos);

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

static uint64_t btree_get_val_at(page_t* p, uint16_t pos) {
  uint16_t* positions = p->address;
  uint64_t k, v;
  uint8_t* key_start = varint_decode(p->address + positions[pos], &k);
  varint_decode(key_start + k, &v);
  return v;
}

result_t btree_cursor_search(btree_cursor_t* c) {
  btree_val_t kvp = {.key = c->key, .tree_id = c->tree_id};
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
      pos += step;  // may underflow, handled via max_pos check
      if (pos >= max_pos) {
        break;  // go up...
      }
      ensure(btree_stack_push(&c->stack, p.page_num, pos));
      p.page_num = btree_get_val_at(&p, (uint16_t)pos);
      ensure(txn_get_page_and_metadata(c->tx, &p, &metadata));
      // go down all branches
      while (metadata->tree.page_flags == page_flags_tree_branch) {
        uint16_t edge_pos = step > 0 ? 0 : (max_pos - 1);
        p.page_num        = btree_get_val_at(&p, edge_pos);
        ensure(txn_get_page_and_metadata(c->tx, &p, &metadata));
        max_pos = metadata->tree.floor / sizeof(uint16_t);
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

static void btree_remove_entry(
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
}

static void btree_copy_entries(page_t* src,
    page_metadata_t* src_metadata, page_t* dst,
    page_metadata_t* dst_metadata) {
  uint16_t max_pos    = src_metadata->tree.floor / sizeof(uint16_t);
  uint16_t* positions = src->address;
  for (uint16_t i = 0; i < max_pos; i++) {
    btree_val_t ent;
    ent.key.address =
        varint_decode(src->address + positions[i], &ent.key.size);
    varint_decode(ent.key.address + ent.key.size, &ent.val);
    uint16_t req_size =
        (uint16_t)(varint_get_length(ent.key.size) + ent.key.size +
                   varint_get_length(ent.val));
    btree_search_pos_in_page(dst, dst_metadata, &ent);
    btree_write_in_page(dst, dst_metadata, req_size, &ent, 0);
  }
}

static result_t btree_maybe_merge_pages(
    txn_t* tx, page_t* p, page_metadata_t* metadata) {
  if (tx->state->tmp_stack.index == 0)
    return success();  // nothing to merge with

  int16_t cur_pos;
  page_t parent = {0};
  ensure(btree_stack_pop(
      &tx->state->tmp_stack, &parent.page_num, &cur_pos));
  page_metadata_t* parent_metadata;
  ensure(txn_get_page_and_metadata(tx, &parent, &parent_metadata));

  uint16_t siblings_count =
      parent_metadata->tree.floor / sizeof(uint16_t);
  assert(siblings_count >= 2);
  int16_t sibling_pos;
  if (cur_pos == siblings_count - 1) {
    sibling_pos = cur_pos - 1;
  } else {
    sibling_pos = cur_pos + 1;
  }

  page_t sibling = {
      .page_num = btree_get_val_at(&parent, (uint16_t)sibling_pos)};
  page_metadata_t* sibling_metadata;
  ensure(txn_get_page_and_metadata(tx, &sibling, &sibling_metadata));

  if (sibling_metadata->tree.page_flags != metadata->tree.page_flags)
    return success();  // cannot merge leaf & branch pages

  ensure(txn_modify_page(tx, &parent));
  ensure(txn_modify_metadata(tx, parent.page_num, &parent_metadata));

  uint32_t joint_free_space =
      metadata->tree.free_space + sibling_metadata->tree.free_space;
  // not enough free space to be worth the merge
  if (joint_free_space > (PAGE_SIZE / 4) * 3) return success();

  page_t joined = {.number_of_pages = 1};
  page_metadata_t* j_metadata;
  bool merge_parent =
      parent_metadata->tree.floor == 2 * sizeof(uint16_t);
  if (merge_parent) {
    joined     = parent;
    j_metadata = parent_metadata;
    memset(j_metadata, 0, sizeof(page_metadata_t));
  } else {
    ensure(txn_allocate_page(tx, &joined, &j_metadata, p->page_num));
  }

  j_metadata->tree.ceiling    = PAGE_SIZE;
  j_metadata->tree.free_space = PAGE_SIZE;
  j_metadata->tree.page_flags = metadata->tree.page_flags;

  btree_copy_entries(p, metadata, &joined, j_metadata);
  btree_copy_entries(&sibling, sibling_metadata, &joined, j_metadata);

  btree_remove_entry(&parent, parent_metadata, (uint16_t)cur_pos);
  btree_remove_entry(&parent, parent_metadata, (uint16_t)sibling_pos);

  ensure(txn_free_page(tx, p));
  ensure(txn_free_page(tx, &sibling));

  if (merge_parent == false) {
    btree_val_t ent = {.val = joined.page_num};
    ent.key.address = varint_decode(
        joined.address + *(uint16_t*)joined.address, &ent.key.size);
    btree_search_pos_in_page(&joined, j_metadata, &ent);
    uint16_t req_size =
        (uint16_t)(varint_get_length(ent.key.size) + ent.key.size +
                   varint_get_length(ent.val));
    btree_write_in_page(&parent, parent_metadata, req_size, &ent, 0);
  }

  return success();
}

result_t btree_del(txn_t* tx, btree_val_t* del) {
  page_t p;
  page_metadata_t* metadata;
  ensure(btree_get_leaf_page_for(tx, del, &p, &metadata));
  if (del->last_match != 0) {
    del->has_val = false;
    return success();
  }
  del->has_val = true;
  btree_remove_entry(&p, metadata, (uint16_t)del->position);
  ensure(btree_maybe_merge_pages(tx, &p, metadata));
  return success();
}