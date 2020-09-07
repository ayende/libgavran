#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

#define RECORD_SEPARATOR 30  // ASCII dedicated value

typedef struct multi_search_args {
  span_t key_buffer;
  uint64_t nested_id;
  uint64_t val;
  uint8_t pos;
  bool forward;
  uint8_t padding[6];
} multi_search_args_t;

typedef enum __attribute__((__packed__)) btree_multi_flags {
  btree_multi_flags_uniquifier = 1,
  btree_multi_flags_nested     = 2,
} btree_multi_flags_t;

static result_t btree_create_nested(
    txn_t *tx, uint64_t root_tree_id, uint64_t *nested_tree_id) {
  ensure(btree_create(tx, nested_tree_id));
  page_metadata_t *root, *nested;
  ensure(txn_modify_metadata(tx, root_tree_id, &root));
  ensure(txn_modify_metadata(tx, *nested_tree_id, &nested));

  if (root->tree.next_nested) {
    page_metadata_t *next_nested;
    ensure(txn_modify_metadata(
        tx, root->tree.next_nested, &next_nested));
    next_nested->tree.prev_nested = *nested_tree_id;
  }

  nested->tree.next_nested = root->tree.next_nested;
  root->tree.next_nested   = *nested_tree_id;
  return success();
}

static result_t btree_convert_to_nested(
    txn_t *tx, uint64_t tree_id, span_t *buf, uint64_t *nested_id) {
  uint8_t key_buf[10];
  *(uint8_t *)(buf->address + buf->size - 1) = 0;
  ensure(btree_create_nested(tx, tree_id, nested_id));
  btree_val_t set = {
      .tree_id = *nested_id, .key = {.size = sizeof(uint64_t)}};
  btree_val_t del   = {.tree_id = tree_id};
  btree_cursor_t it = {.tree_id = tree_id, .tx = tx};
  while (true) {
    it.key = *buf;  // search first match for key
    ensure(btree_cursor_search(&it));
    ensure(btree_get_next(&it));
    if (it.has_val == false) break;
    if (it.key.size != buf->size) break;
    if (memcmp(it.key.address, buf->address, buf->size - 1)) break;
    varint_encode(it.val, key_buf);
    set.key.address = key_buf;
    set.key.size    = varint_get_length(it.val);
    ensure(btree_set(tx, &set, 0));  // add to nested
    del.key = it.key;
    ensure(btree_del(tx, &del));  // remove from root tree
  }
  // here we moved all the values to the nested tree, now update root
  memset(&set, 0, sizeof(btree_val_t));
  set.tree_id = tree_id;
  set.key     = *buf;
  set.flags   = btree_multi_flags_nested;
  set.val     = *nested_id;
  ensure(btree_set(tx, &set, 0));
  return success();
}

static result_t btree_multi_search_entry(
    txn_t *tx, btree_val_t *get, multi_search_args_t *args) {
  args->key_buffer.size = get->key.size + 2;  // rec sep + pos
  ensure(txn_alloc_temp(
      tx, args->key_buffer.size, &args->key_buffer.address));
  btree_cursor_t it = {
      .tx = tx, .tree_id = get->tree_id, .key = args->key_buffer};
  memcpy(args->key_buffer.address, get->key.address, get->key.size);
  *(uint8_t *)(args->key_buffer.address + get->key.size) =
      RECORD_SEPARATOR;
  *(uint8_t *)(args->key_buffer.address + get->key.size + 1) =
      args->pos;
  ensure(btree_cursor_search(&it));  // this searches _after_ the key
  defer(btree_free_cursor, it);
  if (args->forward) {
    ensure(btree_get_next(&it));
  } else {
    ensure(btree_get_prev(&it));
  }
  if (it.has_val == false || it.key.size != args->key_buffer.size ||
      memcmp(args->key_buffer.address, it.key.address,
          args->key_buffer.size - 1)) {
    args->pos = 0;  // not a match, start from scratch
  } else if (it.flags == btree_multi_flags_nested) {
    args->nested_id = it.val;
  } else {
    args->pos = *(uint8_t *)(it.key.address + it.key.size - 1);
    args->val = it.val;
  }
  return success();
}

static result_t btree_multi_append_entry(
    txn_t *tx, btree_val_t *set, span_t *buf, uint8_t pos) {
  *(uint8_t *)(buf->address + buf->size - 1) = pos;
  btree_val_t uniq                           = {
      .tree_id = set->tree_id,
      .key     = *buf,
      .flags   = btree_multi_flags_uniquifier,
      .val     = set->val,
  };
  ensure(btree_set(tx, &uniq, 0));
  return success();
}

result_t btree_multi_append(txn_t *tx, btree_val_t *set) {
  uint8_t key_buf[10];
  multi_search_args_t args = {.forward = false, .pos = UINT8_MAX};
  ensure(btree_multi_search_entry(tx, set, &args));
  if (!args.pos && ~args.nested_id) {  // first time
    return btree_multi_append_entry(tx, set, &args.key_buffer, 1);
  }
  if (args.nested_id) {  // nested tree
    varint_encode(set->val, key_buf);
    btree_val_t nested = {.tree_id = args.nested_id,
        .key                       = {
            .address = key_buf, .size = varint_get_length(set->val)}};
    ensure(btree_set(tx, &nested, 0));
    return success();
  }

  btree_cursor_t it     = {.is_uniquifier_search = true,
      .uniquifier_cursor_pos                 = 0,
      .tree_id                               = set->tree_id,
      .tx                                    = tx,
      .key                                   = set->key};
  uint8_t available_pos = 0;
  uint8_t prev_pos      = 1;
  for (size_t i = 0; i < 16; i++) {
    ensure(btree_multi_get_next(&it));
    if (it.has_val == false) {  // done scanning
      if (!available_pos) available_pos = prev_pos + 1;
      return btree_multi_append_entry(
          tx, set, &args.key_buffer, available_pos);
    }
    if (it.val == set->val) return success();  // value already exists
    if (((it.uniquifier_cursor_pos - 1) - prev_pos) > 1) {
      available_pos = prev_pos + 1;
    }
    prev_pos = it.uniquifier_cursor_pos - 1;
  }
  {
    // scanned through 16 items, better create nested tree at this
    // point
    ensure(btree_convert_to_nested(
        tx, set->tree_id, &args.key_buffer, &args.nested_id));
    varint_encode(set->val, key_buf);
    btree_val_t nested = {.tree_id = args.nested_id,
        .key                       = {
            .address = key_buf, .size = varint_get_length(set->val)}};
    ensure(btree_set(tx, &nested, 0));
  }
  return success();
}

result_t btree_multi_cursor_search(btree_cursor_t *cursor) {
  btree_val_t get = {.tree_id = cursor->tree_id, .key = cursor->key};
  multi_search_args_t args = {.forward = false, .pos = UINT8_MAX};
  ensure(btree_multi_search_entry(cursor->tx, &get, &args));

  if (args.nested_id) {
    cursor->tree_id = args.nested_id;
    ensure(btree_cursor_at_start(cursor));
    return success();
  }
  cursor->is_uniquifier_search  = true;
  cursor->uniquifier_cursor_pos = 0;
  return success();
}

result_t btree_multi_get_next(btree_cursor_t *cursor) {
  if (cursor->is_uniquifier_search == false) {
    span_t k = cursor->key;
    ensure(btree_get_next(cursor));
    varint_decode(cursor->key.address, &cursor->val);
    cursor->key = k;
    return success();
  }

  btree_val_t get = {.tree_id = cursor->tree_id, .key = cursor->key};
  multi_search_args_t args = {
      .forward = true, .pos = cursor->uniquifier_cursor_pos};
  ensure(btree_multi_search_entry(cursor->tx, &get, &args));
  if (args.pos) {
    cursor->uniquifier_cursor_pos = args.pos + 1;  // setup next item
    cursor->val                   = args.val;
    cursor->has_val               = true;
  } else {
    cursor->has_val = false;
  }
  return success();
}

static result_t btree_drop_nested(
    txn_t *tx, uint64_t tree_id, uint64_t nested_tree_id) {
  page_metadata_t *root, *nested;
  ensure(txn_modify_metadata(tx, tree_id, &root));
  ensure(txn_modify_metadata(tx, nested_tree_id, &nested));
  if (nested->tree.next_nested) {
    page_metadata_t *next_nested;
    ensure(txn_modify_metadata(
        tx, nested->tree.next_nested, &next_nested));
    next_nested->tree.prev_nested = nested->tree.prev_nested;
  }
  if (nested->tree.prev_nested) {
    page_metadata_t *prev_nested;
    ensure(txn_modify_metadata(
        tx, nested->tree.prev_nested, &prev_nested));
    prev_nested->tree.next_nested = nested->tree.next_nested;
  }
  nested->tree.next_nested = 0;
  nested->tree.prev_nested = 0;
  ensure(btree_drop(tx, nested_tree_id));
  return success();
}

result_t btree_multi_del(txn_t *tx, btree_val_t *del) {
  multi_search_args_t args = {.forward = true};
  ensure(btree_multi_search_entry(tx, del, &args));
  if (args.nested_id) {
    btree_val_t nested = {.tree_id = args.nested_id,
        .key = {.address = &del->val, .size = sizeof(uint64_t)}};
    ensure(btree_del(tx, &nested));
    page_metadata_t *metadata;
    ensure(txn_get_metadata(tx, args.nested_id, &metadata));
    if (metadata->tree.free_space == PAGE_SIZE) {
      // empty tree, can drop nested tree and delete entry
      ensure(btree_drop_nested(tx, del->tree_id, args.nested_id));
      btree_val_t del_nested = {
          .tree_id = del->tree_id, .key = args.key_buffer};
      ensure(btree_del(tx, &del_nested));
      return success();
    }
  }
  if (args.pos != 0) {
    btree_cursor_t it = {.tree_id = del->tree_id,
        .is_uniquifier_search     = true,
        .tx                       = tx,
        .key                      = del->key};
    while (true) {
      ensure(btree_multi_get_next(&it));
      if (it.has_val == false) break;
      if (it.val == del->val) {
        *(uint8_t *)(args.key_buffer.address + args.key_buffer.size -
                     1)        = it.uniquifier_cursor_pos;
        btree_val_t del_nested = {
            .tree_id = del->tree_id, .key = args.key_buffer};
        ensure(btree_del(tx, &del_nested));
        break;
      }
    }
  }
  return success();
}