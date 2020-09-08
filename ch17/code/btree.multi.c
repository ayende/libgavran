#include <assert.h>
#include <byteswap.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

#define RECORD_SEPARATOR 30  // ASCII dedicated value

typedef struct multi_search_args {
  span_t key_buffer;
  uint64_t nested_id;
  uint64_t val;
  bool has_val;
  uint8_t padding[7];
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
  ensure(btree_create_nested(tx, tree_id, nested_id));
  uint8_t key_buf[10];
  btree_val_t set = {
      .key = {.address = key_buf, .size = 0}, .tree_id = *nested_id};
  btree_val_t del   = {.tree_id = tree_id};
  btree_cursor_t it = {.tree_id = tree_id, .tx = tx};
  defer(btree_free_cursor, it);
  while (true) {
    memset(buf->address + buf->size - sizeof(uint64_t), 0,
        sizeof(uint64_t));
    it.key = *buf;  // search first match for key
    ensure(btree_cursor_search(&it));
    ensure(btree_get_next(&it));
    if (it.has_val == false) break;
    if (it.key.size != buf->size) break;
    if (memcmp(it.key.address, buf->address,
            buf->size - sizeof(uint64_t)))
      break;
    uint8_t *end = varint_encode(it.val, key_buf);
    set.key.size = (size_t)(end - key_buf);
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
  args->key_buffer.size = get->key.size + 9;  // max used buffer
  ensure(txn_alloc_temp(
      tx, args->key_buffer.size, &args->key_buffer.address));
  btree_cursor_t it = {
      .tx = tx, .tree_id = get->tree_id, .key = args->key_buffer};
  memcpy(args->key_buffer.address, get->key.address, get->key.size);
  *(uint8_t *)(args->key_buffer.address + get->key.size) =
      RECORD_SEPARATOR;
  memset(args->key_buffer.address + args->key_buffer.size -
             sizeof(uint64_t),
      0, sizeof(uint64_t));
  ensure(btree_cursor_search(&it));  // this searches _after_ the key
  defer(btree_free_cursor, it);
  ensure(btree_get_next(&it));
  if (it.has_val == false ||
      memcmp(args->key_buffer.address, it.key.address,
          args->key_buffer.size - sizeof(uint64_t))) {
    args->has_val = false;
  } else if (it.flags == btree_multi_flags_nested) {
    args->nested_id = it.val;
    args->has_val   = true;
  } else {
    args->val     = it.val;
    args->has_val = true;
  }
  return success();
}

result_t btree_multi_append(txn_t *tx, btree_val_t *set) {
  multi_search_args_t args = {.has_val = false};
  ensure(btree_multi_search_entry(tx, set, &args));
  if (args.nested_id) {  // nested tree
    uint8_t key_buf[10];
    varint_encode(set->val, key_buf);
    btree_val_t nested = {.tree_id = args.nested_id,
        .key                       = {
            .address = key_buf, .size = varint_get_length(set->val)}};
    ensure(btree_set(tx, &nested, 0));
    return success();
  }
  uint64_t rev = bswap_64(set->val);
  memcpy(args.key_buffer.address + args.key_buffer.size -
             sizeof(uint64_t),
      &rev, sizeof(uint64_t));
  btree_val_t nested = {.tree_id = set->tree_id,
      .key                       = args.key_buffer,
      .val                       = set->val};
  ensure(btree_set(tx, &nested, 0));

  size_t count = 0;
  memset(args.key_buffer.address + args.key_buffer.size -
             sizeof(uint64_t),
      0, sizeof(uint64_t));
  btree_cursor_t it = {
      .tree_id = set->tree_id, .key = args.key_buffer, .tx = tx};
  defer(btree_free_cursor, it);
  ensure(btree_cursor_search(&it));
  while (true) {
    ensure(btree_get_next(&it));
    // check if we moved past the right key
    if (it.has_val == false) break;
    if (it.key.size != args.key_buffer.size) break;
    if (memcmp(it.key.address, args.key_buffer.address,
            args.key_buffer.size - sizeof(uint64_t)))
      break;
    count++;
  }

  if (count >= 16) {  // enough items that we should move to nested
    ensure(btree_convert_to_nested(
        tx, set->tree_id, &args.key_buffer, &args.nested_id));
  }
  return success();
}

result_t btree_multi_cursor_search(btree_cursor_t *cursor) {
  span_t buf = {.size = cursor->key.size + 1 + sizeof(uint64_t)};
  ensure(txn_alloc_temp(cursor->tx, buf.size, &buf.address));
  memcpy(buf.address, cursor->key.address, cursor->key.size);
  *(uint8_t *)(buf.address + cursor->key.size) = RECORD_SEPARATOR;
  memset(
      buf.address + buf.size - sizeof(uint64_t), 0, sizeof(uint64_t));
  cursor->key = buf;
  ensure(btree_cursor_search(cursor));
  ensure(btree_get_next(cursor));
  if (cursor->has_val == false) return success();
  if (cursor->key.size != buf.size ||
      memcmp(cursor->key.address, buf.address,
          buf.size - sizeof(uint64_t))) {
    cursor->has_val = false;
    return success();
  }
  if (cursor->flags == btree_multi_flags_nested) {
    cursor->tree_id = cursor->val;
    ensure(btree_cursor_at_start(cursor));
    return success();
  }
  cursor->is_uniquifier_search = true;
  int16_t pos;
  uint64_t page_num;
  ensure(btree_stack_pop(&cursor->stack, &page_num, &pos));
  ensure(btree_stack_push(&cursor->stack, page_num, pos - 1));
  return success();
}

result_t btree_multi_get_next(btree_cursor_t *cursor) {
  span_t k = cursor->key;
  ensure(btree_get_next(cursor));
  if (cursor->is_uniquifier_search == false) {
    varint_decode(cursor->key.address, &cursor->val);
  } else if (cursor->has_val) {
    if (cursor->key.size != k.size ||
        memcmp(cursor->key.address, k.address,
            k.size - sizeof(uint64_t))) {
      cursor->has_val = false;
    }
  }
  cursor->key = k;
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
  multi_search_args_t args = {.has_val = false};
  ensure(btree_multi_search_entry(tx, del, &args));
  if (args.has_val == false) return success();

  if (args.nested_id) {
    uint8_t key_buf[10];
    varint_encode(del->val, key_buf);
    btree_val_t nested = {.tree_id = args.nested_id,
        .key                       = {
            .address = key_buf, .size = varint_get_length(del->val)}};
    ensure(btree_del(tx, &nested));
    page_metadata_t *metadata;
    ensure(txn_get_metadata(tx, args.nested_id, &metadata));
    if (metadata->tree.free_space < PAGE_SIZE) return success();
    // empty tree, can drop nested tree and delete entry
    ensure(btree_drop_nested(tx, del->tree_id, args.nested_id));
  } else {
    uint64_t rev = bswap_64(del->val);
    memcpy(args.key_buffer.address + args.key_buffer.size -
               sizeof(uint64_t),
        &rev, sizeof(uint64_t));
  }
  btree_val_t del_nested = {
      .tree_id = del->tree_id, .key = args.key_buffer};
  ensure(btree_del(tx, &del_nested));
  return success();
}