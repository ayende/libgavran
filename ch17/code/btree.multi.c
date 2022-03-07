#include <assert.h>
#include <byteswap.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

// tag::multi_search_args[]
typedef struct multi_search_args {
  span_t buf;
  uint64_t nested_id;
  uint64_t val;
  bool has_val;
  uint8_t padding[7];
} multi_search_args_t;
// end::multi_search_args[]

typedef enum __attribute__((__packed__)) btree_multi_flags {
  btree_multi_flags_uniquifier = 1,
  btree_multi_flags_nested     = 2,
} btree_multi_flags_t;

// tag::btree_create_nested[]
static result_t btree_create_nested(
    txn_t *tx, uint64_t root_tree_id, uint64_t *nested_tree_id) {
  ensure(btree_create(tx, nested_tree_id));
  page_metadata_t *root, *nested;
  ensure(txn_modify_metadata(tx, root_tree_id, &root));
  ensure(txn_modify_metadata(tx, *nested_tree_id, &nested));
  if (root->tree.nested.next) {
    page_metadata_t *nested_next;
    ensure(txn_modify_metadata(
        tx, root->tree.nested.next, &nested_next));
    nested_next->tree.nested.prev = *nested_tree_id;
  }
  nested->tree.nested.next = root->tree.nested.next;
  root->tree.nested.next   = *nested_tree_id;
  return success();
}
// end::btree_create_nested[]

// tag::btree_convert_to_nested[]
static result_t btree_convert_to_nested(
    txn_t *tx, uint64_t tree_id, span_t *buf) {
  uint64_t nested;
  ensure(btree_create_nested(tx, tree_id, &nested));
  uint8_t key_buf[10];
  btree_val_t set = {.key = {.address = key_buf}, .tree_id = nested};
  btree_val_t del = {.tree_id = tree_id};
  btree_cursor_t it = {.tree_id = tree_id, .tx = tx};
  defer(btree_free_cursor, it);
  while (true) {
    it.key = *buf;  // search first match for key
    ensure(btree_cursor_search(&it));
    ensure(btree_get_next(&it));
    if (it.has_val == false) break;
    if (it.key.size != buf->size) break;
    if (it.flags != btree_multi_flags_uniquifier) break;
    if (memcmp(it.key.address, buf->address,
            buf->size - sizeof(uint64_t)))
      break;
    uint8_t *end = varint_encode(it.val, key_buf);
    set.key.size = (size_t)(end - key_buf);
    ensure(btree_set(tx, &set, 0));  // add to nested
    del.key = it.key;
    ensure(btree_del(tx, &del));  // remove from root tree
  }
  btree_val_t set_root = {.tree_id = tree_id,
      .key                         = *buf,
      .val                         = nested,
      .flags                       = btree_multi_flags_nested};
  ensure(btree_set(tx, &set_root, 0));  // now update root
  return success();
}
// end::btree_convert_to_nested[]

// tag::btree_multi_search_entry[]
static result_t btree_multi_search_entry(
    txn_t *tx, btree_val_t *get, multi_search_args_t *args) {
  args->buf.size = get->key.size + sizeof(uint64_t);
  ensure(txn_alloc_temp(tx, args->buf.size, &args->buf.address));
  btree_cursor_t it = {
      .tx = tx, .tree_id = get->tree_id, .key = args->buf};
  memcpy(args->buf.address, get->key.address, get->key.size);
  memset(args->buf.address + args->buf.size - sizeof(uint64_t), 0,
      sizeof(uint64_t));  // key + 00000, the first possible key
  ensure(btree_cursor_search(&it));  // this searches eq or gt key
  defer(btree_free_cursor, it);
  ensure(btree_get_next(&it));
  if (it.has_val == false || it.key.size != args->buf.size ||
      memcmp(get->key.address, it.key.address, get->key.size)) {
    args->has_val = false;
  } else if (it.flags == btree_multi_flags_nested) {
    args->nested_id = it.val;
    args->has_val   = true;
  } else if (it.flags == btree_multi_flags_uniquifier &&
             it.key.size == args->buf.size) {
    args->val     = it.val;
    args->has_val = true;
  } else {
    args->has_val = false;
  }
  return success();
}
// end::btree_multi_search_entry[]

// tag::btree_convert_to_nested_if_needed[]
static result_t btree_convert_to_nested_if_needed(
    txn_t *tx, btree_val_t *set, span_t *buf) {
  size_t count = 0;
  memset(buf->address + buf->size - sizeof(uint64_t), 0,
      sizeof(uint64_t));
  btree_cursor_t it = {
      .tree_id = set->tree_id, .key = *buf, .tx = tx};
  defer(btree_free_cursor, it);
  ensure(btree_cursor_search(&it));
  while (true) {
    ensure(btree_get_next(&it));
    // check if we moved past the right key
    if (it.has_val == false) break;
    if (it.key.size != buf->size) break;
    if (it.flags != btree_multi_flags_uniquifier) break;
    if (memcmp(it.key.address, buf->address,
            buf->size - sizeof(uint64_t)))
      break;
    count++;
  }
  if (count >= 16) {  // enough items that we should move to nested
    ensure(btree_convert_to_nested(tx, set->tree_id, buf));
  }
  return success();
}
// end::btree_convert_to_nested_if_needed[]

// tag::btree_multi_append[]
result_t btree_multi_append(txn_t *tx, btree_val_t *set) {
  multi_search_args_t args = {.has_val = false};
  // <1>
  ensure(btree_multi_search_entry(tx, set, &args));
  // <2>
  if (args.nested_id) {  // nested tree
    uint8_t key_buf[10];
    varint_encode(set->val, key_buf);
    btree_val_t nested = {.tree_id = args.nested_id,
        .key                       = {
            .address = key_buf, .size = varint_get_length(set->val)}};
    ensure(btree_set(tx, &nested, 0));
    return success();
  }
  // <3>
  uint64_t rev = bswap_64(set->val);
  memcpy(args.buf.address + args.buf.size - sizeof(uint64_t), &rev,
      sizeof(uint64_t));
  btree_val_t nested = {.flags = btree_multi_flags_uniquifier,
      .tree_id                 = set->tree_id,
      .key                     = args.buf,
      .val                     = set->val};
  ensure(btree_set(tx, &nested, 0));
  // <4>
  ensure(btree_convert_to_nested_if_needed(tx, set, &args.buf));
  return success();
}
// end::btree_multi_append[]

// tag::btree_multi_cursor_search[]
result_t btree_multi_cursor_search(btree_cursor_t *cursor) {
  ensure(btree_free_cursor(cursor));
  btree_cursor_t it = {.tx = cursor->tx,
      .key     = {.size = cursor->key.size + sizeof(uint64_t)},
      .tree_id = cursor->tree_id};
  ensure(txn_alloc_temp(cursor->tx, it.key.size, &it.key.address));
  memcpy(it.key.address, cursor->key.address, cursor->key.size);
  memset(it.key.address + cursor->key.size, 0, sizeof(uint64_t));
  ensure(btree_cursor_search(&it));
  defer(btree_free_cursor, it);
  ensure(btree_get_next(&it));
  if (it.has_val == false ||
      it.key.size != cursor->key.size + sizeof(uint64_t) ||
      memcmp(cursor->key.address, it.key.address, cursor->key.size)) {
    cursor->has_val = false;
    return success();
  }
  if (it.flags == btree_multi_flags_nested) {
    cursor->tree_id = it.val;
    ensure(btree_free_cursor(&it));  // avoid concurrent cursors
    ensure(btree_cursor_at_start(cursor));
    return success();
  }
  if (it.flags != btree_multi_flags_uniquifier) {
    cursor->has_val = false;
    return success();
  }
  cursor->has_val              = true;
  cursor->is_uniquifier_search = true;
  int16_t pos;
  uint64_t page_num;
  ensure(btree_stack_pop(&it.stack, &page_num, &pos));
  ensure(btree_stack_push(&it.stack, page_num, pos - 1));
  memcpy(&cursor->stack, &it.stack, sizeof(btree_stack_t));
  memset(&it.stack, 0, sizeof(btree_stack_t));  // change cursor owner
  return success();
}
// end::btree_multi_cursor_search[]

// tag::btree_multi_get_next[]
result_t btree_multi_get_next(btree_cursor_t *cursor) {
  if (cursor->has_val == false) return success();
  span_t k = cursor->key;
  ensure(btree_get_next(cursor));
  if (cursor->has_val == false) return success();
  if (cursor->is_uniquifier_search == false) {
    varint_decode(cursor->key.address, &cursor->val);
  } else {
    if (cursor->key.size != k.size + sizeof(uint64_t) ||
        memcmp(cursor->key.address, k.address, k.size)) {
      cursor->has_val = false;
    }
  }
  cursor->key = k;
  return success();
}
// end::btree_multi_get_next[]

// tag::btree_drop_nested[]
static result_t btree_drop_nested(
    txn_t *tx, uint64_t nested_tree_id) {
  page_metadata_t *nested;
  ensure(txn_modify_metadata(tx, nested_tree_id, &nested));
  if (nested->tree.nested.next) {
    page_metadata_t *nested_next;
    ensure(txn_modify_metadata(
        tx, nested->tree.nested.next, &nested_next));
    nested_next->tree.nested.prev = nested->tree.nested.prev;
  }
  if (nested->tree.nested.prev) {
    page_metadata_t *nested_prev;
    ensure(txn_modify_metadata(
        tx, nested->tree.nested.prev, &nested_prev));
    nested_prev->tree.nested.next = nested->tree.nested.next;
  }
  nested->tree.nested.next = 0;
  nested->tree.nested.prev = 0;
  ensure(btree_drop(tx, nested_tree_id));
  return success();
}
// end::btree_drop_nested[]

// tag::btree_multi_del[]
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
    if (metadata->tree.floor) return success();
    // empty tree, can drop nested tree and delete entry
    ensure(btree_drop_nested(tx, args.nested_id));
    memset(args.buf.address + args.buf.size - sizeof(uint64_t), 0,
        sizeof(uint64_t));
  } else {
    uint64_t rev = bswap_64(del->val);
    memcpy(args.buf.address + args.buf.size - sizeof(uint64_t), &rev,
        sizeof(uint64_t));
  }
  btree_val_t del_nested = {.tree_id = del->tree_id, .key = args.buf};
  ensure(btree_del(tx, &del_nested));
  return success();
}
// end::btree_multi_del[]