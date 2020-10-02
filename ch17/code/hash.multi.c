#include <assert.h>
#include <gavran/db.h>
#include <gavran/internal.h>

#include <string.h>

typedef enum __attribute__((__packed__)) hash_multi_flags {
  hash_multi_single = 1,
  hash_multi_packed = 2,
  hash_multi_nested = 3
} hash_multi_flags_t;

// tag::hash_multi_set_single[]
static result_t hash_multi_set_single(txn_t *tx, hash_val_t *set,
    hash_val_t *existing, uint64_t container_id) {
  uint8_t buf[20];  // convert to a packed instance
  uint8_t *end =
      varint_encode(set->val, varint_encode(existing->val, buf));
  container_item_t item = {.container_id = container_id,
      .data = {.address = buf, .size = (size_t)(end - buf)}};
  ensure(container_item_put(tx, &item));
  existing->flags = hash_multi_packed;
  existing->val   = item.item_id;
  ensure(hash_set(tx, existing, 0));
  return success();
}
// end::hash_multi_set_single[]

// tag::hash_drop_nested[]
static result_t hash_drop_nested(txn_t *tx, uint64_t nested_hash_id) {
  page_metadata_t *nested;
  ensure(txn_modify_metadata(tx, nested_hash_id, &nested));
  assert(nested->common.page_flags == page_flags_hash);  // empty now
  if (nested->hash.nested.next) {
    page_metadata_t *nested_next;
    ensure(txn_modify_metadata(
        tx, nested->hash.nested.next, &nested_next));
    if (nested_next->common.page_flags == page_flags_hash) {
      nested_next->hash.nested.prev = nested->hash.nested.prev;
    } else if (nested_next->common.page_flags ==
               page_flags_hash_directory) {
      nested_next->hash_dir.nested.prev = nested->hash.nested.prev;
    } else {
      failed(EINVAL, msg("Bad page flag"));
    }
  }
  if (nested->tree.nested.prev) {
    page_metadata_t *nested_prev;
    ensure(txn_modify_metadata(
        tx, nested->hash.nested.prev, &nested_prev));
    if (nested_prev->common.page_flags == page_flags_hash) {
      nested_prev->hash.nested.next = nested->hash.nested.next;
    } else if (nested_prev->common.page_flags ==
               page_flags_hash_directory) {
      nested_prev->hash_dir.nested.next = nested->hash.nested.next;
    } else {
      failed(EINVAL, msg("Bad page flag"));
    }
  }
  nested->tree.nested.next = 0;
  nested->tree.nested.prev = 0;
  ensure(hash_drop(tx, nested_hash_id));
  return success();
}
// end::hash_drop_nested[]

// tag::hash_multi_write_nested_hash[]
static result_t hash_multi_write_nested_hash(
    txn_t *tx, uint64_t root, uint64_t nested) {
  page_metadata_t *root_metadata, *nested_metadata;
  ensure(txn_modify_metadata(tx, root, &root_metadata));
  ensure(txn_modify_metadata(tx, nested, &nested_metadata));
  nested_list_t *l;
  if (root_metadata->common.page_flags == page_flags_hash) {
    l = &root_metadata->hash.nested;
  } else if (root_metadata->common.page_flags == page_flags_hash) {
    l = &root_metadata->hash_dir.nested;
  } else {
    failed(EINVAL, msg("Unexpected page flags"));
  }
  nested_metadata->hash_dir.nested.next =
      root_metadata->hash_dir.nested.next;
  root_metadata->hash_dir.nested.next = nested;
  if (nested_metadata->hash_dir.nested.next) {
    page_metadata_t *next;
    ensure(txn_modify_metadata(
        tx, nested_metadata->hash_dir.nested.next, &next));
    next->hash_dir.nested.prev = nested;
  }
  return success();
}
// end::hash_multi_write_nested_hash[]

// tag::hash_multi_set_convert_to_nested[]
static result_t hash_multi_set_convert_to_nested(
    txn_t *tx, hash_val_t *set, container_item_t *item) {
  uint64_t nested_hash;
  ensure(hash_create(tx, &nested_hash));
  hash_val_t nested = {.hash_id = nested_hash, .key = set->val};
  ensure(hash_set(tx, &nested, 0));
  void *start = item->data.address;
  void *end   = start + item->data.size;
  while (start < end) {
    start = varint_decode(start, &nested.key);
    ensure(hash_set(tx, &nested, 0));
  }
  uint64_t old_val = set->val;
  set->val         = nested.hash_id;
  set->flags       = hash_multi_nested;
  ensure(hash_set(tx, set, 0));
  set->val = old_val;
  ensure(container_item_del(tx, item));
  ensure(
      hash_multi_write_nested_hash(tx, set->hash_id, nested.hash_id));
  return success();
}
// end::hash_multi_set_convert_to_nested[]

// tag::hash_multi_set_packed[]
static result_t hash_multi_set_packed(txn_t *tx, hash_val_t *set,
    hash_val_t *existing, uint64_t container_id) {
  container_item_t item = {
      .container_id = container_id, .item_id = existing->val};
  ensure(container_item_get(tx, &item));
  void *end   = item.data.address + item.data.size;
  void *start = item.data.address;
  while (start < end) {
    uint64_t v;
    start = varint_decode(start, &v);
    if (v == set->val) return success();  // value already exists
  }
  span_t new_val = {
      .size = item.data.size + varint_get_length(set->val)};
  if (new_val.size > 128) {
    return hash_multi_set_convert_to_nested(tx, set, &item);
  }
  ensure(txn_alloc_temp(tx, new_val.size, &new_val.address));
  memcpy(new_val.address, item.data.address, item.data.size);
  varint_encode(set->val, new_val.address + item.data.size);
  item.data = new_val;
  bool in_place;
  ensure(container_item_update(tx, &item, &in_place));
  if (in_place == false) {
    uint64_t old_val = set->val;
    set->val         = item.item_id;
    set->flags       = hash_multi_packed;
    ensure(hash_set(tx, set, 0));
    set->val = old_val;
  }
  return success();
}
// end::hash_multi_set_packed[]

static result_t hash_multi_set_nested(
    txn_t *tx, hash_val_t *set, uint64_t nested_hash_id) {
  hash_val_t nested = {.hash_id = nested_hash_id, .key = set->val};
  ensure(hash_set(tx, &nested, 0));
  return success();
}

// tag::hash_multi_append[]
result_t hash_multi_append(
    txn_t *tx, hash_val_t *set, uint64_t container_id) {
  hash_val_t existing = {.hash_id = set->hash_id, .key = set->key};
  ensure(hash_get(tx, &existing));
  // <1>
  if (existing.has_val == false) {  // new val
    set->flags = hash_multi_single;
    ensure(hash_set(tx, set, 0));
    return success();
  }
  switch (existing.flags) {
    case hash_multi_single:
      // <2>
      if (existing.val == set->val) return success();  // no change
      ensure(hash_multi_set_single(tx, set, &existing, container_id));
      return success();
    case hash_multi_packed:
      // <3>
      ensure(hash_multi_set_packed(tx, set, &existing, container_id));
      return success();
    case hash_multi_nested:
      // <4>
      ensure(hash_multi_set_nested(tx, set, existing.val));
      return success();
    default:
      failed(EINVAL, msg("Unknown flag type"));
  }
  return success();
}
// end::hash_multi_append[]

static result_t hash_multi_del_packed(txn_t *tx, hash_val_t *del,
    hash_val_t *existing, uint64_t container_id) {
  container_item_t item = {
      .container_id = container_id, .item_id = existing->val};
  ensure(container_item_get(tx, &item));
  void *end   = item.data.address + item.data.size;
  void *start = item.data.address;
  while (start < end) {
    uint64_t v;
    void *cur = varint_decode(start, &v);
    if (v == del->val) {  // found the value
      span_t new_val = {.size = (size_t)(start - item.data.address)};
      ensure(txn_alloc_temp(tx, item.data.size, &new_val.address));
      memcpy(new_val.address, item.data.address, new_val.size);
      memcpy(
          new_val.address + new_val.size, cur, (size_t)(end - cur));
      new_val.size += (size_t)(end - cur);
      if (new_val.size == 0) {  // completely empty
        ensure(container_item_del(tx, &item));
        ensure(hash_del(tx, del));
        return success();
      }
      item.data = new_val;
      bool in_place;
      ensure(container_item_update(tx, &item, &in_place));
      if (in_place == false) {
        existing->val = item.item_id;
        ensure(hash_set(tx, existing, 0));
        del->hash_id = existing->hash_id;
      }
      return success();
    }
    start = cur;
  }
  return success();  // value not found, no change
}

result_t hash_multi_del(
    txn_t *tx, hash_val_t *del, uint64_t container_id) {
  hash_val_t existing = {.hash_id = del->hash_id, .key = del->key};
  ensure(hash_get(tx, &existing));
  if (existing.has_val == false) return success();  // already gone
  switch (existing.flags) {
    case hash_multi_single:  // just delete normally
      ensure(hash_del(tx, del));
      return success();
    case hash_multi_packed:
      ensure(hash_multi_del_packed(tx, del, &existing, container_id));
      return success();
    case hash_multi_nested:
      existing.hash_id = existing.val;
      existing.key     = del->val;
      ensure(hash_del(tx, &existing));
      page_metadata_t *metadata;
      ensure(txn_get_metadata(tx, existing.hash_id, &metadata));
      if (metadata->common.page_flags == page_flags_hash &&
          metadata->hash.number_of_entries == 0) {  // empty, remove
        ensure(hash_drop_nested(tx, existing.hash_id));
        ensure(hash_del(tx, del));
        return success();
      }
      return success();
    default:
      failed(EINVAL, msg("Unknown flag type"));
  }
}

static result_t hash_multi_get_next_packed(txn_t *tx,
    uint64_t item_id, hash_val_t *it, uint64_t container_id) {
  container_item_t item = {
      .container_id = container_id, .item_id = item_id};
  ensure(container_item_get(tx, &item));
  if (it->iter_state.pos_in_page >= item.data.size) {
    it->has_val = false;
    return success();
  }
  void *end = varint_decode(
      item.data.address + it->iter_state.pos_in_page, &it->val);
  it->iter_state.pos_in_page = (uint16_t)(end - item.data.address);
  return success();
}

result_t hash_multi_get_next(txn_t *tx, pages_map_t **state,
    hash_val_t *it, uint64_t container_id) {
  if (it->iter_state.iterating_nested) {
    uint64_t k = it->key;
    ensure(hash_get_next(tx, state, it));
    it->val = it->key;
    it->key = k;
    return success();
  }
  hash_val_t existing = {.hash_id = it->hash_id, .key = it->key};
  ensure(hash_get(tx, &existing));
  if (existing.has_val == false) {
    it->has_val = false;
    return success();
  }
  it->has_val = true;
  switch (existing.flags) {
    case hash_multi_single:  // just delete normally
      if (it->iter_state.pos_in_page != 0) {
        it->has_val = false;
        return success();
      }
      it->iter_state.pos_in_page++;
      return success();
    case hash_multi_packed:
      ensure(hash_multi_get_next_packed(
          tx, existing.val, it, container_id));
      return success();
    case hash_multi_nested:
      it->hash_id                     = existing.val;
      it->iter_state.iterating_nested = true;
      ensure(hash_multi_get_next(tx, state, it, container_id));
      return success();
    default:
      failed(EINVAL, msg("Unknown flag type"));
  }
}