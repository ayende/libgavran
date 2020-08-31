#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

static index_type_t root_types[2] = {
    index_type_container, index_type_btree};
static uint64_t root_ids[2] = {2, 4};

table_schema_t table_root_schema() {
  table_schema_t root = {.name = "root",
      .count                   = 2,
      .index_ids               = root_ids,
      .types                   = root_types};
  return root;
}

result_t table_create_anonymous(txn_t *tx, table_schema_t *schema) {
  ensure(schema->count > 1);
  ensure(schema->types[0] == index_type_container);
  ensure(container_create(tx, &schema->index_ids[0]));
  for (size_t i = 1; i < schema->count; i++) {
    switch (schema->types[i]) {
      case index_type_btree:
        ensure(btree_create(tx, &schema->index_ids[i]));
        break;
      case index_type_hash:
        ensure(hash_create(tx, &schema->index_ids[i]));
        break;
      case index_type_container:
        failed(EINVAL,
            msg("container must appear only as the first element"));
      default:
        failed(EINVAL, msg("Uknown index type"));
    }
  }
  return success();
}

result_t table_drop_anonymous(txn_t *tx, table_schema_t *schema) {
  ensure(schema->count > 0);
  ensure(schema->types[0] == index_type_container);

  ensure(container_drop(tx, schema->index_ids[0]));

  for (size_t i = 1; i < schema->count; i++) {
    switch (schema->types[i]) {
      case index_type_btree:
        ensure(btree_drop(tx, schema->index_ids[i]));
        break;
      case index_type_hash:
        ensure(hash_drop(tx, schema->index_ids[i]));
        break;
      case index_type_container:
        failed(EINVAL,
            msg("container must appear only as the first element"));
      default:
        failed(EINVAL, msg("Uknown index type"));
    }
  }
  return success();
}

result_t table_create(txn_t *tx, table_schema_t *schema) {
  ensure(table_create_anonymous(tx, schema));

  size_t name_len = strlen(schema->name) + 1;
  size_t size =
      name_len + sizeof(uint16_t) +
      schema->count * (sizeof(index_type_t) + sizeof(uint64_t));
  void *buffer;
  ensure(mem_calloc(&buffer, size));
  defer(free, buffer);
  span_t entries[2] = {{.address = buffer, .size = size},
      {.address = schema->name, .size = name_len}};
  void *cur         = buffer;
  memcpy(cur, &schema->count, sizeof(uint16_t));
  cur += sizeof(uint16_t);
  memcpy(cur, schema->types, sizeof(index_type_t) * schema->count);
  cur += sizeof(index_type_t) * schema->count;
  memcpy(cur, schema->index_ids, sizeof(uint64_t) * schema->count);
  cur += sizeof(uint64_t) * schema->count;
  memcpy(cur, schema->name, name_len);

  table_schema_t root = table_root_schema();
  table_item_t item   = {
      .entries = entries, .number_of_entries = 2, .schema = &root};
  ensure(table_set(tx, &item));

  return success();
}

result_t table_get_schema(
    txn_t *tx, char *table_name, table_schema_t *schema) {
  table_schema_t root = table_root_schema();
  btree_val_t kvp     = {
      .key = {.address = table_name, .size = strlen(table_name) + 1},
      .tree_id = root.index_ids[1]};
  ensure(btree_get(tx, &kvp));
  if (kvp.has_val == false) {
    memset(schema, 0, sizeof(table_schema_t));
    return success();
  }
  container_item_t item = {
      .container_id = root.index_ids[0], .item_id = kvp.val};
  ensure(container_item_get(tx, &item));
  memcpy(&schema->count, item.data.address, sizeof(uint16_t));
  schema->types     = item.data.address + sizeof(uint16_t);
  schema->index_ids = item.data.address + sizeof(uint16_t) +
                      (sizeof(index_type_t) * schema->count);
  return success();
}

// taken from: https://sites.google.com/site/murmurhash/
static uint64_t MurmurHash64A(
    const void *key, size_t len, unsigned int seed) {
  const uint64_t m     = 0xc6a4a7935bd1e995;
  const int r          = 47;
  uint64_t h           = seed ^ (len * m);
  const uint64_t *data = (const uint64_t *)key;
  const uint64_t *end  = data + (len / 8);

  while (data != end) {
    uint64_t k;
    memcpy(&k, data++, sizeof(uint64_t));

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const unsigned char *data2 = (const unsigned char *)data;

  switch (len & 7) {
    case 7:
      h ^= (uint64_t)data2[6] << 48;
    case 6:
      h ^= (uint64_t)data2[5] << 40;
    case 5:
      h ^= (uint64_t)data2[4] << 32;
    case 4:
      h ^= (uint64_t)data2[3] << 24;
    case 3:
      h ^= (uint64_t)data2[2] << 16;
    case 2:
      h ^= (uint64_t)data2[1] << 8;
    case 1:
      h ^= (uint64_t)data2[0];
      h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

static uint64_t table_compute_hash_for(span_t *entry) {
  if (entry->size <= 8) {
    uint64_t i = 0;
    memcpy(&i, entry->address, entry->size);
    return i;
  }
  return MurmurHash64A(entry->address, entry->size, 0);
}

static result_t table_ensure_item(table_item_t *item) {
  ensure(item->number_of_entries == item->schema->count);
  ensure(item->schema->count > 0);
  ensure(item->schema->types[0] == index_type_container);
  return success();
}

result_t table_set(txn_t *tx, table_item_t *item) {
  ensure(table_ensure_item(item));

  container_item_t c_item = {
      .container_id = item->schema->index_ids[0],
      .data         = item->entries[0]};
  ensure(container_item_put(tx, &c_item));

  for (size_t i = 1; i < item->number_of_entries; i++) {
    switch (item->schema->types[i]) {
      case index_type_btree: {
        btree_val_t set = {
            .key     = item->entries[i],
            .tree_id = item->schema->index_ids[i],
            .val     = c_item.item_id,
        };
        btree_val_t old = {0};
        ensure(btree_set(tx, &set, &old));
        ensure(old.has_val == false, msg("Duplicate value"));
        break;
      }
      case index_type_hash: {
        ensure(item->entries[i].size < 512);
        uint8_t buffer[10 /*item_id*/ + 3 /* entry_size*/ +
                       512 /*entry_bytes*/ +
                       10 /*next_id*/];  // max size of entry
        uint8_t *end = varint_encode(item->entries[i].size,
            varint_encode(c_item.item_id, buffer));
        memcpy(end, item->entries[i].address, item->entries[i].size);
        end += item->entries[i].size;

        hash_val_t get = {.hash_id = item->schema->index_ids[i],
            .key = table_compute_hash_for(&item->entries[i])};
        ensure(hash_get(tx, &get));
        end = varint_encode(get.has_val ? get.val : 0, end);
        container_item_t ref = {
            .container_id = item->schema->index_ids[0],
            .data         = {
                .address = buffer, .size = (size_t)(end - buffer)}};
        ensure(container_item_put(tx, &ref));
        hash_val_t set = {
            .key = table_compute_hash_for(&item->entries[i]),
            .val = ref.item_id};
        ensure(hash_set(tx, &set, 0));
        break;
      }
      case index_type_container:
        failed(EINVAL,
            msg("container must appear only as the first element"));
      default:
        failed(EINVAL, msg("Uknown index type"));
    }
  }
  return success();
}

result_t table_del(txn_t *tx, table_item_t *item) {
  ensure(table_ensure_item(item));

  container_item_t c_item = {
      .container_id = item->schema->index_ids[0],
      .item_id      = item->item_id};
  ensure(container_item_del(tx, &c_item));

  for (size_t i = 1; i < item->number_of_entries; i++) {
    switch (item->schema->types[i]) {
      case index_type_btree: {
        btree_val_t del = {
            .key     = item->entries[i],
            .tree_id = item->schema->index_ids[i],
        };
        ensure(btree_del(tx, &del));
        break;
      }
      case index_type_hash: {
        hash_val_t del = {
            .key = table_compute_hash_for(&item->entries[i])};
        ensure(hash_del(tx, &del));
        break;
      }
      case index_type_container:
        failed(EINVAL,
            msg("container must appear only as the first element"));
      default:
        failed(EINVAL, msg("Uknown index type"));
    }
  }
  return success();
}

result_t table_get(txn_t *tx, table_item_t *item) {
  ensure(table_ensure_item(item));
  switch (item->schema->types[item->index_to_use]) {
    case index_type_container:
    get_from_container : {
      container_item_t ci = {
          .container_id = item->schema->index_ids[0],
          .item_id      = item->item_id};
      ensure(container_item_get(tx, &ci));
      item->result = ci.data;
      return success();
    }
    case index_type_btree: {
      btree_val_t kvp = {
          .tree_id = item->schema->index_ids[item->index_to_use],
          .key     = item->entries[0]};
      ensure(btree_get(tx, &kvp));
      if (kvp.has_val == false) goto no_entry_found;
      item->item_id = kvp.val;
      goto get_from_container;
    }
    case index_type_hash: {
      hash_val_t get = {
          .hash_id = item->schema->index_ids[item->index_to_use],
          .key     = table_compute_hash_for(item->entries)};
      ensure(hash_get(tx, &get));
      while (true) {
        if (get.has_val == false) goto no_entry_found;
        container_item_t ref = {.item_id = get.val,
            .container_id = item->schema->index_ids[0]};
        ensure(container_item_get(tx, &ref));
        item->result.address = varint_decode(
            varint_decode(ref.data.address, &item->item_id),
            &item->result.size);
        if (item->entries->size == item->result.size &&
            memcmp(item->entries->address, item->result.address,
                item->entries->size) == 0) {
          goto get_from_container;  // found match
        }
        if (item->result.address + item->result.size ==
            ref.data.address + ref.data.size) {  // end of data
          get.has_val = false;
        } else {
          varint_decode(
              item->result.address + item->result.size, &get.val);
        }
      }
    }
    default:
      failed(EINVAL, msg("Uknown index type"));
  }
no_entry_found:
  memset(&item->result, 0, sizeof(span_t));
  return success();
}