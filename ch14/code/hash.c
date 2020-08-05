#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

#define HASH_BUCKET_DATA_SIZE (63)
#define HASH_OVERFLOW_CHAIN_SIZE (8)

typedef struct hash_bucket {
  bool overflowed : 1;
  uint8_t bytes_used : 7;
  uint8_t data[HASH_BUCKET_DATA_SIZE];
} hash_bucket_t;

static_assert(sizeof(hash_bucket_t) == 64, "Bad size");

#define BUCKETS_IN_PAGE (PAGE_SIZE / sizeof(hash_bucket_t))

result_t hash_create(txn_t* tx, uint64_t* hash_id) {
  page_t p = {.number_of_pages = 1};
  page_metadata_t* metadata;
  ensure(txn_allocate_page(tx, &p, &metadata, 0));
  metadata->hash.page_flags = page_flags_hash;
  *hash_id                  = p.page_num;
  return success();
}

static result_t hash_get_from_page(txn_t* tx, uint64_t page_num,
    uint64_t key, uint64_t* val, bool* exists) {
  page_t p = {.page_num = page_num};
  ensure(txn_get_page(tx, &p));
  hash_bucket_t* buckets = p.address;
  for (size_t i = 0; i < HASH_OVERFLOW_CHAIN_SIZE; i++) {
    uint64_t idx = (key + i) % BUCKETS_IN_PAGE;
    uint8_t* end = buckets[idx].data + buckets[idx].bytes_used;
    uint8_t* cur = buckets[idx].data;
    while (cur < end) {
      uint64_t k, v;
      cur = varint_decode(varint_decode(cur, &k), &v);
      if (k == key) {
        *val    = v;
        *exists = true;
        return success();
      }
    }
    if (buckets[idx].overflowed == false) break;
  }
  *exists = false;
  return success();
}

static result_t hash_append_to_page(hash_bucket_t* buckets,
    page_metadata_t* metadata, uint64_t key, uint8_t* buffer,
    size_t size, bool* added) {
  for (size_t i = 0; i < HASH_OVERFLOW_CHAIN_SIZE; i++) {
    uint64_t idx = (key + i) % BUCKETS_IN_PAGE;
    if (buckets[idx].bytes_used + size > HASH_BUCKET_DATA_SIZE) {
      buckets[idx].overflowed = true;
      continue;
    }
    memcpy(buckets[idx].data + buckets[idx].bytes_used, buffer, size);
    buckets[idx].bytes_used += size;
    metadata->hash.number_of_entries++;
    *added = true;
    return success();
  }
  *added = false;
  return success();
}

static void hash_remove_in_bucket(hash_bucket_t* bucket,
    uint8_t* start, uint8_t** end, uint8_t* cur) {
  // move other data to cover current one
  uint8_t size = (uint8_t)(cur - start);
  memmove(start, cur, (size_t)(*end - cur));
  bucket->bytes_used -= size;
  *end -= size;
  // zero the remaining bytes
  memset(bucket->data + bucket->bytes_used, 0,
      HASH_BUCKET_DATA_SIZE - bucket->bytes_used);
}

static result_t hash_set_in_page(page_t* p, page_metadata_t* metadata,
    hash_val_t* set, hash_val_t* old) {
  uint8_t buffer[20];
  uint8_t* buf_end =
      varint_encode(set->val, varint_encode(set->key, buffer));
  uint8_t size = (uint8_t)(buf_end - buffer);

  hash_bucket_t* buckets = p->address;
  set->has_val           = true;
  if (old) {
    old->has_val = false;
  }

  for (size_t i = 0; i < HASH_OVERFLOW_CHAIN_SIZE; i++) {
    uint64_t idx = (set->key + i) % BUCKETS_IN_PAGE;
    uint8_t* end = buckets[idx].data + buckets[idx].bytes_used;
    uint8_t* cur = buckets[idx].data;
    while (cur < end) {
      uint64_t k, v;
      uint8_t* start = cur;
      cur            = varint_decode(varint_decode(cur, &k), &v);
      if (k == set->key) {
        if (old) {
          old->has_val = true;
          old->key     = k;
          old->val     = v;
        }
        if (v == set->val) {
          // nothing to change
          return success();
        }
        // same size, can just overwrite
        if ((cur - start) == size) {
          memcpy(start, buffer, size);
          return success();
        }
        hash_remove_in_bucket(buckets + idx, start, &end, cur);
        metadata->hash.number_of_entries--;

        if (buckets[idx].bytes_used + size <= HASH_BUCKET_DATA_SIZE) {
          memcpy(buckets[idx].data + buckets[idx].bytes_used, buffer,
              size);
          buckets[idx].bytes_used += size;
          return success();
        }
        metadata->hash.number_of_entries--;
        // can't fit, mark as overflow and try again
        buckets[idx].overflowed = true;
      }
    }
    if (buckets[idx].overflowed == false) break;
  }
  // we now call it _knowing_ the value isn't here
  ensure(hash_append_to_page(
      buckets, metadata, set->key, buffer, size, &set->has_val));
  return success();
}

static void hash_compact_buckets(
    hash_bucket_t* buckets, uint64_t start_idx) {
  size_t max_overflow = 0;
  for (size_t i = 0; i < HASH_OVERFLOW_CHAIN_SIZE; i++) {
    uint64_t idx = (start_idx + i) % BUCKETS_IN_PAGE;
    if (!buckets[idx].overflowed) break;
    max_overflow++;
  }
  while (max_overflow) {
    uint64_t idx      = (start_idx + max_overflow) % BUCKETS_IN_PAGE;
    uint8_t* end      = buckets[idx].data + buckets[idx].bytes_used;
    uint8_t* cur      = buckets[idx].data;
    bool has_overflow = false;
    while (cur < end) {
      uint64_t k, v;
      uint8_t* start = cur;
      cur            = varint_decode(varint_decode(cur, &k), &v);
      uint64_t k_idx = k % BUCKETS_IN_PAGE;
      if (k_idx != idx) {
        // can move to the right location
        uint8_t size = (uint8_t)(cur - start);
        if (buckets[k_idx].bytes_used + size <=
            HASH_BUCKET_DATA_SIZE) {
          memcpy(buckets[k_idx].data + buckets[k_idx].bytes_used,
              start, size);
          buckets[k_idx].bytes_used += size;
          hash_remove_in_bucket(buckets + idx, start, &end, cur);

        } else {
          has_overflow = true;
        }
      }
    }
    if (has_overflow) break;

    max_overflow--;
    uint64_t prev_idx            = idx ? idx - 1 : BUCKETS_IN_PAGE;
    buckets[prev_idx].overflowed = false;
  }
}

static result_t hash_remove_from_page(
    page_t* p, page_metadata_t* metadata, hash_val_t* del) {
  hash_bucket_t* buckets = p->address;
  del->has_val           = false;
  for (size_t i = 0; i < 8; i++) {
    uint64_t idx = (del->key + i) % BUCKETS_IN_PAGE;
    uint8_t* end = buckets[idx].data + buckets[idx].bytes_used;
    uint8_t* cur = buckets[idx].data;
    while (cur < end) {
      uint64_t k, v;
      uint8_t* start = cur;
      cur            = varint_decode(varint_decode(cur, &k), &v);
      if (k == del->key) {
        del->has_val = true;
        del->val     = v;
        hash_remove_in_bucket(buckets + idx, start, &end, cur);
        metadata->hash.number_of_entries--;

        if (buckets[idx].overflowed)
          hash_compact_buckets(buckets, idx);
        return success();
      }
    }
    if (buckets[idx].overflowed == false) break;
  }
  return success();
}

result_t hash_get(txn_t* tx, hash_val_t* kvp) {
  ensure(hash_get_from_page(
      tx, kvp->hash_id, kvp->key, &kvp->val, &kvp->has_val));
  return success();
}

result_t hash_set(txn_t* tx, hash_val_t* set, hash_val_t* old) {
  page_t p = {.page_num = set->hash_id};
  page_metadata_t* metadata;
  ensure(txn_modify_page(tx, &p));
  ensure(txn_modify_metadata(tx, set->hash_id, &metadata));
  ensure(hash_set_in_page(&p, metadata, set, old));
  return success();
}

result_t hash_del(txn_t* tx, hash_val_t* del) {
  page_t p = {.page_num = del->hash_id};
  page_metadata_t* metadata;
  ensure(txn_modify_page(tx, &p));
  ensure(txn_modify_metadata(tx, del->hash_id, &metadata));
  ensure(hash_remove_from_page(&p, metadata, del));
  return success();
}