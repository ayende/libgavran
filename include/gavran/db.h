#pragma once

#include <errno.h>
#include <sodium.h>
#include <stdint.h>
#include <unistd.h>

#include <gavran/infrastructure.h>
#include <gavran/pal.h>

// tag::tx_flags[]
#define TX_WRITE (1 << 1)
#define TX_READ (1 << 2)
#define TX_APPLY_LOG (1 << 3)
#define TX_COMMITED (1 << 4)
// end::tx_flags[]

#define BITS_IN_PAGE (PAGE_SIZE * 8)
#define PAGES_IN_METADATA (128UL)
#define PAGES_IN_METADATA_MASK (-128UL)

typedef struct txn txn_t;
typedef struct db_state db_state_t;
typedef struct file_header file_header_t;

#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#define MAX(a, b) (((a) > (b)) ? (a) : (b))

#define PAGE_METADATA_CRYPTO_HEADER_SIZE 32
#define PAGE_METADATA_CRYPTO_NONCE_SIZE 16

// tag::paging_api[]
#define PAGE_SIZE 8192
#define PAGE_ALIGNMENT 4096

#define ROUND_UP(size, amount) \
  MAX(((size) / amount + ((size) % amount ? 1 : 0)), 1)

#define TO_PAGES(size) ROUND_UP(size, PAGE_SIZE)

typedef struct page {
  void *address;
  void *previous;  // relevant only for modified page
  uint64_t page_num;
  uint32_t number_of_pages;
  uint32_t _padding;
} page_t;

result_t pages_get(txn_t *tx, page_t *p);
result_t pages_write(db_state_t *db, page_t *p);
// end::paging_api[]

// tag::page_crypto_metadata_t[]
typedef struct page_crypto_metadata {
  union {
    // <1>
    struct {
      uint8_t nonce[16];
      uint8_t mac[16];
    } aead;
    // <2>
    uint8_t hash_blake2b[crypto_generichash_BYTES];
  };
} page_crypto_metadata_t;
// end::page_crypto_metadata_t[]

typedef enum __attribute__((__packed__)) page_flags {
  page_flags_free              = 0,
  page_flags_file_header       = 1,
  page_flags_metadata          = 2,
  page_flags_free_space_bitmap = 3,
  page_flags_overflow          = 4,
  page_flags_hash_directory    = 5,
  page_flags_hash              = 6,
  page_flags_container         = 7,
  page_flags_tree_leaf         = 8,
  page_flags_tree_branch       = 9,
} page_flags_t;

typedef struct tree_page {
  page_flags_t page_flags;
  uint8_t padding[1];
  uint16_t floor;
  uint16_t ceiling;
  uint16_t free_space;
} tree_page_t;

typedef struct hash_page_directory {
  page_flags_t page_flags;
  uint8_t depth;
  uint8_t padding[2];
  uint32_t number_of_buckets;
  uint64_t number_of_entries;
  uint8_t padding2[16];
} hash_page_directory_t;

typedef struct hash_page {
  page_flags_t page_flags;
  uint8_t depth;
  uint16_t number_of_entries;
  uint16_t bytes_used;
  uint8_t _padding[26];
} hash_page_t;

// tag::container_page_t[]
typedef struct container_page {
  page_flags_t page_flags;
  uint8_t _padding1[1];
  uint16_t free_space;
  uint16_t floor;
  uint16_t ceiling;
  uint64_t next;
  uint64_t prev;
  uint64_t free_list;
} container_page_t;
// end::container_page_t[]

typedef struct overflow_page {
  page_flags_t page_flags;
  bool is_container_value;
  uint8_t _padding[2];
  uint32_t number_of_pages;
  uint64_t size_of_value;
  uint64_t container_item_id;
} overflow_page_t;

typedef struct free_space_bitmap_heart {
  page_flags_t page_flags;
  uint8_t _padding1[3];
  uint32_t number_of_pages;
  uint8_t _padding2[20];
} free_space_bitmap_heart_t;

// tag::file_header[]
#define FILE_HEADER_MAGIC "GVRN!"

typedef struct file_header {
  page_flags_t page_flags;
  uint8_t version;
  uint8_t page_size_power_of_two;
  uint8_t magic[5];  // should be FILE_HEADER_MAGIC
  uint64_t number_of_pages;
  uint64_t free_space_bitmap_start;
  uint64_t last_tx_id;
} file_header_t;
// end::file_header[]

// tag::page_metadata_t[]
typedef struct page_metadata_common {
  page_flags_t page_flags;
  char padding[23];
  uint64_t last_tx_id;
} page_metadata_common_t;

typedef struct page_metadata {
  page_crypto_metadata_t cyrpto;

  union {
    page_metadata_common_t common;
    file_header_t file_header;
    free_space_bitmap_heart_t free_space;
    overflow_page_t overflow;
    container_page_t container;
    hash_page_t hash;
    hash_page_directory_t hash_dir;
    tree_page_t tree;
  };
} page_metadata_t;

_Static_assert(sizeof(page_crypto_metadata_t) == 32,
    "The size of page crypto must be 32 bytes");
_Static_assert(sizeof(page_metadata_t) == 64,
    "The size of page metadata must be 64 bytes");
// end::page_metadata_t[]

// tag::tx_structs[]
typedef struct db_state db_state_t;
typedef struct txn_state txn_state_t;
typedef struct pages_hash_table pages_map_t;

typedef struct db {
  db_state_t *state;
} db_t;

typedef enum db_flags {
  db_flags_none                   = 0,
  txn_write                       = 1 << 1,
  txn_read                        = 1 << 2,
  txn_flags_apply_log             = 1 << 3,
  txn_flags_commited              = 1 << 4,
  db_flags_avoid_mmap_io          = 1 << 5,
  db_flags_encrypted              = 1 << 6,
  db_flags_page_validation_once   = 1 << 7,
  db_flags_page_validation_always = 1 << 8,
  db_flags_log_shipping_target    = 1 << 9,
  db_flags_page_validation_none =
      db_flags_page_validation_once | db_flags_page_validation_always,
  db_flags_page_validation_none_mask =
      ~(db_flags_page_validation_once |
          db_flags_page_validation_always),
  db_flags_page_need_txn_working_set =
      db_flags_encrypted | db_flags_avoid_mmap_io

} db_flags_t;

// tag::txn_t[]
typedef struct txn {
  txn_state_t *state;
  pages_map_t *working_set;
} txn_t;
// end::txn_t[]
// end::tx_structs[]

// tag::wal_write_callback_t[]
typedef void (*wal_write_callback_t)(
    void *state, uint64_t tx_id, span_t *wal_record);
// end::wal_write_callback_t[]

// tag::database_page_validation_options[]
typedef struct db_options {
  uint64_t minimum_size;
  uint64_t maximum_size;
  uint64_t wal_size;
  uint8_t encryption_key[32];
  db_flags_t flags;
  uint32_t _padding;
  wal_write_callback_t wal_write_callback;
  void *wal_write_callback_state;
} db_options_t;
// end::database_page_validation_options[]

// tag::wal_data_structs[]
typedef struct wal_file_state {
  file_handle_t *handle;
  span_t span;
  uint64_t last_write_pos;
  uint64_t last_tx_id;
} wal_file_state_t;

typedef struct wal_state {
  size_t current_append_file_index;
  wal_file_state_t files[2];
} wal_state_t;
// end::wal_data_structs[]

// tag::db_state_t[]
typedef struct db_state {
  db_options_t options;
  span_t map;
  uint64_t number_of_pages;
  uint64_t last_tx_id;
  file_handle_t *handle;
  wal_state_t wal_state;
  txn_state_t *last_write_tx;
  uint64_t active_write_tx;
  txn_state_t *default_read_tx;
  txn_state_t *transactions_to_free;
  uint64_t *first_read_bitmap;
  uint64_t original_number_of_pages;
  uint64_t oldest_active_tx;
} db_state_t;
// end::db_state_t[]

// tag::cleanup_callback_t[]
typedef struct cleanup_callback {
  void (*func)(void *state);
  struct cleanup_callback *next;
  char state[];
} cleanup_callback_t;
// end::cleanup_callback_t[]

typedef struct btree_stack {
  uint64_t *pages;
  int16_t *positions;
  uint32_t size;
  uint32_t index;  // one based indexer
} btree_stack_t;

// tag::txn_state_t[]
typedef struct txn_state {
  uint64_t tx_id;
  db_state_t *db;
  span_t map;
  uint64_t number_of_pages;
  pages_map_t *modified_pages;
  cleanup_callback_t *on_forget;
  cleanup_callback_t *on_rollback;
  txn_state_t *prev_tx;
  txn_state_t *next_tx;
  void *shipped_wal_record;
  uint64_t can_free_after_tx_id;
  btree_stack_t tmp_stack;
  uint32_t usages;
  db_flags_t flags;
} txn_state_t;
// end::txn_state_t[]

// tag::txn_api[]
result_t db_create(
    const char *filename, db_options_t *options, db_t *db);
result_t db_close(db_t *db);
enable_defer(db_close);

result_t txn_create(db_t *db, db_flags_t flags, txn_t *tx);
result_t txn_close(txn_t *tx);
enable_defer(txn_close);

result_t txn_commit(txn_t *tx);
result_t txn_raw_get_page(txn_t *tx, page_t *page);

result_t txn_raw_modify_page(txn_t *tx, page_t *page);
// end::txn_api[]

result_t txn_register_cleanup_action(cleanup_callback_t **head,
    void (*action)(void *), void *state_to_copy,
    size_t size_of_state);

result_t txn_get_page(txn_t *tx, page_t *page);
result_t txn_get_page_and_metadata(
    txn_t *tx, page_t *page, page_metadata_t **metadata);
result_t txn_modify_page(txn_t *tx, page_t *page);

// tag::tx_allocation[]
result_t txn_allocate_page(txn_t *tx, page_t *page,
    page_metadata_t **metadata, uint64_t nearby_hint);
result_t txn_free_page(txn_t *tx, page_t *page);
// end::tx_allocation[]

// tag::free_space[]
result_t txn_is_page_busy(txn_t *tx, uint64_t page_num, bool *busy);

// tag::bit-manipulations[]
static inline void bitmap_set(
    uint64_t *buffer, uint64_t pos, bool val) {
  if (val)
    buffer[pos / 64] |= (1UL << pos % 64);
  else
    buffer[pos / 64] ^= (1UL << pos % 64);
}
static inline bool bitmap_is_set(uint64_t *buffer, uint64_t pos) {
  return (buffer[pos / 64] & (1UL << pos % 64)) != 0;
}
// end::bit-manipulations[]

// tag::metadata_api[]
result_t txn_get_metadata(
    txn_t *tx, uint64_t page_num, page_metadata_t **metadata);

result_t txn_modify_metadata(
    txn_t *tx, uint64_t page_num, page_metadata_t **metadata);
// end::metadata_api[]

typedef struct reusable_buffer {
  void *address;
  size_t size;
  size_t used;
} reusable_buffer_t;

result_t wal_apply_wal_record(db_t *db, reusable_buffer_t *tmp_buffer,
    uint64_t tx_id, span_t *wal_record);

// tag::container_api[]
// create / delete container
result_t container_create(txn_t *tx, uint64_t *container_id);
result_t container_drop(txn_t *tx, uint64_t container_id);

typedef struct container_item {
  uint64_t container_id;
  uint64_t item_id;
  span_t data;
} container_item_t;

// CRUD operations
result_t container_item_put(txn_t *tx, container_item_t *item);
result_t container_item_update(
    txn_t *tx, container_item_t *item, bool *in_place);
result_t container_item_get(txn_t *tx, container_item_t *item);
result_t container_item_del(txn_t *tx, container_item_t *item);
// iteration
result_t container_get_next(txn_t *tx, container_item_t *item);
// end::container_api[]

// tag::hash_api[]
typedef struct hash_val {
  uint64_t hash_id;
  uint64_t key;
  uint64_t val;
  bool has_val;
  bool hash_id_changed;
  uint8_t padding[2];
  uint32_t iter_state;
} hash_val_t;

result_t hash_create(txn_t *tx, uint64_t *hash_id);
result_t hash_drop(txn_t *tx, uint64_t hash_id);

result_t hash_set(txn_t *tx, hash_val_t *set, hash_val_t *old);
result_t hash_get(txn_t *tx, hash_val_t *kvp);
result_t hash_del(txn_t *tx, hash_val_t *del);
result_t hash_get_next(
    txn_t *tx, pages_map_t **state, hash_val_t *it);
// end::hash_api[]

// tag::btree_api[]
typedef struct btree_val {
  uint64_t tree_id;
  span_t key;
  uint64_t val;
  int16_t position;
  int8_t last_match;
  bool has_val;
  bool tree_id_changed;
  uint8_t padding[3];
} btree_val_t;

result_t btree_create(txn_t *tx, uint64_t *tree_id);
result_t btree_drop(txn_t *tx, uint64_t tree_id);

result_t btree_set(txn_t *tx, btree_val_t *set, btree_val_t *old);
result_t btree_get(txn_t *tx, btree_val_t *kvp);
result_t btree_del(txn_t *tx, btree_val_t *del);

typedef struct btree_cursor {
  txn_t *tx;
  uint64_t tree_id;
  btree_stack_t stack;
  span_t key;
  uint64_t val;
  bool has_val;
  uint8_t padding[7];
} btree_cursor_t;

result_t btree_cursor_at_start(btree_cursor_t *cursor);
result_t btree_cursor_at_end(btree_cursor_t *cursor);
result_t btree_cursor_search(btree_cursor_t *cursor);
result_t btree_get_next(btree_cursor_t *cursor);
result_t btree_get_prev(btree_cursor_t *cursor);
result_t btree_free_cursor(btree_cursor_t *cursor);
enable_defer(btree_free_cursor);
// end::btree_api[]