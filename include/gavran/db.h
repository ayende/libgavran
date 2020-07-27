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
#define TX_COMMITED (1 << 24)
// end::tx_flags[]

#define BITS_IN_PAGE (PAGE_SIZE * 8)
#define PAGES_IN_METADATA (128UL)
#define PAGES_IN_METADATA_MASK (-128UL)

typedef struct txn txn_t;
typedef struct db_state db_state_t;
typedef struct file_header file_header_t;

#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#define MAX(a, b) (((a) > (b)) ? (a) : (b))

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
  uint64_t number_of_pages;
} page_t;

result_t pages_get(txn_t *tx, page_t *p);
result_t pages_write(db_state_t *db, page_t *p);
// end::paging_api[]

typedef struct page_crypto_metadata {
  union {
    // <1>
    struct {
      uint8_t nonce[crypto_aead_aes256gcm_NPUBBYTES];
      uint8_t mac[crypto_aead_aes256gcm_ABYTES];
    } aes_gcm;
    // <2>
    uint8_t page_hash[crypto_generichash_BYTES];
  };
} page_crypto_metadata_t;

typedef enum __attribute__((__packed__)) page_flags {
  page_flags_free = 0,
  page_flags_file_header = 1,
  page_flags_metadata = 2,
  page_flags_free_space_bitmap = 3,
  page_flags_overflow = 4,
} page_flags_t;

typedef struct overflow_page {
  page_flags_t page_flags;
  uint8_t _padding1[7];
  uint64_t number_of_pages;
  uint64_t size_of_value;
} overflow_page_t;

typedef struct free_space_bitmap_heart {
  page_flags_t page_flags;
  uint8_t _padding1[7];
  uint64_t number_of_pages;
  uint8_t _padding2[16];
} free_space_bitmap_heart_t;

// tag::file_header[]
#define FILE_HEADER_MAGIC "GVRN!"

typedef struct file_header {
  page_flags_t page_flags;
  uint8_t version;
  uint8_t page_size_power_of_two;
  uint8_t magic[5];  // should be FILE_HEADER_MAGIC
  uint64_t number_of_pages;
  uint64_t last_tx_id;
  uint64_t free_space_bitmap_start;
} file_header_t;
// end::file_header[]

// tag::page_metadata_t[]
typedef struct page_metadata_common {
  page_flags_t page_flags;
  char padding[31];
} page_metadata_common_t;

typedef struct page_metadata {
  page_crypto_metadata_t cyrpto;

  union {
    page_metadata_common_t common;
    file_header_t file_header;
    free_space_bitmap_heart_t free_space;
    overflow_page_t overflow;
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
typedef struct pages_hash_table pages_hash_table_t;

typedef struct db {
  db_state_t *state;
} db_t;

typedef struct txn {
  txn_state_t *state;
  pages_hash_table_t *working_set;
} txn_t;
// end::tx_structs[]

// tag::database_page_validation_options[]
enum database_page_validation_options {
  page_validation_none = 0,
  page_validation_once = 1,
  page_validation_always = 2
};

typedef struct db_options {
  uint64_t minimum_size;
  uint64_t maximum_size;
  uint64_t wal_size;
  uint8_t encryption_key[crypto_aead_aes256gcm_KEYBYTES];
  uint32_t encrypted;
  enum database_page_validation_options page_validation;
  uint32_t avoid_mmap_io;
  uint32_t _padding;
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
typedef struct db_global_state {
  span_t span;
  file_header_t header;
} db_global_state_t;

typedef struct db_state {
  db_options_t options;
  db_global_state_t global_state;
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

// tag::txn_state_t[]
typedef struct txn_state {
  db_state_t *db;
  db_global_state_t global_state;
  pages_hash_table_t *modified_pages;
  cleanup_callback_t *on_forget;
  cleanup_callback_t *on_rollback;
  txn_state_t *prev_tx;
  txn_state_t *next_tx;
  uint64_t can_free_after_tx_id;
  uint32_t usages;
  uint32_t flags;
} txn_state_t;
// end::txn_state_t[]

// tag::txn_api[]
result_t db_create(const char *filename, db_options_t *options,
                   db_t *db);
result_t db_close(db_t *db);
enable_defer(db_close);

result_t txn_create(db_t *db, uint32_t flags, txn_t *tx);
result_t txn_close(txn_t *tx);
enable_defer(txn_close);

result_t txn_commit(txn_t *tx);
result_t txn_raw_get_page(txn_t *tx, page_t *page);
result_t txn_raw_modify_page(txn_t *tx, page_t *page);
// end::txn_api[]

result_t txn_register_cleanup_action(cleanup_callback_t **head,
                                     void (*action)(void *),
                                     void *state_to_copy,
                                     size_t size_of_state);

result_t txn_get_page(txn_t *tx, page_t *page);
result_t txn_modify_page(txn_t *tx, page_t *page);

// tag::tx_allocation[]
result_t txn_allocate_page(txn_t *tx, page_t *page,
                           page_metadata_t **metadata,
                           uint64_t nearby_hint);
result_t txn_free_page(txn_t *tx, page_t *page);
// end::tx_allocation[]

// tag::free_space[]
result_t txn_is_page_busy(txn_t *tx, uint64_t page_num, bool *busy);

// tag::bit-manipulations[]
static inline void bitmap_set(uint64_t *buffer, uint64_t pos,
                              bool val) {
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
result_t txn_get_metadata(txn_t *tx, uint64_t page_num,
                          page_metadata_t **metadata);

result_t txn_modify_metadata(txn_t *tx, uint64_t page_num,
                             page_metadata_t **metadata);
// end::metadata_api[]
