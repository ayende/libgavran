#pragma once

#include <errno.h>
#include <sodium.h>
#include <stdint.h>
#include <unistd.h>

#include "defer.h"
#include "errors.h"

// tag::tx_flags[]
// <1>
#define TX_WRITE (1 << 1)
#define TX_READ (1 << 2)
#define TX_COMMITED (1 << 24)
// end::tx_flags[]

#define PAGE_SIZE 8192
#define PAGE_ALIGNMENT 4096

#define size_to_pages(size) ((size) / PAGE_SIZE + ((size) % PAGE_SIZE ? 1 : 0))

typedef struct page_metadata page_metadata_t;

// tag::paging_api[]
typedef struct page {
  void *address;
  uint64_t page_num;
  uint32_t overflow_size;
  uint32_t _padding;
  void *previous; // relevant only for modified page
} page_t;
// end::paging_api[]

// tag::file_header[]
// <1>
typedef struct __attribute__((__packed__)) file_header {
  uint8_t version;
  uint8_t page_size_power_of_two;
  uint64_t number_of_pages;
  uint64_t last_tx_id;
  uint64_t free_space_bitmap_start;
} file_header_t;
// end::file_header[]

// tag::tx_api[]
// <1>
typedef struct database_state db_state_t;
typedef struct transaction_state txn_state_t;
typedef struct pages_hash_table pages_hash_table_t;

// <2>
typedef struct database {
  db_state_t *state;
} db_t;

// tag::txn_t[]
typedef struct transaction {
  txn_state_t *state;
  pages_hash_table_t *working_set;
} txn_t;
// end::txn_t[]

// tag::database_page_validation_options[]
enum database_page_validation_options {
  page_validation_none = 0,
  page_validation_once = 1,
  page_validation_always = 2
};

typedef struct database_options {
  uint64_t minimum_size;
  uint64_t maximum_size;
  uint64_t wal_size;
  uint8_t encryption_key[crypto_aead_aes256gcm_KEYBYTES];
  uint32_t encrypted;
  enum database_page_validation_options page_validation;
} database_options_t;

// end::database_page_validation_options[]

// <4>
result_t db_create(const char *filename, database_options_t *options, db_t *db);

result_t db_close(db_t *db);
// <5>
enable_defer(db_close);

// <6>
result_t txn_create(db_t *db, uint32_t flags, txn_t *tx);

result_t txn_close(txn_t *tx);
// <7>
enable_defer(txn_close);

result_t txn_commit(txn_t *tx);

// <8>
result_t txn_get_page(txn_t *tx, page_t *page);
result_t txn_modify_page(txn_t *tx, page_t *page);
// end::tx_api[]

// tag::new_tx_api[]
result_t txn_free_page(txn_t *tx, page_t *page);
result_t txn_allocate_page(txn_t *tx, page_t *page, uint64_t nearby_hint);
// end::new_tx_api[]

// tag::free_space[]
result_t txn_page_busy(txn_t *tx, uint64_t page_num, bool *busy);

static inline void set_bit(uint64_t *buffer, uint64_t pos) {
  buffer[pos / 64] |= (1UL << pos % 64);
}

static inline bool is_bit_set(uint64_t *buffer, uint64_t pos) {
  return (buffer[pos / 64] & (1UL << pos % 64)) != 0;
}

static inline void clear_bit(uint64_t *buffer, uint64_t pos) {
  buffer[pos / 64] ^= (1UL << pos % 64);
}
// end::free_space[]

// tag::page_metadata[]
result_t txn_get_metadata(txn_t *tx, uint64_t page_num,
                          page_metadata_t **metadata);

result_t txn_modify_metadata(txn_t *tx, uint64_t page_num,
                             page_metadata_t **metadata);
// end::page_metadata[]
