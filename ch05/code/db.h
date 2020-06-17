#pragma once

#include <stdint.h>
#include <unistd.h>

#include "defer.h"
#include "errors.h"

// tag::paging_api[]
#define PAGE_SIZE 8192
#define PAGE_ALIGNMENT 4096

typedef struct page {
  void* address;
  uint64_t page_num;
  size_t num_of_pages;
} page_t;
// end::paging_api[]

// tag::file_header[]
#define FILE_HEADER_MAGIC (9410039042695495)  // = 'Gavran!\0'

typedef struct file_header {
  uint64_t magic;
  uint64_t number_of_pages;
  uint32_t version;
  uint32_t page_size;
  uint64_t free_space_bitmap_start;
  uint64_t free_space_bitmap_in_pages;
} file_header_t;
// end::file_header[]

// tag::tx_api[]
// <1>
typedef struct database_state db_state_t;
typedef struct transaction_state txn_state_t;

// <2>
typedef struct database {
  db_state_t* state;
} db_t;

typedef struct transaction {
  txn_state_t* state;
} txn_t;

// <3>
typedef struct database_options {
  uint64_t minimum_size;
} database_options_t;

// <4>
result_t db_create(const char* filename, database_options_t* options,
                   db_t* db);

result_t db_close(db_t* db);
// <5>
enable_defer(db_close);

// <6>
result_t txn_create(db_t* db, uint32_t flags, txn_t* tx);

result_t txn_close(txn_t* tx);
// <7>
enable_defer(txn_close);

result_t txn_commit(txn_t* tx);
// <8>
result_t txn_get_page(txn_t* tx, page_t* page);
result_t txn_modify_page(txn_t* tx, page_t* page);
// end::tx_api[]
