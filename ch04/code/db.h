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

// tag::tx_api[]
typedef struct database_state db_state_t;

typedef struct transaction_state txn_state_t;

typedef struct database_options {
  uint64_t minimum_size;
} database_options_t;

typedef struct database {
  db_state_t* state;
} db_t;

typedef struct transaction {
  txn_state_t* state;
} txn_t;

result_t db_create(const char* filename, database_options_t* options,
                   db_t* db);

result_t db_close(db_t* db);
enable_defer(db_close);

result_t txn_create(db_t* db, uint32_t flags, txn_t* tx);

result_t txn_close(txn_t* tx);
enable_defer(txn_close);

result_t txn_commit(txn_t* tx);

result_t txn_get_page(txn_t* tx, page_t* page);
result_t txn_modify_page(txn_t* tx, page_t* page);
// end::tx_api[]
