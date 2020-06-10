#pragma once
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "errors.h"
#include "pal.h"

#define PAGE_SIZE 8192
#define PAGE_ALIGNMENT 4096

_Static_assert(PAGE_SIZE && PAGE_SIZE % PAGE_ALIGNMENT == 0, "PAGE_SIZE must be of a multiple of 4096");

typedef struct page{
    uint64_t page_num;
	void* address;
} page_t;

typedef struct transaction_state txn_state_t;

typedef struct transaction {
    txn_state_t* state;
} txn_t;

#define FILE_HEADER_MAGIC (9410039042695495) // = 'Gavran!\0'

typedef struct file_header {
    uint64_t magic;
    uint64_t number_of_pages;
    uint32_t version;
    uint32_t page_size;

} file_header_t;

typedef struct database_options{
    uint64_t minimum_size;
} database_options_t;

typedef struct database_state db_state_t;

typedef struct database {
    db_state_t* state;
} database_t;

MUST_CHECK bool open_database(const char* path, database_options_t* options, database_t* db);
MUST_CHECK bool close_database(database_t* db);

MUST_CHECK bool create_transaction(database_t* db, uint32_t flags, txn_t* tx);

MUST_CHECK bool get_page(txn_t* tx, page_t* page);
MUST_CHECK bool modify_page(txn_t* tx, page_t* page);
MUST_CHECK bool allocate_page(txn_t tx, page_t* page);
MUST_CHECK bool free_page(txn_t* tx, page_t* page);

MUST_CHECK bool commit_transaction(txn_t* tx);
MUST_CHECK bool close_transaction(txn_t* tx);
