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

MUST_CHECK bool create_transaction(file_handle_t* handle, uint32_t flags, txn_t* tx);

MUST_CHECK bool get_page(txn_t* tx, page_t* page);
MUST_CHECK bool modify_page(txn_t* tx, page_t* page);
MUST_CHECK bool allocate_page(txn_t tx, page_t* page);
MUST_CHECK bool free_page(txn_t* tx, page_t* page);

MUST_CHECK bool commit_transaction(txn_t* tx);
MUST_CHECK bool close_transaction(txn_t* tx);
