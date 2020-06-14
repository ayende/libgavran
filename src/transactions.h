#pragma once
#include "errors.h"
#include "pal.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define PAGE_SIZE 8192
#define PAGE_ALIGNMENT 4096

_Static_assert(PAGE_SIZE &&PAGE_SIZE % PAGE_ALIGNMENT == 0,
               "PAGE_SIZE must be of a multiple of 4096");

enum page_operations {
  page_op_none = 0,
  page_op_skip_metadata = 1,
} __attribute__((__packed__));
_Static_assert(
  sizeof(enum page_operations) == 1,
  "Expecting page_operations to be a single char in size");

typedef struct page {
  uint64_t page_num;
  void *address;
  uint32_t overflow_size;
  char flags;
  char _padding[3];
} page_t;

typedef struct transaction_state txn_state_t;

typedef struct transaction {
  txn_state_t *state;
} txn_t;

#define FILE_HEADER_MAGIC (9410039042695495) // = 'Gavran!\0'

typedef struct file_header {
  uint64_t magic;
  uint64_t number_of_pages;
  uint64_t free_space_bitmap_start;
  uint64_t free_space_bitmap_in_pages;
  uint32_t pages_in_metadata_section;
  uint32_t version;
  uint32_t page_size;
  uint32_t _padding;
} file_header_t;

typedef struct database_options {
  uint64_t minimum_size;
} database_options_t;

typedef struct database_state db_state_t;

typedef struct database {
  db_state_t *state;
} database_t;

MUST_CHECK bool open_database(const char *path,
                              database_options_t *options,
                              database_t *db);
MUST_CHECK bool close_database(database_t *db);

MUST_CHECK bool create_transaction(database_t *db, uint32_t flags,
                                   txn_t *tx);

MUST_CHECK bool get_page(txn_t *tx, page_t *page);
MUST_CHECK bool modify_page(txn_t *tx, page_t *page);
MUST_CHECK bool allocate_page(txn_t *tx, page_t *page,
                              uint64_t near_page);
MUST_CHECK bool free_page(txn_t *tx, page_t *page);

MUST_CHECK bool commit_transaction(txn_t *tx);
MUST_CHECK bool close_transaction(txn_t *tx);

bool find_free_range_in_bitmap(uint64_t *bitmap, size_t bitmap_size,
                               size_t size_required, size_t near_pos,
                               size_t *bit_pos);

MUST_CHECK bool get_metadata_page_offset(
  uint64_t total_number_of_pages, uint64_t page_num,
  uint64_t *metadata_page_num, size_t *metadata_index);
