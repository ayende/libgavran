#pragma once

#include "db.h"
#include "errors.h"

#define PAGE_SIZE 8192
#define PAGE_ALIGNMENT 4096

typedef struct page {
  void* address;
  uint64_t page_num;
  size_t num_of_pages;
} page_t;

result_t pages_get(txn_t* tx, page_t* p);

result_t pages_write(txn_t* tx, page_t* p);