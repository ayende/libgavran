#include "db.h"
#include "platform.fs.h"

// tag::database_state[]
struct database_state {
  database_options_t options;
  struct mmap_args mmap;
  file_handle_t* handle;
};
// end::database_state[]

// tag::transaction_state[]
struct transaction_state {
  db_state_t* db;
  size_t allocated_size;
  uint32_t flags;
  uint32_t modified_pages;
  page_t entries[];
};
// end::transaction_state[]
