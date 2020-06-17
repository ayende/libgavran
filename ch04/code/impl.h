#include "db.h"
#include "platform.fs.h"

// tag::transaction_state[]
struct transaction_state {
  db_state_t* db;
  size_t allocated_size;
  uint32_t flags;
  uint32_t modified_pages;
  page_t entries[];
};
// end::transaction_state[]

// tag::paging_api[]

struct database_state {
  database_options_t options;
  struct mmap_args mmap;
  file_handle_t* handle;
};

result_t pages_get(db_state_t* db, page_t* p);
result_t pages_write(db_state_t* db, page_t* p);
// end::paging_api[]
