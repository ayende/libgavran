#include "db.h"
#include "platform.fs.h"

#define BITS_IN_PAGE (PAGE_SIZE * 8)

// tag::transaction_state[]
struct transaction_state {
  db_state_t* db;
  size_t allocated_size;
  uint32_t flags;
  uint32_t modified_pages;
  page_t entries[];
};
// end::transaction_state[]

// tag::database_state[]
struct database_state {
  database_options_t options;
  struct mmap_args mmap;
  file_handle_t* handle;
  file_header_t header;
};
// end::database_state[]

result_t pages_get(db_state_t* db, page_t* p);
result_t pages_write(db_state_t* db, page_t* p);

// tag::search_free_range_in_bitmap[]
typedef struct bitmap_search_state {
  // intput
  uint64_t* bitmap;
  size_t bitmap_size;
  uint64_t space_required;
  uint64_t near_position;
  // output
  uint64_t found_position;
  uint64_t space_available_at_position;

  // internal state
  uint64_t index;
  uint64_t current_word;
  uint64_t current_set_bit;
  uint64_t previous_set_bit;
} bitmap_search_state_t;

void init_search(bitmap_search_state_t* search, void* bitmap,
                 uint64_t size, uint64_t required);

bool search_free_range_in_bitmap(bitmap_search_state_t* search);
// end::search_free_range_in_bitmap[]
