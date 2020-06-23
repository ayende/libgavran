#include <stdalign.h>

#include "db.h"
#include "platform.fs.h"

#define BITS_IN_PAGE (PAGE_SIZE * 8)
#define PAGES_IN_METADATA (128UL)
#define PAGES_IN_METADATA_MASK (-128UL)

// tag::transaction_state[]
struct transaction_state {
  db_state_t *db;
  uint32_t flags;
  uint32_t modified_pages;
  txn_state_t *previous_tx;
  txn_state_t *next_tx;
  txn_state_t *next_tx_in_free_list;

  // <2>
  uint64_t tx_id;
  size_t allocated_size;
  size_t usages;
  uint64_t can_free_after_tx_id;

  page_t entries[];
};
// end::transaction_state[]

// tag::database_state[]
struct database_state {
  database_options_t options;
  struct mmap_args mmap;
  file_handle_t *handle;
  file_header_t header;
  // <1>
  char _padding[6];
  txn_state_t *last_write_tx;
  txn_state_t *active_write_tx;
  txn_state_t *default_read_tx;
  txn_state_t *transactions_to_free;
  uint64_t last_tx_id;
};
// end::database_state[]

result_t pages_get(db_state_t *db, page_t *p);
result_t pages_write(db_state_t *db, page_t *p);

void txn_free_single_tx(txn_state_t *state);

// tag::search_free_range_in_bitmap[]
typedef struct bitmap_search_state {
  // intput
  uint64_t *bitmap;
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

void init_search(bitmap_search_state_t *search, void *bitmap, uint64_t size,
                 uint64_t required);

bool search_free_range_in_bitmap(bitmap_search_state_t *search);
// end::search_free_range_in_bitmap[]

// tag::page_metadata_t[]
enum __attribute__((__packed__)) page_type {
  page_metadata = 1,
  page_single = 2,
  page_overflow_first = 3,
  page_overflow_rest = 2,
  page_free_space_bitmap = 4,
};

_Static_assert(sizeof(enum page_type) == 1, "Must be a single byte");

struct page_metadata {
  uint32_t overflow_size;
  enum page_type type;
  uint8_t number_of_pages_beyond_overflow_size;

  union {
    struct {
      char _file_header_padding[2];
      file_header_t file_header;
    };
    char reserved_future[30];
  };

  // these are used for AES GCM metadata for the page
  char reserved_nonce[12];
  char reserved_mac[16];
};

_Static_assert(sizeof(page_metadata_t) == 64,
               "The size of page metadata must be 64 bytes");
// end::page_metadata_t[]
