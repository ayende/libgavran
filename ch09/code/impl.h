#include <stdalign.h>

#include "db.h"
#include "platform.fs.h"

#define BITS_IN_PAGE (PAGE_SIZE * 8)
#define PAGES_IN_METADATA (128UL)
#define PAGES_IN_METADATA_MASK (-128UL)

// tag::wal_api[]
typedef struct wal_state wal_state_t;
result_t wal_open_and_recover(db_t *db);
result_t wal_append(txn_state_t *tx);
bool wal_will_checkpoint(db_state_t *db, uint64_t tx_id);
result_t wal_checkpoint(db_state_t *db, uint64_t tx_id);
result_t wal_close(db_state_t *db);
enable_defer(wal_close);
// end::wal_api[]

// tag::transaction_state[]
struct transaction_state {
  db_state_t *db;
  uint32_t flags;
  uint32_t modified_pages;
  size_t allocated_size;

  // <2>
  txn_state_t *prev_tx;
  txn_state_t *next_tx;
  uint64_t tx_id;
  size_t usages;
  uint64_t can_free_after_tx_id;

  page_t entries[];
};

#define get_number_of_buckets(state)                                           \
  ((state->allocated_size - sizeof(txn_state_t)) / sizeof(page_t))

// end::transaction_state[]

// tag::database_state[]
struct database_state {
  database_options_t options;
  struct mmap_args mmap;
  file_handle_t *handle;
  file_header_t header;
  char _padding[6];

  // <1>
  wal_state_t *wal_state;

  txn_state_t *last_write_tx;
  txn_state_t *active_write_tx;
  txn_state_t *default_read_tx;
  txn_state_t *transactions_to_free;
  uint64_t oldest_active_tx;
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

result_t txn_write_state_to_disk(txn_state_t *state);

result_t txn_modify_page_raw(txn_t *tx, page_t *page);
