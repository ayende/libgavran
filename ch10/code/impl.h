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

typedef struct db_global_state {
  char _padding[6];
  file_header_t header;
  struct mmap_args mmap;
} db_global_state_t;

struct cleanup_act {
  void (*func)(void *state);
  struct cleanup_act *next;
  char state[];
};

struct transaction_state {
  db_state_t *db;
  uint32_t flags;
  uint32_t modified_pages;

  struct cleanup_act *on_forget;
  struct cleanup_act *on_rollback;
  db_global_state_t global_state;

  txn_state_t *prev_tx;
  txn_state_t *next_tx;
  uint64_t can_free_after_tx_id;
  size_t usages;
  size_t allocated_size;

  page_t entries[];
};

#define get_number_of_buckets(state)                                           \
  ((state->allocated_size - sizeof(txn_state_t)) / sizeof(page_t))

// end::transaction_state[]

// tag::database_state[]
struct database_state {
  database_options_t options;
  db_global_state_t global_state;
  file_handle_t *handle;

  // <1>
  wal_state_t *wal_state;

  txn_state_t *last_write_tx;
  uint64_t active_write_tx;
  txn_state_t *default_read_tx;
  txn_state_t *transactions_to_free;
  uint64_t oldest_active_tx;
};
// end::database_state[]

result_t pages_get(db_global_state_t *state, page_t *p);
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
  char _padding;

  union {
    struct {
      file_header_t file_header;
    };
  };

  // these are used for AES GCM metadata for the page
  union {
    struct {
      char reserved_nonce[12];
      char reserved_mac[16];
    } aes_gcm_header;
    char reserved_blake2b_hash[32];
  };
};

_Static_assert(sizeof(page_metadata_t) == 64,
               "The size of page metadata must be 64 bytes");
// end::page_metadata_t[]

result_t txn_write_state_to_disk(txn_state_t *state, bool can_checkpoint);

uint64_t db_find_next_db_size(uint64_t current, uint64_t requested_size);

uint64_t TEST_wal_get_last_write_position(db_t *db);

result_t txn_register_on_forget(txn_state_t *tx, void (*action)(void *),
                                void *state_to_copy, size_t size_of_state);
result_t txn_register_on_rollback(txn_state_t *tx, void (*action)(void *),
                                  void *state_to_copy, size_t size_of_state);
result_t db_try_increase_file_size(txn_t *tx,
                                   uint64_t required_additional_pages);
