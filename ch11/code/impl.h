#include <sodium.h>
#include <stdalign.h>

#include "db.h"
#include "platform.fs.h"

#define BITS_IN_PAGE (PAGE_SIZE * 8)
#define PAGES_IN_METADATA (128UL)
#define PAGES_IN_METADATA_MASK (-128UL)

// tag::wal_api[]

// tag::wal_state_t[]
struct wal_file_state {
  file_handle_t *handle;
  struct mmap_args map;
  uint64_t last_write_pos;
  // <1>
  uint64_t last_tx_id;
};

typedef struct wal_state {
  // <2>
  size_t current_append_file_index;
  // <3>
  struct wal_file_state files[2];
} wal_state_t;
// end::wal_state_t[]

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

typedef struct pages_hash_table {
  size_t allocated_size;
  uint32_t modified_pages;
  uint32_t _padding;
  page_t entries[];
} pages_hash_table_t;

result_t hash_put_new(pages_hash_table_t **table_p, page_t *page);
bool hash_lookup(pages_hash_table_t *table, page_t *page);
bool hash_get_next(pages_hash_table_t *table, size_t *state, page_t **page);
result_t hash_try_add(pages_hash_table_t **table_p, uint64_t page_num);
result_t hash_new(size_t initial_number_of_elements,
                  pages_hash_table_t **table);

struct transaction_state {
  db_state_t *db;
  struct cleanup_act *on_forget;
  struct cleanup_act *on_rollback;
  db_global_state_t global_state;

  txn_state_t *prev_tx;
  txn_state_t *next_tx;
  uint64_t can_free_after_tx_id;
  size_t usages;
  uint32_t flags;
  uint32_t _padding;
  pages_hash_table_t *pages;
};

// end::transaction_state[]

// tag::database_state[]
struct database_state {
  database_options_t options;
  db_global_state_t global_state;
  file_handle_t *handle;
  wal_state_t wal_state;
  txn_state_t *last_write_tx;
  uint64_t active_write_tx;
  txn_state_t *default_read_tx;
  txn_state_t *transactions_to_free;
  uint64_t *first_read_bitmap;
  uint64_t original_number_of_pages;
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

enum __attribute__((__packed__)) page_type {
  page_metadata = 1,
  page_single = 2,
  page_overflow_first = 3,
  page_overflow_rest = 2,
  page_free_space_bitmap = 4,
};

_Static_assert(sizeof(enum page_type) == 1, "Must be a single byte");

// tag::page_metadata_t[]
struct page_metadata {
  // these are used for AES GCM metadata for the page
  union {
    // <1>
    struct {
      uint8_t nonce[crypto_aead_aes256gcm_NPUBBYTES];
      uint8_t mac[crypto_aead_aes256gcm_ABYTES];
    } aes_gcm;
    // <2>
    uint8_t page_hash[crypto_generichash_BYTES];
  };

  uint32_t overflow_size;
  enum page_type type;
  char _padding;

  union {
    struct {
      file_header_t file_header;
    };
  };
};

#define PAGE_METADATA_CRYPTO_HEADER_SIZE 32

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

result_t txn_hash_page(page_t *page, uint8_t hash[crypto_generichash_BYTES]);
result_t
txn_validate_page_hash(page_t *page,
                       uint8_t expected_hash[crypto_generichash_BYTES]);

__attribute__((const)) static inline uint64_t next_power_of_two(uint64_t x) {
  return 1 << (64 - __builtin_clzll(x - 1));
}

result_t txn_decrypt(database_options_t *options, void *start, size_t size,
                     void *dest, page_metadata_t *metadata, uint64_t page_num);
