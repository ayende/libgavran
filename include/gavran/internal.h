#include <gavran/db.h>

#define implementation_detail __attribute__((visibility("hidden")))

// tag::pages_hash_table_t[]
typedef struct pages_hash_table {
  size_t number_of_buckets;
  size_t count;
  size_t resize_required;
  page_t entries[];
} pages_hash_table_t;

result_t hash_put_new(pages_hash_table_t **table_p, page_t *page);
bool hash_lookup(pages_hash_table_t *table, page_t *page);
bool hash_get_next(pages_hash_table_t *table, size_t *state,
                   page_t **page);
result_t hash_new(size_t initial_number_of_elements,
                  pages_hash_table_t **table);
// end::pages_hash_table_t[]

implementation_detail void txn_free_single_tx_state(
    txn_state_t *state);

implementation_detail result_t txn_clear_working_set(txn_t *tx);
enable_defer(txn_clear_working_set);

implementation_detail result_t
db_increase_file_size(txn_t *tx, uint64_t new_size);

implementation_detail uint64_t
db_find_next_db_size(uint64_t current, uint64_t requested_size);

implementation_detail result_t db_validate_options(
    db_options_t *user_options, db_options_t *default_options);
implementation_detail result_t db_setup_page_validation(db_t *ptr);

implementation_detail result_t db_init(db_t *db);

implementation_detail result_t
db_initialize_default_read_tx(db_state_t *db_state);

implementation_detail result_t
db_try_increase_file_size(txn_t *tx, uint64_t pages);

implementation_detail void db_initialize_default_options(
    db_options_t *options);

__attribute__((const)) static inline uint64_t next_power_of_two(
    uint64_t x) {
  return 1 << (64 - __builtin_clzll(x - 1));
}

// tag::bitmap_search[]
typedef struct bitmap_search_state {
  struct {
    uint64_t *bitmap;
    size_t bitmap_size;
    uint64_t space_required;
    uint64_t near_position;
  } input;
  struct {
    uint64_t found_position;
    uint64_t space_available_at_position;
  } output;
  struct {
    uint64_t index;
    uint64_t current_word;
    uint64_t current_set_bit;
    uint64_t previous_set_bit;
  } internal;

} bitmap_search_state_t;

implementation_detail bool bitmap_search(
    bitmap_search_state_t *search);
implementation_detail bool bitmap_is_acceptable_match(
    bitmap_search_state_t *search);
// end::bitmap_search[]

// tag::wal_api[]
result_t wal_open_and_recover(db_t *db);
result_t wal_append(txn_state_t *tx);
bool wal_will_checkpoint(db_state_t *db, uint64_t tx_id);
result_t wal_checkpoint(db_state_t *db, uint64_t tx_id);
result_t wal_close(db_state_t *db);
enable_defer(wal_close);
// end::wal_api[]
