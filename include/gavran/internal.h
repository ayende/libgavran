#include <gavran/db.h>

#define implementation_detail __attribute__((visibility("hidden")))

// tag::pages_map_t[]
typedef struct pages_hash_table {
  size_t number_of_buckets;
  size_t count;
  size_t resize_required;
  page_t entries[];
} pages_map_t;

result_t pagesmap_put_new(pages_map_t **table_p, page_t *page);
bool pagesmap_lookup(pages_map_t *table, page_t *page);
bool pagesmap_get_next(
    pages_map_t *table, size_t *state, page_t **page);
result_t pagesmap_new(
    size_t initial_number_of_elements, pages_map_t **table);
// end::pages_map_t[]

implementation_detail void txn_free_single_tx_state(
    txn_state_t *state);

implementation_detail void txn_clear_working_set(txn_t *tx);
static inline void defer_txn_clear_working_set(cancel_defer_t *cd) {
  if (cd->cancelled && *cd->cancelled) return;
  txn_clear_working_set(cd->target);
}

implementation_detail result_t db_increase_file_size(
    txn_t *tx, uint64_t new_size);

implementation_detail uint64_t db_find_next_db_size(
    uint64_t current, uint64_t requested_size);

implementation_detail result_t db_validate_options(
    db_options_t *user_options, db_options_t *default_options);
implementation_detail result_t db_setup_page_validation(db_t *ptr);

implementation_detail result_t db_init(db_t *db);

implementation_detail result_t db_initialize_default_read_tx(
    db_state_t *db_state);

implementation_detail result_t db_try_increase_file_size(
    txn_t *tx, uint64_t pages);

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
    size_t search_offset;
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

// varint
// tag::varint_api[]
uint32_t varint_get_length(uint64_t n);
uint8_t *varint_encode(uint64_t n, uint8_t *buf);
uint8_t *varint_decode(uint8_t *buf, uint64_t *value);
// end::varint_api[]

implementation_detail bool hash_page_get_next(
    page_t *p, hash_val_t *it);

implementation_detail result_t print_hash_table(
    FILE *f, txn_t *tx, uint64_t hash_id);
implementation_detail uint64_t hash_permute_key(uint64_t x);

// tag::btree_stack_api[]
void btree_stack_clear(btree_stack_t *s);
result_t btree_stack_push(
    btree_stack_t *s, uint64_t page_num, int16_t pos);
result_t btree_stack_pop(
    btree_stack_t *s, uint64_t *page_num, int16_t *pos);
result_t btree_stack_peek(
    btree_stack_t *s, uint64_t *page_num, int16_t *pos);
result_t btree_stack_free(btree_stack_t *s);
enable_defer(btree_stack_free);
// end::btree_stack_api[]

implementation_detail result_t btree_dump_tree(
    txn_t *tx, uint64_t tree_id, uint16_t max);

implementation_detail void btree_dump_page(
    page_t *p, page_metadata_t *metadata, uint16_t max);

implementation_detail result_t txn_alloc_temp(
    txn_t *tx, size_t min_size, void **buffer);