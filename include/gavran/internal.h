#include <gavran/db.h>

#define implementation_detail __attribute__((visibility("hidden")))
#define weak_symbol __attribute__((weak))

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
result_t hash_try_add(pages_hash_table_t **table_p,
                      uint64_t page_num);
result_t hash_new(size_t initial_number_of_elements,
                  pages_hash_table_t **table);
// end::pages_hash_table_t[]

implementation_detail result_t db_validate_options(
    db_options_t *user_options, db_options_t *default_options);
implementation_detail result_t
db_setup_page_validation(db_state_t *ptr);

implementation_detail result_t db_init(db_t *db);

implementation_detail result_t
db_initialize_default_read_tx(db_state_t *db_state);

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

bool bitmap_search(bitmap_search_state_t *search);
// end::bitmap_search[]
