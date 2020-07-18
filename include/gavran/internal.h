#include <gavran/db.h>

#define implementation_detail __attribute__((visibility("hidden")))

// tag::pages_hash_table_t[]
typedef struct pages_hash_table {
  size_t number_of_buckets;
  uint32_t count;
  uint32_t resize_required;
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
