#include <gavran/db.h>
#include <gavran/internal.h>

implementation_detail result_t db_setup_page_validation(db_t *ptr) {
  (void)ptr;
  return success();
}

implementation_detail result_t
db_initialize_default_read_tx(db_state_t *db_state) {
  (void)db_state;
  return success();
}

implementation_detail bool bitmap_is_acceptable_match(
    bitmap_search_state_t *search) {
  (void)search;
  return true;
}
