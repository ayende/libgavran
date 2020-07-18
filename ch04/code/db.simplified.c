#include <gavran/db.h>
#include <gavran/internal.h>
#include <string.h>

// tag::db_validate_options[]
implementation_detail result_t db_validate_options(
    db_options_t *user_options, db_options_t *default_options) {
  if (user_options->minimum_size)
    default_options->minimum_size = user_options->minimum_size;

  if (default_options->minimum_size < 128 * 1024) {
    failed(EINVAL,
           msg("The minimum_size cannot be less than the minimum "
               "value of 128KB"),
           with(default_options->minimum_size, "%lu"));
  }

  return success();
}
// end::db_validate_options[]

// tag::db_initialize_default_options[]
implementation_detail void db_initialize_default_options(
    db_options_t *options) {
  memset(options, 0, sizeof(db_options_t));
  options->minimum_size = 1024 * 1024;
}
// end::db_initialize_default_options[]

implementation_detail result_t
db_setup_page_validation(db_state_t *ptr) {
  (void)ptr;
  return success();
}

implementation_detail result_t db_init(db_t *db) {
  (void)db;
  return success();
}

implementation_detail result_t
db_initialize_default_read_tx(db_state_t *db_state) {
  (void)db_state;
  return success();
}

// tag::db_close[]
result_t db_close(db_t *db) {
  if (!db || !db->state) return success();  // double close?

  bool failure = false;
  failure |= !pal_unmap(&db->state->global_state.span);
  failure |= !pal_close_file(db->state->handle);

  if (failure) {
    errors_push(EIO, msg("Unable to properly close the database"));
  }

  free(db->state);
  db->state = 0;

  if (failure) {
    return failure_code();
  }
  return success();
}
// end::db_close[]
