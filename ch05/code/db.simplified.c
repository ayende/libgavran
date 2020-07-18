#include <gavran/db.h>
#include <gavran/internal.h>
#include <string.h>

#define GAVRAN_VERSION 1

static result_t db_init_free_space(db_t *db, txn_t *tx) {
  return success();
}

// tag::db_init_file_structure[]
static result_t db_init_file_header(db_t *db, txn_t *tx) {
  page_t first_page = {.page_num = 0};
  ensure(txn_raw_modify_page(&tx, &first_page));
  memset(first_page.address, 0, PAGE_SIZE);
  page_metadata_t *entry = first_page.address;
  entry->common.page_flags = page_flags_file_header;

  entry->file_header.number_of_pages =
      db->state->global_state.span.size / PAGE_SIZE;
  memcpy(entry->file_header.magic, FILE_HEADER_MAGIC,
         sizeof(entry->file_header.magic));
  entry->file_header.page_size_power_of_two =
      (uint8_t)(log2(PAGE_SIZE));
  entry->file_header.version = GAVRAN_VERSION;
  return success();
}

static result_t db_init_file_structure(db_t *db) {
  txn_t tx;
  ensure(txn_create(db, TX_WRITE, &tx));
  defer(txn_close, tx);

  ensure(db_init_file_header(db, &tx));
  ensure(db_init_free_space(db, &tx));

  return success();
}
// end::db_init_file_structure[]

// tag::db_validate_file_header[]
static result_t db_validate_file_header(db_t *db,
                                        file_header_t *header) {
  ensure(header->number_of_pages * PAGE_SIZE <=
             db->state->global_state.span.size,
         msg("File size smaller than expected, truncated?"),
         with(db->state->handle->filename, "%s"),
         with(db->state->global_state.span.size, "%lu"),
         with(header->number_of_pages * PAGE_SIZE, "%lu"));

  ensure(header->version == GAVRAN_VERSION,
         msg("The file version invalid"),
         with(db->state->handle->filename, "%s"),
         with(header->version, "%d"), with(GAVRAN_VERSION, "%d"));

  uint32_t size = pow(2, header->page_size_power_of_two);
  ensure(size == PAGE_SIZE, msg("The file page size is invalid"),
         with(db->state->handle->filename, "%s"), with(size, "%d"),
         with(PAGE_SIZE, "%d"));

  return success();
}
// end::db_validate_file_header[]

// tag::db_init_from_existing_file[]
static result_t db_init_from_existing_file(db_t *db) {
  txn_t tx;
  ensure(txn_create(db, TX_READ, &tx));
  defer(txn_close, tx);
  page_t first_page = {.page_num = 0};
  ensure(txn_raw_get_page(&tx, &first_page));
  page_metadata_t *entry = first_page.address;

  ensure(entry->common.page_flags == page_flags_file_header,
         msg("The first page must be file header page, but wasn't."),
         with(db->state->handle->filename, "%s"));

  ensure(db_validate_file_header(db, &entry->file_header));
  return success();
}
// end::db_init_from_existing_file[]

// tag::db_init[]
static result_t db_is_new_file(db_t *db, bool *is_new) {
  txn_t tx;
  ensure(txn_create(db, TX_READ, &tx));
  defer(txn_close, tx);
  page_t first_page = {.page_num = 0};
  ensure(txn_raw_get_page(&tx, &first_page));
  page_metadata_t *entry = first_page.address;
  page_metadata_t zero = {0};
  *is_new = memcpy(&zero, entry, sizeof(page_metadata_t)) == 0;
  return success();
}

static result_t db_init(db_t *db) {
  bool is_new;
  ensure(db_is_new_file(db, &is_new));
  if (is_new) {
    ensure(db_init_file_structure(db));
  }
  ensure(db_init_from_existing_file(db));
  return success();
}
// end::db_init[]