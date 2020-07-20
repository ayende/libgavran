#include <gavran/db.h>
#include <gavran/internal.h>
#include <math.h>
#include <string.h>

#define GAVRAN_VERSION 1

static result_t db_is_new_file(db_t *db, bool *is_new) {
  txn_t tx;
  ensure(txn_create(db, TX_READ, &tx));
  defer(txn_close, tx);

  page_t page = {.page_num = 0};
  ensure(txn_raw_get_page(&tx, &page));
  page_metadata_t zero;
  memset(&zero, 0, sizeof(page_metadata_t));
  page_metadata_t *entry = page.address;
  *is_new = memcmp(&entry->file_header, &zero,
                   sizeof(page_metadata_t)) == 0;
  return success();
}

static result_t db_init_file_header(db_t *db, txn_t *tx) {
  page_t page = {.page_num = 0};
  ensure(txn_raw_modify_page(tx, &page));
  page_metadata_t *entry = page.address;
  entry->file_header.page_flags = page_flags_file_header;
  entry->file_header.last_tx_id = 0;
  entry->file_header.page_size_power_of_two =
      (uint8_t)(log2(PAGE_SIZE));
  entry->file_header.version = GAVRAN_VERSION;
  memcpy(&entry->file_header.magic, FILE_HEADER_MAGIC, 5);
  entry->file_header.number_of_pages =
      db->state->global_state.span.size / PAGE_SIZE;

  return success();
}

static result_t db_init_free_space_bitmap(txn_t *tx) {
  page_t page = {.page_num = 0};
  ensure(txn_raw_modify_page(tx, &page));
  page_metadata_t *entry = page.address;
  uint64_t free_space_start = 1;
  entry->file_header.free_space_bitmap_start = free_space_start;

  uint64_t pages =
      ROUND_UP(entry->file_header.number_of_pages, BITS_IN_PAGE);

  entry[free_space_start].free_space.page_flags =
      page_flags_free_space_bitmap;
  entry[free_space_start].free_space.number_of_pages = pages;

  page_t p = {.page_num = 1, .size = pages * PAGE_SIZE};
  ensure(txn_raw_modify_page(tx, &p));
  // mark header & free space pages as busy
  for (size_t i = 0; i <= pages; i++) {
    bitmap_set(p.address, i);
  }
  // mark as busy the pages beyond the end of the file
  size_t end = entry->file_header.number_of_pages % BITS_IN_PAGE;
  for (size_t i = end; i < BITS_IN_PAGE; i++) {
    bitmap_set(p.address, i);
  }
  return success();
}

static result_t db_init_file_structure(db_t *db) {
  txn_t tx;
  ensure(txn_create(db, TX_WRITE, &tx));
  defer(txn_close, tx);

  ensure(db_init_file_header(db, &tx));
  ensure(db_init_free_space_bitmap(&tx));

  page_t page = {.page_num = 0};
  ensure(txn_raw_get_page(&tx, &page));
  page_metadata_t *entry = page.address;
  memcpy(&db->state->global_state.header, &entry->file_header,
         sizeof(file_header_t));

  ensure(txn_commit(&tx));
  return success();
}

static result_t db_validate_file_on_startup(db_t *db) {
  txn_t tx;
  ensure(txn_create(db, TX_READ, &tx));
  defer(txn_close, tx);
  page_t page = {.page_num = 0};
  ensure(txn_raw_get_page(&tx, &page));
  page_metadata_t *entry = page.address;

  ensure(!memcmp(FILE_HEADER_MAGIC, entry->file_header.magic, 5),
         msg("Unable to find valid file header magic value"),
         with(db->state->handle->filename, "%s"));

  ensure(GAVRAN_VERSION == entry->file_header.version,
         msg("Gavran version mismatch"), with(GAVRAN_VERSION, "%d"),
         with(entry->file_header.version, "%d"),
         with(db->state->handle->filename, "%s"));

  ensure(entry->file_header.number_of_pages * PAGE_SIZE <=
             db->state->global_state.span.size,
         msg("The size of the file is smaller than the expected."),
         with(db->state->handle->filename, "%s"),
         with(db->state->global_state.span.size, "%lu"),
         with(entry->file_header.number_of_pages * PAGE_SIZE, "%lu"));

  ensure(
      PAGE_SIZE == pow(2, entry->file_header.page_size_power_of_two),
      msg("The file page size is invalid"),
      with(db->state->handle->filename, "%s"),
      with(pow(2, entry->file_header.page_size_power_of_two), "%f"),
      with(PAGE_SIZE, "%d"));

  return success();
}

implementation_detail result_t db_init(db_t *db) {
  bool is_new;
  ensure(db_is_new_file(db, &is_new));

  if (is_new) {
    ensure(db_init_file_structure(db));
  }

  ensure(db_validate_file_on_startup(db));

  return success();
}
