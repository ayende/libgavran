#include <gavran/db.h>
#include <gavran/internal.h>

// tag::metadata_api[]
static result_t get_metadata_entry(uint64_t page_num,
                                   page_t *metadata_page,
                                   page_metadata_t **metadata) {
  page_metadata_t *entries = metadata_page->address;
  page_flags_t expected = metadata_page->page_num
                              ? page_flags_metadata
                              : page_flags_file_header;
  ensure(expected == entries->common.page_flags,
         msg("Got invalid metadata page"), with(page_num, "%lu"));

  *metadata = &entries[page_num & ~PAGES_IN_METADATA_MASK];
  return success();
}

result_t txn_get_metadata(txn_t *tx, uint64_t page_num,
                          page_metadata_t **metadata) {
  page_t metadata_page = {.page_num =
                              page_num & PAGES_IN_METADATA_MASK};
  ensure(txn_raw_get_page(tx, &metadata_page));
  return get_metadata_entry(page_num, &metadata_page, metadata);
}
result_t txn_modify_metadata(txn_t *tx, uint64_t page_num,
                             page_metadata_t **metadata) {
  page_t metadata_page = {.page_num =
                              page_num & PAGES_IN_METADATA_MASK};
  ensure(txn_raw_modify_page(tx, &metadata_page));
  return get_metadata_entry(page_num, &metadata_page, metadata);
}
// end::metadata_api[]