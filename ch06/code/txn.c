#include <gavran/db.h>
#include <gavran/internal.h>
#include <string.h>

// tag::txn_get_number_of_pages[]
static result_t txn_get_number_of_pages(page_metadata_t *metadata,
                                        uint64_t *number_of_pages) {
  switch (metadata->common.page_flags) {
    case page_flags_file_header:
    case page_flags_free:
    case page_flags_metadata:
      *number_of_pages = 1;
      return success();
    case page_flags_overflow:
      *number_of_pages = metadata->overflow.number_of_pages;
      return success();
    case page_flags_free_space_bitmap:
      *number_of_pages = metadata->free_space.number_of_pages;
      return success();
    default:
      failed(
          EINVAL,
          msg("Unable to get number of pages from unknown page type"),
          with(metadata->common.page_flags, "%d"));
  }
}
// end::txn_get_number_of_pages[]

// tag::txn_get_modify_page[]
result_t txn_get_page(txn_t *tx, page_t *page) {
  page_metadata_t *metadata;
  ensure(txn_get_metadata(tx, page->page_num, &metadata));
  ensure(txn_get_number_of_pages(metadata, &page->number_of_pages));
  ensure(txn_raw_get_page(tx, page));
  return success();
}

result_t txn_modify_page(txn_t *tx, page_t *page) {
  page_metadata_t *metadata;
  ensure(txn_get_metadata(tx, page->page_num, &metadata));
  ensure(
      metadata->common.page_flags != page_flags_free,
      msg("Tried to modify a free page, need to allocate it first"));
  ensure(txn_get_number_of_pages(metadata, &page->number_of_pages));
  ensure(txn_raw_modify_page(tx, page));
  return success();
}
// end::txn_get_modify_page[]
