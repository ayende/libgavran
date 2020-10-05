#include <gavran/db.h>
#include <gavran/internal.h>
#include <string.h>

// tag::txn_get_number_of_pages[]
static result_t txn_get_number_of_pages(
    page_metadata_t *metadata, uint32_t *number_of_pages) {
  switch (metadata->common.page_flags) {
    case page_flags_file_header:
    case page_flags_free:
    case page_flags_metadata:
    case page_flags_container:
    case page_flags_hash:
    case page_flags_tree_branch:
    case page_flags_tree_leaf:
      *number_of_pages = 1;
      return success();
    case page_flags_hash_directory: {
      uint32_t buckets = metadata->hash_dir.number_of_buckets;
      *number_of_pages = TO_PAGES(buckets * sizeof(uint64_t));
      return success();
    }
    case page_flags_overflow:
      *number_of_pages = metadata->overflow.number_of_pages;
      return success();
    case page_flags_free_space_bitmap:
      *number_of_pages = metadata->free_space.number_of_pages;
      return success();
    default:
      failed(EINVAL,
          msg("Unable to get number of pages from unknown page type"),
          with(metadata->common.page_flags, "%d"));
  }
}
// end::txn_get_number_of_pages[]

// tag::txn_get_modify_page[]
result_t txn_get_page(txn_t *tx, page_t *page) {
  page_metadata_t *mt;
  ensure(txn_get_metadata(tx, page->page_num, &mt));
  ensure(txn_get_number_of_pages(mt, &page->number_of_pages));
  ensure(txn_raw_get_page(tx, page));
  page->metadata = mt;
  return success();
}

result_t txn_modify_page(txn_t *tx, page_t *page) {
  page_metadata_t *metadata;
  ensure(txn_modify_metadata(tx, page->page_num, &metadata));
  ensure(metadata->common.page_flags != page_flags_free,
      msg("Tried to modify a free page, need to allocate it first"));
  ensure(txn_get_number_of_pages(metadata, &page->number_of_pages));
  ensure(txn_raw_modify_page(tx, page));
  page->metadata = metadata;
  return success();
}
// end::txn_get_modify_page[]
