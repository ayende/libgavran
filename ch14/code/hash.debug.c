#include <gavran/db.h>
#include <gavran/internal.h>

#include <stdio.h>

static void print_bits(FILE* fd, uint64_t v, int depth) {
  uint8_t* b = (uint8_t*)&v;
  uint8_t byte;
  int i, j;
  int bits          = sizeof(uint64_t) * 8;
  bool foundNonZero = false;
  for (i = sizeof(uint64_t) - 1; i >= 0; i--) {
    for (j = 7; j >= 0; j--) {
      bits--;
      byte = (b[i] >> j) & 1;
      if (byte) {
        foundNonZero = true;
      }
      if (!foundNonZero && bits > depth && bits > 8) continue;
      fprintf(fd, "%u", byte);
      if (bits == depth) fprintf(fd, " ");
    }
  }
}

implementation_detail void print_hash_page(
    FILE* f, page_t* p, page_metadata_t* metadata) {
  fprintf(f,
      "\tbucket_%lu [label=\"Depth: %d, Entries: %d, Bytes Used: "
      "%d\\l--------\\l",
      p->page_num, metadata->hash.depth,
      metadata->hash.number_of_entries, metadata->hash.bytes_used);
  hash_val_t it = {0};
  while (hash_page_get_next(p->address, &it)) {
    print_bits(f, hash_permute_key(it.key), metadata->hash.depth);
    fprintf(f, " \\| %ld (%ld)= %ld (flags: %d)\\l", it.key,
        hash_permute_key(it.key), it.val, it.flags);
  }
  fprintf(f, "\"]\n");
}

implementation_detail result_t print_hash_table(
    FILE* f, txn_t* tx, uint64_t hash_id) {
  fprintf(f, "digraph hash {\n\tnode[shape = record ]; \n");
  page_t p = {.page_num = hash_id};
  page_metadata_t* metadata;
  ensure(txn_get_page_and_metadata(tx, &p, &metadata));
  if (metadata->common.page_flags == page_flags_hash) {
    print_hash_page(f, &p, metadata);
    return success();
  }
  fprintf(f,
      "\ttable [label=\"Page: %lu, Depth: %d \\lBuckets: "
      "%i\\lEntries: %lu\"]\n",
      p.page_num, metadata->hash_dir.depth,
      metadata->hash_dir.number_of_buckets,
      metadata->hash_dir.number_of_entries);
  fprintf(f, "\t\tbuckets [label=\"");
  uint64_t* buckets = p.address;
  {
    pages_map_t* pages;
    ensure(pagesmap_new(8, &pages));
    defer(free, pages);
    for (size_t i = 0; i < metadata->hash_dir.number_of_buckets;
         i++) {
      page_t tp = {.page_num = buckets[i]};
      if (pagesmap_lookup(pages, &tp)) continue;
      if (i != 0) fprintf(f, "|");
      fprintf(f, "<bucket_%zu> %zu - %lu ", i, i, buckets[i]);
      ensure(pagesmap_put_new(&pages, &tp));
    }
  }
  fprintf(f, "\"]\n");

  for (size_t i = 0; i < metadata->hash_dir.number_of_buckets; i++) {
    page_t hash_page = {.page_num = buckets[i]};
    page_metadata_t* hash_metadata;
    ensure(txn_get_page_and_metadata(tx, &hash_page, &hash_metadata));
    print_hash_page(f, &hash_page, hash_metadata);
    fprintf(f, "\n");
  }

  for (size_t i = 0; i < metadata->hash_dir.number_of_buckets; i++) {
    fprintf(f, "\tbuckets:bucket_%ld->bucket_%ld;\n", i, buckets[i]);
  }
  fprintf(f, "\ttable->buckets;\n}\n");
  return success();
}
