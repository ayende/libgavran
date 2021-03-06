#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

implementation_detail void btree_dump_page(page_t* p, uint16_t max) {
  char* type = p->metadata->tree.page_flags == page_flags_tree_branch
                   ? "branch"
                   : "leaf";
  printf("Page #%lu (%s) (%lu entries) (space: %d) [%d to %d]\n",
      p->page_num, type, p->metadata->tree.floor / sizeof(uint16_t),
      p->metadata->tree.free_space, p->metadata->tree.floor,
      p->metadata->tree.ceiling);
  uint16_t max_pos    = p->metadata->tree.floor / sizeof(uint16_t);
  uint16_t* positions = p->address;
  for (uint16_t i = 0; i < MIN(max / 2, max_pos); i++) {
    uint64_t key_size, val;
    uint8_t* key_start =
        varint_decode(p->address + positions[i], &key_size);
    varint_decode(key_start + key_size, &val);
    printf(
        "%d)\t %.*s -> %lu\n", i, (int32_t)key_size, key_start, val);
  }

  for (uint16_t i = MIN(max_pos, max_pos - (max / 2)); i < max_pos;
       i++) {
    uint64_t key_size, val;
    uint8_t* key_start =
        varint_decode(p->address + positions[i], &key_size);
    varint_decode(key_start + key_size, &val);
    printf(
        "%d)\t %.*s -> %lu\n", i, (int32_t)key_size, key_start, val);
  }
}

static result_t btree_dump_branch_page(
    txn_t* tx, page_t* p, uint16_t max) {
  printf("Page #%lu (branch) (%lu entries) (space: %d) [%d to %d]\n",
      p->page_num, p->metadata->tree.floor / sizeof(uint16_t),
      p->metadata->tree.free_space, p->metadata->tree.floor,
      p->metadata->tree.ceiling);
  uint16_t max_pos    = p->metadata->tree.floor / sizeof(uint16_t);
  uint16_t* positions = p->address;
  for (uint16_t i = 0; i < max_pos; i++) {
    uint64_t key_size, val;
    uint8_t* key_start =
        varint_decode(p->address + positions[i], &key_size);
    varint_decode(key_start + key_size, &val);
    printf(
        "%d)\t %.*s -> %lu\n", i, (int32_t)key_size, key_start, val);
  }
  printf("==========\n");
  for (uint16_t i = 0; i < max_pos; i++) {
    uint64_t key_size, val;
    uint8_t* key_start =
        varint_decode(p->address + positions[i], &key_size);
    varint_decode(key_start + key_size, &val);
    ensure(btree_dump_tree(tx, val, max));
    printf("-------------\n");
  }
  return success();
}
implementation_detail result_t btree_dump_tree(
    txn_t* tx, uint64_t tree_id, uint16_t max) {
  page_t p = {.page_num = tree_id};
  ensure(txn_get_page(tx, &p));
  if (p.metadata->tree.page_flags == page_flags_tree_branch) {
    ensure(btree_dump_branch_page(tx, &p, max));
  } else {
    btree_dump_page(&p, max);
  }
  return success();
}