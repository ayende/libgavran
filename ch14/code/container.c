#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

#define CONTAINER_ITEM_SMALL_MAX_SIZE 6 * 1024

static inline size_t container_get_total_size(size_t size) {
  return sizeof(int16_t) +               // offset to value
         varint_encoding_length(size) +  // varint len
         size;
}

result_t container_create(txn_t *tx, uint64_t *container_id) {
  page_t p = {.number_of_pages = 1};
  page_metadata_t *metadata;
  ensure(txn_allocate_page(tx, &p, &metadata, 0));
  metadata->container.next_allocation_at = p.page_num;
  metadata->container.page_flags = page_flags_container;
  metadata->container.prev = 0;
  metadata->container.next = 0;
  metadata->container.floor = 0;
  metadata->container.ceiling = PAGE_SIZE;
  metadata->container.free_space = PAGE_SIZE;
  *container_id = p.page_num;
  return success();
}

static result_t container_allocate_new_page(txn_t *tx,
                                            uint64_t container_id,
                                            uint64_t previous,
                                            uint64_t *page_num) {
  page_t p = {.number_of_pages = 1};
  page_metadata_t *metadata;
  ensure(txn_allocate_page(tx, &p, &metadata, previous));
  page_metadata_t *prev_metadata;
  ensure(txn_modify_metadata(tx, previous, &prev_metadata));
  assert(prev_metadata->common.page_flags == page_flags_container);
  page_metadata_t *header_metadata;
  ensure(txn_modify_metadata(tx, container_id, &header_metadata));

  metadata->container.page_flags = page_flags_container;
  metadata->container.prev = previous;
  metadata->container.next = prev_metadata->container.next;
  metadata->container.floor = 0;
  metadata->container.ceiling = PAGE_SIZE;
  metadata->container.free_space = PAGE_SIZE;
  metadata->container.next_allocation_at =
      header_metadata->container.next_allocation_at;
  header_metadata->container.next_allocation_at = p.page_num;
  prev_metadata->container.next = p.page_num;
  *page_num = p.page_num;
  return success();
}

static result_t container_defrag_page(page_metadata_t *metadata,
                                      page_t *p) {
  int32_t max_pos = metadata->container.floor / sizeof(uint16_t);
  int16_t *positions = p->address;
  for (int32_t i = max_pos - 1; i >= 0; i--) {
    if (positions[i] != 0) break;  // clear empties from end
    metadata->container.floor -= sizeof(uint16_t);
    max_pos--;
  }
  void *tmp;
  ensure(mem_alloc_page_aligned(&tmp, PAGE_SIZE));
  defer(free, tmp);
  memcpy(tmp, p->address, PAGE_SIZE);
  max_pos = metadata->container.floor / sizeof(uint16_t);
  metadata->container.ceiling = PAGE_SIZE;
  for (int32_t i = 0; i < max_pos; i++) {
    if (positions[i] == 0) continue;
    uint64_t item_sz;
    void *end = varint_decode(tmp + positions[i], &item_sz) + item_sz;
    uint16_t entry_size = (uint16_t)(end - (tmp + positions[i]));
    metadata->container.ceiling -= entry_size;
    positions[i] = (int16_t)metadata->container.ceiling;
    memcpy(p->address + metadata->container.ceiling,
           tmp + positions[i], entry_size);
  }

  // clear old values
  memset(p->address + metadata->container.floor, 0,
         metadata->container.ceiling - metadata->container.floor);

  return success();
}

static result_t container_find_small_space_to_allocate(
    txn_t *tx, uint64_t container_id, uint64_t required_size,
    uint64_t *page_num) {
  page_metadata_t *header_metadata;
  ensure(txn_get_metadata(tx, container_id, &header_metadata));
  assert(header_metadata->common.page_flags == page_flags_container);

  uint64_t next_alloc = header_metadata->container.next_allocation_at;
  uint64_t prev_page = 0;
  do {
    page_metadata_t *metadata;
    ensure(txn_get_metadata(tx, next_alloc, &metadata));
    assert(metadata->common.page_flags == page_flags_container);
    // found page without enough free space
    if (metadata->container.free_space >= required_size) {
      // is it usable space?
      if (required_size >
          metadata->container.ceiling - metadata->container.floor) {
        ensure(txn_modify_metadata(tx, next_alloc, &metadata));
        page_t p = {.page_num = next_alloc};
        ensure(txn_modify_page(tx, &p));
        ensure(container_defrag_page(metadata, &p));
      }
      // double check, defrag may not be able to free enough space
      if (required_size <=
          metadata->container.ceiling - metadata->container.floor) {
        *page_num = next_alloc;
        return success();
      }
    }
    if (metadata->container.next_allocation_at == 0 ||
        metadata->container.next_allocation_at == next_alloc ||
        // back to start? probably a cycle
        metadata->container.next_allocation_at == container_id)
      break;

    uint64_t old_previous = prev_page;
    prev_page = next_alloc;
    next_alloc = metadata->container.next_allocation_at;
    if (metadata->container.free_space < 256) {
      // the page has < ~3% free space, and didn't fit the current
      // value good indication tha it won't fit _new_ values, let's
      // remove it from the allocation chain
      if (old_previous) {
        page_metadata_t *prev_metadata;
        ensure(txn_modify_metadata(tx, old_previous, &prev_metadata));
        prev_metadata->container.next_allocation_at =
            metadata->container.next_allocation_at;
      }
      ensure(txn_modify_metadata(tx, next_alloc, &metadata));
      metadata->container.next_allocation_at = 0;
    }
  } while (next_alloc);

  // couldn't find enough space anywhere, need to allocate a new one
  ensure(container_allocate_new_page(tx, container_id, next_alloc,
                                     page_num));
  return success();
}

static result_t container_add_item_to_page(txn_t *tx, uint64_t size,
                                           uint64_t page_num,
                                           uint64_t *item_id,
                                           bool is_reference,
                                           void **ptr) {
  page_metadata_t *metadata;
  ensure(txn_modify_metadata(tx, page_num, &metadata));
  assert(metadata->common.page_flags == page_flags_container);
  page_t p = {.page_num = page_num, .number_of_pages = 1};
  ensure(txn_modify_page(tx, &p));

  uint64_t actual_size = container_get_total_size(size);

  int16_t *positions = p.address;
  size_t max_pos = metadata->container.floor / sizeof(uint16_t);
  size_t index = 0;
  for (; index < max_pos; index++) {  // find first empty position
    if (positions[index] == 0) break;
  }
  if (index == max_pos) {  // none found, allocate a new one
    metadata->container.floor += sizeof(uint16_t);
  }
  metadata->container.ceiling -=
      (uint16_t)(size + varint_encoding_length(size));
  metadata->container.free_space -= actual_size;

  void *start = p.address + metadata->container.ceiling;
  *ptr = varint_encode(size, start);
  positions[index] = (int16_t)metadata->container.ceiling;
  if (is_reference) positions[index] *= -1;
  *item_id = page_num * PAGE_SIZE + (index + 1);
  return success();
}

static result_t container_item_allocate(txn_t *tx,
                                        container_item_t *item,
                                        bool is_reference,
                                        void **ptr) {
  uint64_t total_size_required =
      container_get_total_size(item->data.size);

  uint64_t page_num;
  ensure(container_find_small_space_to_allocate(
      tx, item->container_id, total_size_required, &page_num));
  ensure(container_add_item_to_page(tx, item->data.size, page_num,
                                    &item->item_id, is_reference,
                                    ptr));
  return success();
}

static result_t container_item_put_large(txn_t *tx,
                                         container_item_t *item) {
  page_metadata_t *metadata;
  page_t p = {.number_of_pages = (uint32_t)TO_PAGES(item->data.size)};
  ensure(txn_allocate_page(tx, &p, &metadata, item->container_id));
  metadata->overflow.page_flags = page_flags_overflow;
  metadata->overflow.is_container_value = true;
  metadata->overflow.number_of_pages = p.number_of_pages;
  metadata->overflow.size_of_value = item->data.size;
  memcpy(p.address, item->data.address, item->data.size);
  uint8_t buffer[10];
  uint8_t *buffer_end = varint_encode(p.page_num, buffer);
  container_item_t ref = {
      // now wire the other side
      .container_id = item->container_id,
      .data = {.address = buffer,
               .size = (size_t)(buffer_end - buffer)}};
  void *ptr;
  ensure(container_item_allocate(tx, &ref, /*is_ref*/ true, &ptr));
  memcpy(ptr, buffer, ref.data.size);
  metadata->overflow.container_item_id = ref.item_id;
  item->item_id = p.page_num * PAGE_SIZE;
  return success();
}

result_t container_item_put(txn_t *tx, container_item_t *item) {
  if (item->data.size > CONTAINER_ITEM_SMALL_MAX_SIZE) {
    ensure(container_item_put_large(tx, item));
    return success();
  }
  void *ptr;
  ensure(container_item_allocate(tx, item, /*is_ref*/ false, &ptr));
  memcpy(ptr, item->data.address, item->data.size);
  return success();
}

result_t container_item_get(txn_t *tx, container_item_t *item) {
  page_metadata_t *metadata;
  if (item->item_id % PAGE_SIZE == 0) {
    // large item
    page_t p = {.page_num = item->item_id / PAGE_SIZE};
    ensure(txn_get_page_and_metadata(tx, &p, &metadata));
    assert(metadata->overflow.page_flags == page_flags_overflow);
    assert(metadata->overflow.is_container_value);
    item->data.address = p.address;
    item->data.size = metadata->overflow.size_of_value;
  } else {
    uint64_t index = (item->item_id % PAGE_SIZE) - 1;
    page_t p = {.page_num = item->item_id / PAGE_SIZE};
    ensure(txn_get_page_and_metadata(tx, &p, &metadata));
    assert(metadata->container.page_flags == page_flags_container);
    int16_t *positions = p.address;
    ensure(positions[index] > 0, msg("invalid item_id"),
           with(item->item_id, "%lu"));
    item->data.address =
        varint_decode(p.address + positions[index], &item->data.size);
  }
  return success();
}

static result_t container_remove_page(txn_t *tx,
                                      uint64_t container_id,
                                      page_t *p,
                                      page_metadata_t *metadata) {
  page_metadata_t *prev, *next, *header;
  ensure(txn_modify_metadata(tx, metadata->container.prev, &prev));
  prev->container.next = metadata->container.next;
  if (metadata->container.next) {
    ensure(txn_modify_metadata(tx, metadata->container.prev, &next));
    prev->container.prev = metadata->container.prev;
  }
  ensure(txn_get_metadata(tx, container_id, &header));
  if (header->container.next_allocation_at == p->page_num) {
    ensure(txn_modify_metadata(tx, container_id, &header));
    header->container.next_allocation_at = metadata->container.prev;
  }
  ensure(txn_free_page(tx, p));
  return success();
}
result_t container_item_del(txn_t *tx, container_item_t *item) {
  page_metadata_t *metadata;
  if (item->item_id % PAGE_SIZE == 0) {
    // large item
    page_t p = {.page_num = item->item_id};
    ensure(txn_get_page_and_metadata(tx, &p, &metadata));
    assert(metadata->overflow.page_flags == page_flags_overflow);
    assert(metadata->overflow.is_container_value);
    container_item_t ref = {
        .container_id = item->container_id,
        .item_id = metadata->overflow.container_item_id};
    ensure(container_item_del(tx, &ref));
    ensure(txn_free_page(tx, &p));
  } else {
    uint64_t index = (item->item_id % PAGE_SIZE) - 1;
    page_t p = {.page_num = item->item_id / PAGE_SIZE};
    ensure(txn_modify_page(tx, &p));
    ensure(txn_modify_metadata(tx, p.page_num, &metadata));
    assert(metadata->container.page_flags == page_flags_container);
    int16_t *positions = p.address;
    uint64_t size;
    void *end =
        varint_decode(p.address + positions[index], &size) + size;
    size_t item_size = (size_t)(end - p.address - positions[index]);
    memset(p.address + positions[index], 0, item_size);
    positions[index] = 0;
    metadata->container.free_space +=
        (uint16_t)(item_size + sizeof(uint16_t));

    // can delete this whole page?
    if (metadata->container.free_space == PAGE_SIZE &&
        p.page_num != item->container_id) {
      ensure(container_remove_page(tx, item->container_id, &p,
                                   metadata));
    } else if (metadata->container.next_allocation_at == 0 &&
               metadata->container.free_space > 512) {
      // need to wire this again to the allocation chain
      page_metadata_t *header;
      ensure(txn_modify_metadata(tx, item->container_id, &header));
      metadata->container.next_allocation_at =
          header->container.next_allocation_at;
      header->container.next_allocation_at = p.page_num;
    }
  }
  return success();
}

result_t container_get_next(txn_t *tx, container_item_t *item) {
  uint64_t page_num;
  if (item->item_id && item->item_id % PAGE_SIZE == 0) {
    // large item, resolve the small id
    page_metadata_t *metadata;
    ensure(
        txn_get_metadata(tx, item->item_id / PAGE_SIZE, &metadata));
    assert(metadata->overflow.is_container_value);
    item->item_id = metadata->overflow.container_item_id;
  }
  if (item->item_id == 0) {  // first time
    page_num = item->container_id;
    item->item_id = item->container_id * PAGE_SIZE;
  } else {
    page_num = item->item_id / PAGE_SIZE;
  }

  while (page_num) {
    page_metadata_t *metadata;
    page_t p = {.page_num = item->item_id / PAGE_SIZE};
    ensure(txn_get_page_and_metadata(tx, &p, &metadata));
    assert(metadata->common.page_flags == page_flags_container);
    size_t max_pos = metadata->container.floor / sizeof(uint16_t);
    int16_t *positions = p.address;
    // the item_id is + 1, we intentionally don't correct it to scan
    // the next item
    for (size_t i = item->item_id % PAGE_SIZE; i < max_pos; i++) {
      if (positions[i] == 0) continue;
      if (positions[i] < 0) {
        // large reference
        uint64_t size;
        varint_decode(varint_decode(p.address + -positions[i], &size),
                      &item->item_id);
        item->item_id *= PAGE_SIZE;
        ensure(container_item_get(tx, item));
        return success();
      }
      item->item_id = p.page_num * PAGE_SIZE + i + 1;
      item->data.address =
          varint_decode(p.address + positions[i], &item->data.size);
      return success();
    }
    page_num = metadata->container.next;
    item->item_id = page_num * PAGE_SIZE;
  }
  memset(&item->data, 0, sizeof(span_t));
  item->item_id = 0;
  return success();
}

static result_t container_item_replace(txn_t *tx,
                                       container_item_t *item,
                                       bool *in_place) {
  *in_place = false;
  container_item_t del = {.container_id = item->container_id,
                          .item_id = item->item_id};
  ensure(container_item_del(tx, &del));
  ensure(container_item_put(tx, item));
  return success();
}
static result_t container_item_update_large(txn_t *tx,
                                            container_item_t *item,
                                            bool *in_place) {
  page_metadata_t *metadata;
  uint64_t page_num = item->item_id / PAGE_SIZE;
  ensure(txn_modify_metadata(tx, page_num, &metadata));
  assert(metadata->overflow.is_container_value);
  uint32_t pages = (uint32_t)TO_PAGES(item->data.size);
  page_t p = {.page_num = page_num};
  if (pages == metadata->overflow.number_of_pages) {
    // same size in pages, easiest
    ensure(txn_modify_page(tx, &p));
  } else if (pages < metadata->overflow.number_of_pages) {
    // we can _free_ space, but keep same item_id
    for (size_t i = pages; i < metadata->overflow.number_of_pages;
         i++) {
      page_t free = {.page_num = i + page_num};
      ensure(txn_free_page(tx, &free));
    }
    // important, modify page after we adjust the number_of_pages
    metadata->overflow.number_of_pages = pages;
    ensure(txn_modify_page(tx, &p));
  } else {
    return container_item_replace(tx, item, in_place);
  }
  metadata->overflow.size_of_value = item->data.size;
  memcpy(p.address, item->data.address, item->data.size);
  memset(p.address + item->data.size, 0,  // zero remaining buffer
         PAGE_SIZE - (item->data.size % PAGE_SIZE));
  return success();
}
static result_t container_item_update_small_size_increase(
    txn_t *tx, container_item_t *item, bool *in_place, page_t *p,
    page_metadata_t *metadata, size_t old_item_size) {
  uint64_t index = item->item_id % PAGE_SIZE - 1;
  uint64_t old_total_size = container_get_total_size(old_item_size);
  uint64_t required_size = container_get_total_size(item->data.size);
  if (required_size >
      metadata->container.free_space + old_total_size) {
    // no space for this, have to move
    return container_item_replace(tx, item, in_place);
  }
  // zero old value
  int16_t *positions = p->address;
  memset(p->address + positions[index], 0,
         old_total_size - sizeof(int16_t));
  uint64_t just_item_size = required_size - sizeof(int16_t);
  if (just_item_size >
      metadata->container.ceiling - metadata->container.floor) {
    ensure(container_defrag_page(metadata, p));
    // we may still lack space afterward
    if (just_item_size >
        metadata->container.ceiling - metadata->container.floor) {
      return container_item_replace(tx, item, in_place);
    }
  }
  // has room to fit
  metadata->container.ceiling -= just_item_size;
  metadata->container.free_space -= required_size - old_total_size;
  positions[index] = (int16_t)metadata->container.ceiling;
  void *data_start =
      varint_encode(item->data.size, p->address + positions[index]);
  memcpy(data_start, item->data.address, item->data.size);
  return success();
}

static result_t container_item_update_small(txn_t *tx,
                                            container_item_t *item,
                                            bool *in_place) {
  page_t p = {.page_num = item->item_id / PAGE_SIZE};
  uint64_t index = item->item_id % PAGE_SIZE - 1;
  ensure(txn_modify_page(tx, &p));
  int16_t *positions = p.address;
  uint64_t old_item_size;
  varint_decode(p.address + positions[index], &old_item_size);
  if (item->data.size == old_item_size) {
    memcpy(p.address + positions[index] +
               varint_encoding_length(item->data.size),
           item->data.address, item->data.size);
    return success();
  }
  page_metadata_t *metadata;
  ensure(txn_modify_metadata(tx, p.page_num, &metadata));
  if (item->data.size < old_item_size) {
    void *data_start =
        varint_encode(item->data.size, p.address + positions[index]);
    memcpy(data_start, item->data.address, item->data.size);
    memset(data_start + item->data.size, 0,
           old_item_size - item->data.size);
    metadata->container.free_space += old_item_size - item->data.size;
    return success();
  }
  return container_item_update_small_size_increase(
      tx, item, in_place, &p, metadata, old_item_size);
}

result_t container_item_update(txn_t *tx, container_item_t *item,
                               bool *in_place) {
  *in_place = true;
  if (item->item_id % PAGE_SIZE == 0) {
    if (item->data.size <= CONTAINER_ITEM_SMALL_MAX_SIZE) {
      // large to small, can't update in place
      return container_item_replace(tx, item, in_place);
    }
    return container_item_update_large(tx, item, in_place);
  } else {
    // small to large, cannot update in place
    if (item->data.size > CONTAINER_ITEM_SMALL_MAX_SIZE) {
      return container_item_replace(tx, item, in_place);
    }
    return container_item_update_small(tx, item, in_place);
  }
}