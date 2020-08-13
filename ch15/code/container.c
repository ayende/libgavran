#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

// tag::CONTAINER_ITEM_SMALL_MAX_SIZE[]
#define CONTAINER_ITEM_SMALL_MAX_SIZE (6 * 1024)
// end::CONTAINER_ITEM_SMALL_MAX_SIZE[]

// tag::container_get_total_size[]
static inline size_t container_get_total_size(size_t size) {
  return sizeof(int16_t) +          // offset to value
         varint_get_length(size) +  // varint len
         size;
}
// end::container_get_total_size[]

// tag::container_create[]
result_t container_create(txn_t *tx, uint64_t *container_id) {
  page_t p = {.number_of_pages = 1};
  page_metadata_t *metadata;
  ensure(txn_allocate_page(tx, &p, &metadata, 0));
  metadata->container.page_flags = page_flags_container;
  metadata->container.floor      = sizeof(uint16_t);
  metadata->container.ceiling    = PAGE_SIZE;
  metadata->container.free_space = PAGE_SIZE - sizeof(int16_t);

  hash_val_t set = {.key = p.page_num, .val = 0};
  ensure(hash_create(tx, &set.hash_id));
  ensure(hash_set(tx, &set, 0));
  metadata->container.free_list = set.hash_id;

  *container_id = p.page_num;
  return success();
}
// end::container_create[]

// tag::container_drop[]
result_t container_drop(txn_t *tx, uint64_t container_id) {
  uint64_t page_num = container_id;
  page_metadata_t *header;
  ensure(txn_get_metadata(tx, container_id, &header));
  ensure(hash_drop(tx, header->container.free_list));
  while (page_num) {
    page_t p = {.page_num = page_num};
    page_metadata_t *metadata;
    ensure(txn_get_page_and_metadata(tx, &p, &metadata));
    size_t max_pos     = metadata->container.floor / sizeof(int16_t);
    int16_t *positions = p.address;
    for (size_t i = 0; i < max_pos; i++) {
      if (positions[i] >= 0) continue;
      uint64_t size;  // large reference
      uint64_t overflow_page_num;
      varint_decode(varint_decode(p.address + -positions[i], &size),
          &overflow_page_num);
      page_t overflow = {.page_num = overflow_page_num};
      ensure(txn_free_page(tx, &overflow));
    }
    page_num = metadata->container.next;
    ensure(txn_free_page(tx, &p));
  }
  return success();
}
// end::container_drop[]

// tag::container_allocate_new_page[]
static result_t container_allocate_new_page(
    txn_t *tx, uint64_t container_id, uint64_t *page_num) {
  page_t p = {.number_of_pages = 1};
  page_metadata_t *metadata;
  ensure(txn_allocate_page(tx, &p, &metadata, container_id));
  page_metadata_t *header_metadata;
  ensure(txn_modify_metadata(tx, container_id, &header_metadata));

  metadata->container.page_flags  = page_flags_container;
  metadata->container.prev        = container_id;
  metadata->container.next        = header_metadata->container.next;
  metadata->container.floor       = 0;
  metadata->container.ceiling     = PAGE_SIZE;
  metadata->container.free_space  = PAGE_SIZE;
  header_metadata->container.next = p.page_num;

  hash_val_t set = {.hash_id = header_metadata->container.free_list,
      .key                   = p.page_num,
      .val                   = 0};
  ensure(hash_set(tx, &set, 0));
  header_metadata->container.free_list = set.hash_id;

  *page_num = p.page_num;
  return success();
}
// end::container_allocate_new_page[]

// tag::container_defrag_page[]
static result_t container_defrag_page(
    page_metadata_t *metadata, page_t *p) {
  int32_t max_pos    = metadata->container.floor / sizeof(uint16_t);
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
    int16_t offset = positions[i];
    if (offset < 0) offset *= -1;
    void *end = varint_decode(tmp + offset, &item_sz) + item_sz;
    uint16_t entry_size = (uint16_t)(end - (tmp + offset));
    metadata->container.ceiling -= entry_size;
    memcpy(p->address + metadata->container.ceiling,
        tmp + positions[i], entry_size);
    positions[i] = (int16_t)metadata->container.ceiling;
  }
  // clear old values
  memset(p->address + metadata->container.floor, 0,
      metadata->container.ceiling - metadata->container.floor);
  return success();
}
// end::container_defrag_page[]

// tag::container_page_can_fit_item_size[]
static result_t container_page_can_fit_item_size(txn_t *tx,
    uint64_t page_num, page_metadata_t *metadata,
    uint64_t required_size, bool *is_match) {
  *is_match = false;
  if (required_size > metadata->container.free_space) {
    return success();
  }
  if (required_size >  // is it *usable* space?
      metadata->container.ceiling - metadata->container.floor) {
    ensure(txn_modify_metadata(tx, page_num, &metadata));
    page_t p = {.page_num = page_num};
    ensure(txn_modify_page(tx, &p));
    ensure(container_defrag_page(metadata, &p));
  }
  // double check, defrag may not be able to free enough space
  if (required_size <=
      metadata->container.ceiling - metadata->container.floor) {
    *is_match = true;
    return success();
  }
  return success();
}
// end::container_page_can_fit_item_size[]

// tag::container_remove_full_pages[]
static result_t container_remove_full_pages(
    txn_t *tx, uint64_t container_id, pages_map_t *to_remove) {
  if (!to_remove) return success();
  size_t iter_state = 0;
  page_t *p;
  page_metadata_t *header_metadata;
  ensure(txn_modify_metadata(tx, container_id, &header_metadata));
  while (pagesmap_get_next(to_remove, &iter_state, &p)) {
    hash_val_t del = {.hash_id = header_metadata->container.free_list,
        .key                   = p->page_num};
    ensure(hash_del(tx, &del));
    header_metadata->container.free_list = del.hash_id;
  }
  return success();
}
// end::container_remove_full_pages[]

// tag::container_get_page_avg_item_size[]
static uint32_t container_get_page_avg_item_size(
    page_t *p, page_metadata_t *metadata) {
  size_t max_pos     = metadata->container.floor / sizeof(uint16_t);
  int16_t *positions = p->address;
  uint32_t sizes = 0, count = 0;
  for (size_t i = 0; i < max_pos; i++) {
    if (!positions[i]) continue;
    uint64_t item_sz;
    int16_t offset = positions[i];
    if (offset < 0) offset *= -1;
    void *end =
        varint_decode(p->address + offset, &item_sz) + item_sz;
    uint16_t entry_size = (uint16_t)(end - (p->address + offset));
    sizes += entry_size;
    count++;
  }
  if (count == 0) count = 1;
  return sizes / count;
}
// end::container_get_page_avg_item_size[]

// tag::container_find_small_space_to_allocate[]
static result_t container_find_small_space_to_allocate(txn_t *tx,
    uint64_t container_id, uint64_t required_size,
    uint64_t *page_num) {
  page_metadata_t *header_metadata;
  ensure(txn_get_metadata(tx, container_id, &header_metadata));
  pages_map_t *pages, *to_remove = 0;
  ensure(pagesmap_new(8, &pages));
  defer(free, pages);
  defer(free, to_remove);
  hash_val_t it = {.hash_id = header_metadata->container.free_list};
  *page_num     = 0;
  while (true) {
    ensure(hash_get_next(tx, &pages, &it));
    if (it.has_val == false) break;
    page_metadata_t *metadata;
    ensure(txn_get_metadata(tx, it.key, &metadata));
    bool has_enough_space;
    ensure(container_page_can_fit_item_size(
        tx, it.key, metadata, required_size, &has_enough_space));
    if (has_enough_space) {
      *page_num = it.key;
      break;
    }
    page_t p = {.page_num = it.key};
    ensure(txn_get_page(tx, &p));
    uint32_t avg_size =
        container_get_page_avg_item_size(&p, metadata);
    if (metadata->container.free_space >= (avg_size + avg_size / 4))
      continue;
    if (!to_remove) {
      ensure(pagesmap_new(8, &to_remove));
    }
    ensure(pagesmap_put_new(&to_remove, &p));
  }
  ensure(container_remove_full_pages(tx, container_id, to_remove));
  if (!*page_num) {  // couldn't find matching page, allocate new one
    ensure(container_allocate_new_page(tx, container_id, page_num));
  }
  return success();
}
// end::container_find_small_space_to_allocate[]

// tag::container_add_item_to_page[]
static result_t container_add_item_to_page(txn_t *tx, span_t *item,
    uint64_t page_num, uint64_t *item_id, bool is_reference) {
  page_metadata_t *metadata;
  ensure(txn_modify_metadata(tx, page_num, &metadata));
  assert(metadata->common.page_flags == page_flags_container);
  page_t p = {.page_num = page_num, .number_of_pages = 1};
  ensure(txn_modify_page(tx, &p));
  uint64_t actual_size = container_get_total_size(item->size);
  int16_t *positions   = p.address;
  size_t max_pos       = metadata->container.floor / sizeof(uint16_t);
  size_t index         = 0;
  for (; index < max_pos; index++) {  // find first empty position
    if (positions[index] == 0) break;
  }
  if (index == max_pos) {  // none found, allocate a new one
    metadata->container.floor += sizeof(uint16_t);
  }
  metadata->container.ceiling -=
      (uint16_t)(item->size + varint_get_length(item->size));
  metadata->container.free_space -= actual_size;

  void *start      = p.address + metadata->container.ceiling;
  item->address    = varint_encode(item->size, start);
  positions[index] = (int16_t)metadata->container.ceiling;
  if (is_reference) positions[index] *= -1;
  *item_id = page_num * PAGE_SIZE + (index + 1);
  return success();
}
// end::container_add_item_to_page[]

// tag::container_item_allocate[]
static result_t container_item_allocate(txn_t *tx,
    container_item_t *item, span_t *data, bool is_reference) {
  uint64_t total_size_required = container_get_total_size(data->size);

  uint64_t page_num;
  ensure(container_find_small_space_to_allocate(
      tx, item->container_id, total_size_required, &page_num));
  ensure(container_add_item_to_page(
      tx, data, page_num, &item->item_id, is_reference));
  return success();
}
// end::container_item_allocate[]

// tag::container_item_put_large[]
static result_t container_item_put_large(
    txn_t *tx, container_item_t *item) {
  page_metadata_t *metadata;
  page_t p = {.number_of_pages = (uint32_t)TO_PAGES(item->data.size)};
  ensure(txn_allocate_page(tx, &p, &metadata, item->container_id));
  metadata->overflow.page_flags         = page_flags_overflow;
  metadata->overflow.is_container_value = true;
  metadata->overflow.number_of_pages    = p.number_of_pages;
  metadata->overflow.size_of_value      = item->data.size;
  memcpy(p.address, item->data.address, item->data.size);
  uint8_t buffer[10];
  uint8_t *buffer_end  = varint_encode(p.page_num, buffer);
  container_item_t ref = {// now wire the other side
      .container_id = item->container_id,
      .data         = {
          .address = buffer, .size = (size_t)(buffer_end - buffer)}};
  span_t data          = {.size = ref.data.size};
  ensure(container_item_allocate(tx, &ref, &data, /*is_ref*/ true));
  memcpy(data.address, buffer, ref.data.size);
  metadata->overflow.container_item_id = ref.item_id;
  item->item_id                        = p.page_num * PAGE_SIZE;
  return success();
}
// end::container_item_put_large[]

// tag::container_item_put[]
result_t container_item_put(txn_t *tx, container_item_t *item) {
  if (item->data.size > CONTAINER_ITEM_SMALL_MAX_SIZE) {
    ensure(container_item_put_large(tx, item));
    return success();
  }
  span_t span = {.size = item->data.size};
  ensure(container_item_allocate(tx, item, &span, /*is_ref*/ false));
  memcpy(span.address, item->data.address, item->data.size);
  return success();
}
// end::container_item_put[]

// tag::container_item_get[]
result_t container_item_get(txn_t *tx, container_item_t *item) {
  page_metadata_t *metadata;
  if (item->item_id % PAGE_SIZE == 0) {
    // large item
    page_t p = {.page_num = item->item_id / PAGE_SIZE};
    ensure(txn_get_page_and_metadata(tx, &p, &metadata));
    assert(metadata->overflow.page_flags == page_flags_overflow);
    assert(metadata->overflow.is_container_value);
    item->data.address = p.address;
    item->data.size    = metadata->overflow.size_of_value;
  } else {
    uint64_t index = (item->item_id % PAGE_SIZE) - 1;
    page_t p       = {.page_num = item->item_id / PAGE_SIZE};
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
// end::container_item_get[]

// tag::container_remove_page[]
static result_t container_remove_page(txn_t *tx,
    uint64_t container_id, page_t *p, page_metadata_t *metadata) {
  page_metadata_t *prev, *next, *header;
  ensure(txn_modify_metadata(tx, metadata->container.prev, &prev));
  prev->container.next = metadata->container.next;
  if (metadata->container.next) {
    ensure(txn_modify_metadata(tx, metadata->container.prev, &next));
    prev->container.prev = metadata->container.prev;
  }
  ensure(txn_get_metadata(tx, container_id, &header));
  hash_val_t del = {
      .hash_id = header->container.free_list, .key = p->page_num};
  ensure(hash_del(tx, &del));
  if (del.hash_id_changed) {
    ensure(txn_modify_metadata(tx, container_id, &header));
    header->container.free_list = del.hash_id;
  }
  ensure(txn_free_page(tx, p));
  return success();
}
// end::container_remove_page[]

// tag::container_item_del_finalize[]
static result_t container_item_del_finalize(txn_t *tx,
    container_item_t *item, page_t *p, page_metadata_t *m) {
  // can delete this whole page?
  if (m->container.free_space == PAGE_SIZE &&
      p->page_num != item->container_id) {
    ensure(container_remove_page(tx, item->container_id, p, m));
  } else if (m->container.free_space > item->data.size * 2) {
    // need to wire this again to the allocation chain
    page_metadata_t *header;
    ensure(txn_get_metadata(tx, item->container_id, &header));
    hash_val_t kvp = {.hash_id = header->container.free_list,
        .key                   = p->page_num,
        .val                   = 0};
    ensure(hash_get(tx, &kvp));
    if (kvp.has_val == false) {
      ensure(hash_set(tx, &kvp, 0));
      if (kvp.hash_id_changed) {
        ensure(txn_modify_metadata(tx, item->container_id, &header));
        header->container.free_list = kvp.hash_id;
      }
    }
  }
  return success();
}
// end::container_item_del_finalize[]

// tag::container_item_del[]
result_t container_item_del(txn_t *tx, container_item_t *item) {
  page_metadata_t *metadata;
  if (item->item_id % PAGE_SIZE == 0) {
    // large item
    page_t p = {.page_num = item->item_id};
    ensure(txn_get_page_and_metadata(tx, &p, &metadata));
    assert(metadata->overflow.page_flags == page_flags_overflow);
    assert(metadata->overflow.is_container_value);
    container_item_t ref = {.container_id = item->container_id,
        .item_id = metadata->overflow.container_item_id};
    ensure(container_item_del(tx, &ref));
    ensure(txn_free_page(tx, &p));
  } else {
    uint64_t index = (item->item_id % PAGE_SIZE) - 1;
    page_t p       = {.page_num = item->item_id / PAGE_SIZE};
    ensure(txn_modify_page(tx, &p));
    ensure(txn_modify_metadata(tx, p.page_num, &metadata));
    assert(metadata->container.page_flags == page_flags_container);
    int16_t *positions = p.address;
    uint64_t size;
    void *end =
        varint_decode(p.address + positions[index], &size) + size;
    item->data.size = (size_t)(end - p.address - positions[index]);
    memset(p.address + positions[index], 0, item->data.size);
    positions[index] = 0;
    metadata->container.free_space +=
        (uint16_t)(item->data.size + sizeof(uint16_t));
    ensure(container_item_del_finalize(tx, item, &p, metadata));
  }
  return success();
}
// end::container_item_del[]

// tag::container_get_next_item_id[]
static result_t container_get_next_item_id(
    txn_t *tx, container_item_t *item, uint64_t *page_num) {
  if (item->item_id && item->item_id % PAGE_SIZE == 0) {
    // large item, resolve the small id
    page_metadata_t *m;
    ensure(txn_get_metadata(tx, item->item_id / PAGE_SIZE, &m));
    assert(m->overflow.is_container_value);
    item->item_id = m->overflow.container_item_id;
  }
  if (item->item_id == 0) {  // first time
    item->item_id = item->container_id * PAGE_SIZE + 1;
    *page_num     = item->container_id;
  } else {
    *page_num = item->item_id / PAGE_SIZE;
    item->item_id++;  // point to the _next_ item
  }
  return success();
}
// end::container_get_next_item_id[]

// tag::container_get_next[]
result_t container_get_next(txn_t *tx, container_item_t *item) {
  uint64_t page_num;
  ensure(container_get_next_item_id(tx, item, &page_num));
  while (page_num) {
    page_metadata_t *metadata;
    page_t p = {.page_num = page_num};
    ensure(txn_get_page_and_metadata(tx, &p, &metadata));
    assert(metadata->common.page_flags == page_flags_container);
    size_t max_pos     = metadata->container.floor / sizeof(uint16_t);
    int16_t *positions = p.address;
    for (size_t i = (item->item_id % PAGE_SIZE) - 1; i < max_pos;
         i++) {
      if (positions[i] == 0) continue;
      if (positions[i] < 0) {
        uint64_t size;  // large reference
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
    page_num      = metadata->container.next;
    item->item_id = page_num * PAGE_SIZE;
  }
  memset(&item->data, 0, sizeof(span_t));
  item->item_id = 0;
  return success();
}
// end::container_get_next[]

// tag::container_item_replace[]
static result_t container_item_replace(
    txn_t *tx, container_item_t *item, bool *in_place) {
  *in_place            = false;
  container_item_t del = {
      .container_id = item->container_id, .item_id = item->item_id};
  ensure(container_item_del(tx, &del));
  ensure(container_item_put(tx, item));
  return success();
}
// end::container_item_replace[]

// tag::container_item_update_large[]
static result_t container_item_update_large(
    txn_t *tx, container_item_t *item, bool *in_place) {
  page_metadata_t *metadata;
  uint64_t page_num = item->item_id / PAGE_SIZE;
  ensure(txn_modify_metadata(tx, page_num, &metadata));
  assert(metadata->overflow.is_container_value);
  uint32_t pages = (uint32_t)TO_PAGES(item->data.size);
  page_t p       = {.page_num = page_num};
  if (pages == metadata->overflow.number_of_pages) {
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
// end::container_item_update_large[]

// tag::container_item_update_small_size_increase[]
static result_t container_item_update_small_size_increase(txn_t *tx,
    container_item_t *item, bool *in_place, page_t *p,
    page_metadata_t *m, size_t old_item_size) {
  uint64_t index          = item->item_id % PAGE_SIZE - 1;
  uint64_t old_total_size = container_get_total_size(old_item_size);
  uint64_t required_size  = container_get_total_size(item->data.size);
  if (required_size > m->container.free_space + old_total_size) {
    return container_item_replace(tx, item, in_place);  // has to move
  }
  int16_t *positions = p->address;
  memset(p->address + positions[index], 0,  // zero old value
      old_total_size - sizeof(int16_t));
  uint64_t just_item_size = required_size - sizeof(int16_t);
  if (just_item_size > m->container.ceiling - m->container.floor) {
    ensure(container_defrag_page(m, p));
    // we may still lack space afterward
    if (just_item_size > m->container.ceiling - m->container.floor) {
      return container_item_replace(tx, item, in_place);
    }
  }
  m->container.ceiling -= just_item_size;
  m->container.free_space -= required_size - old_total_size;
  positions[index] = (int16_t)m->container.ceiling;
  void *data_start =
      varint_encode(item->data.size, p->address + positions[index]);
  memcpy(data_start, item->data.address, item->data.size);
  return success();
}
// end::container_item_update_small_size_increase[]

// tag::container_item_update_small[]
static result_t container_item_update_small(
    txn_t *tx, container_item_t *item, bool *in_place) {
  page_t p       = {.page_num = item->item_id / PAGE_SIZE};
  uint64_t index = item->item_id % PAGE_SIZE - 1;
  ensure(txn_modify_page(tx, &p));
  int16_t *positions = p.address;
  uint64_t old_item_size;
  varint_decode(p.address + positions[index], &old_item_size);
  if (item->data.size == old_item_size) {
    memcpy(p.address + positions[index] +
               varint_get_length(item->data.size),
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
// end::container_item_update_small[]

// tag::container_item_update[]
result_t container_item_update(
    txn_t *tx, container_item_t *item, bool *in_place) {
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
// end::container_item_update[]