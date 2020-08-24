#include <gavran/internal.h>

// tag::bitmap_finalize_match[]
static bool bitmap_finalize_match(bitmap_search_state_t *search) {
  if (search->internal.current_set_bit >
      search->internal.previous_set_bit +
          search->input.space_required) {
    // intentionally overflowing here
    search->output.found_position =
        search->internal.previous_set_bit + 1;
    search->output.space_available_at_position =
        (search->internal.current_set_bit -
            search->output.found_position);
    return true;
  }
  return false;
}
// end::bitmap_finalize_match[]

// tag::bitmap_search_word[]
static bool bitmap_search_word(bitmap_search_state_t *search) {
  uint64_t bitset = search->internal.current_word;

  if (bitset == ULONG_MAX) {
    // all bits are set, can skip whole thing
    bitmap_finalize_match(search);
    search->internal.previous_set_bit =
        (search->internal.index + 1) * 64 - 1;
    return false;
  }

  if (bitset == 0) {
    search->internal.current_set_bit =
        (search->internal.index + 1) * 64;
    return bitmap_finalize_match(search);
  }

  while (bitset != 0) {
    int r = __builtin_ctzl(bitset);
    search->internal.current_set_bit =
        (search->internal.index) * 64 + (uint64_t)r;
    if (search->internal.current_set_bit >
        search->internal.previous_set_bit +
            search->input.space_required) {
      // intentionally overflowing here
      search->output.found_position =
          search->internal.previous_set_bit + 1;
      search->output.space_available_at_position =
          (search->internal.current_set_bit -
              search->output.found_position);
      search->internal.previous_set_bit =
          search->internal.current_set_bit;
      return true;
    }
    search->internal.previous_set_bit =
        search->internal.current_set_bit;
    bitset ^= (bitset & -bitset);
  }

  search->internal.current_set_bit =
      (search->internal.index + 1) * 64;
  return bitmap_finalize_match(search);
}
// end::bitmap_search_word[]

// tag::bitmap_search_once[]
static bool bitmap_search_once(bitmap_search_state_t *search) {
  uint64_t original_pos = search->output.found_position;
  do {
    if (bitmap_search_word(search)) {
      if (search->internal.current_set_bit % 64) {
        // mask the already found item
        uint64_t mask =
            ~(ULONG_MAX << (search->internal.current_set_bit % 64));
        search->internal.current_word |= mask;
      } else {
        // run out current word, but maybe we have more in the next?
        uint64_t next = search->internal.index + 1;
        if (next < search->input.bitmap_size &&
            (search->input.bitmap[next] & 1) == 0) {
          search->internal.index++;
          search->internal.current_word =
              search->input.bitmap[search->internal.index];
          continue;
        } else {
          search->internal.current_word = ULONG_MAX;
        }
      }
      if (!bitmap_is_acceptable_match(search)) continue;
      return true;
    }
    if (original_pos != search->output.found_position &&
        bitmap_is_acceptable_match(search))
      return true;
    search->internal.index++;
    if (search->internal.index >= search->input.bitmap_size)
      return false;
    search->internal.current_word =
        search->input.bitmap[search->internal.index];
  } while (true);
}
// end::bitmap_search_once[]

// tag::bitmap_search_smallest_nearby[]
#define MAX_SEARCH_DISTANCE 64
static bool bitmap_search_smallest_nearby(
    bitmap_search_state_t *search) {
  uint64_t current_pos  = 0;
  uint64_t current_size = ULONG_MAX;

  // the bigger the request range, the less we care about locality
  size_t boundary =
      (search->input.near_position + MAX_SEARCH_DISTANCE +
          search->input.space_required);
  while (bitmap_search_once(search)) {
    if (search->input.space_required ==
        search->output.space_available_at_position)
      return true;  // perfect match!
    if (current_size > search->output.space_available_at_position) {
      current_size = search->output.space_available_at_position;
      current_pos  = search->output.found_position;
    }
    if (search->input.near_position &&
        search->output.found_position > boundary) {
      // We have gone too far? Stop being choosy
      if (current_size < search->output.space_available_at_position) {
        search->output.space_available_at_position = current_size;
        search->output.found_position              = current_pos;
      }
      return true;
    }
  }

  search->output.space_available_at_position = current_size;
  search->output.found_position              = current_pos;

  return current_size != ULONG_MAX;
}
// end::bitmap_search_smallest_nearby[]

// tag::bitmap_search[]
bool bitmap_search(bitmap_search_state_t *search) {
  if (!search->input.space_required ||
      search->input.near_position / 64 >= search->input.bitmap_size)
    return false;

  search->internal.current_word     = search->input.bitmap[0];
  search->internal.previous_set_bit = ULONG_MAX;

  search->internal.search_offset = search->input.near_position / 64;

  void *old_bitmap  = search->input.bitmap;
  uint64_t old_size = search->input.bitmap_size;

  search->input.bitmap += search->internal.search_offset;
  search->input.bitmap_size -= search->internal.search_offset;
  search->internal.search_offset *= 64;  //  pages instead of words

  if (bitmap_search_smallest_nearby(search)) {
    search->output.found_position += search->internal.search_offset;
    return true;
  }
  if (!search->internal.search_offset) {
    return false;  // already scanned it all
  }

  search->input.bitmap      = old_bitmap;
  search->input.bitmap_size = MIN(old_size,
      search->internal.search_offset + search->input.space_required);

  // we search _after_, couldn't find anything, maybe before?
  return bitmap_search_once(search);
}
// end::bitmap_search[]
