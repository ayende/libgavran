#include <gavran/internal.h>

// tag::bitmap_is_acceptable_small_match[]
static bool bitmap_is_acceptable_small_match(
    bitmap_search_state_t *s) {
  if (!(s->output.found_position & ~PAGES_IN_METADATA_MASK)) {
    // cannot use, falls on metadata page, try to shift it
    s->output.found_position++;
    s->output.space_available_at_position--;
    // may fail if there isn't enough room now
    return (s->input.space_required >=
            s->output.space_available_at_position);
  }
  uint64_t start = s->output.found_position & PAGES_IN_METADATA_MASK;
  uint64_t end =
      (s->output.found_position + s->input.space_required - 1) &
      PAGES_IN_METADATA_MASK;
  if (start == end)  // on the same MB, nothing to do
    return true;
  // past the next metadata page
  uint64_t new_start = start + PAGES_IN_METADATA + 1;
  if (new_start + s->input.space_required >
      s->output.found_position +
          s->output.space_available_at_position) {
    // not enough space to shift things
    return false;
  }
  s->output.space_available_at_position -=
      (new_start - s->output.found_position);
  s->output.found_position = new_start;
  return true;
}
// end::bitmap_is_acceptable_small_match[]

// tag::bitmap_is_acceptable_match[]
implementation_detail bool bitmap_is_acceptable_match(
    bitmap_search_state_t *s) {
  if (s->input.space_required > s->output.space_available_at_position)
    return false;
  if (s->input.space_required < PAGES_IN_METADATA) {
    return bitmap_is_acceptable_small_match(s);
  }
  // large values here, size is guranteed to *not* be  128 multiple
  size_t size =
      (s->output.found_position + s->input.space_required + 1);
  if ((size % PAGES_IN_METADATA) == 0) {
    // nothing to do, already ends just before a metadata page
    return true;
  }

  uint64_t new_end =
      ((s->output.found_position + s->input.space_required) &
       PAGES_IN_METADATA_MASK) +
      PAGES_IN_METADATA;
  if (new_end > s->output.found_position +
                    s->output.space_available_at_position) {
    return false;  // not enough room to shift things
  }
  s->output.space_available_at_position -=
      new_end - s->output.found_position - s->input.space_required;
  s->output.found_position = new_end - s->input.space_required;
  return true;
}
// end::bitmap_is_acceptable_match[]
