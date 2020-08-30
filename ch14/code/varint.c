#include <byteswap.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <gavran/internal.h>

// Modified from
// https://github.com/danburkert/bytekey/blob/master/src/encoder.rs

static const uint64_t N1 = 1UL << 4;
static const uint64_t N2 = 1UL << 12;
static const uint64_t N3 = 1UL << 20;
static const uint64_t N4 = 1UL << 28;
static const uint64_t N5 = 1UL << 36;
static const uint64_t N6 = 1UL << 44;
static const uint64_t N7 = 1UL << 52;
static const uint64_t N8 = 1UL << 60;

// clang-format off
uint32_t varint_get_length(uint64_t n) {
  return (
      n < N1 ? 1
    : n < N2 ? 2
    : n < N3 ? 3
    : n < N4 ? 4
    : n < N5 ? 5
    : n < N6 ? 6
    : n < N7 ? 7
    : n < N8 ? 8
    :          9
  );
}
// clang-format on

uint8_t* varint_encode(uint64_t n, uint8_t* buf) {
  uint16_t s;
  uint32_t i;
  if (n < N1) {
    *buf++ = (uint8_t)n;
  } else if (n < N2) {
    s = bswap_16((uint16_t)(n | 1 << 12));
    memcpy(buf, &s, sizeof(uint16_t));
    buf += sizeof(uint16_t);
  } else if (n < N3) {
    *buf++ = (uint8_t)(n >> 16 | 2 << 4);
    s      = bswap_16((uint16_t)n);
    memcpy(buf, &s, sizeof(uint16_t));
    buf += sizeof(uint16_t);
  } else if (n < N4) {
    i = bswap_32((uint32_t)(n | 3 << 28));
    memcpy(buf, &i, sizeof(uint32_t));
    buf += sizeof(uint32_t);
  } else if (n < N5) {
    *buf++ = (uint8_t)(n >> 32 | 4 << 4);
    i      = (uint32_t)n;
    i      = bswap_32(i);
    memcpy(buf, &i, sizeof(uint32_t));
    buf += sizeof(uint32_t);
  } else if (n < N6) {
    s = bswap_16((uint16_t)(n >> 32 | 5 << 12));
    memcpy(buf, &s, sizeof(uint16_t));
    buf += sizeof(uint16_t);
    i = bswap_32((uint32_t)n);
    memcpy(buf, &i, sizeof(uint32_t));
    buf += sizeof(uint32_t);
  } else if (n < N7) {
    *buf++ = (uint8_t)(n >> 48 | 6 << 4);
    i      = bswap_16((uint16_t)(n >> 32));
    memcpy(buf, &i, sizeof(uint16_t));
    buf += sizeof(uint16_t);
    i = bswap_32((uint32_t)n);
    memcpy(buf, &i, sizeof(uint32_t));
    buf += sizeof(uint32_t);
  } else if (n < N8) {
    n = bswap_64(n | 7UL << 60);
    memcpy(buf, &n, sizeof(uint64_t));
    buf += sizeof(uint64_t);
  } else {
    *buf = 8 << 4;
    n    = bswap_64(n);
    memcpy(buf, &n, sizeof(uint64_t));
    buf += sizeof(uint64_t);
  }
  return buf;
}

uint8_t* varint_decode(uint8_t* buf, uint64_t* value) {
  uint8_t header = *buf++;
  uint8_t n      = header >> 4;

  uint64_t result = (uint64_t)((header & 0x0F) << (n * 8));
  for (size_t i = 1; i <= n; i++) {
    result += ((uint64_t)*buf++) << ((n - i) * 8);
  }
  *value = result;
  return buf;
}