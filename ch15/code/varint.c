#include <stdint.h>
#include <stdio.h>

#include <gavran/internal.h>

// Code from https://github.com/sorribas/varint.c/blob/master/varint.c
// slightly modified to my coding convention

static const uint64_t MSB = (uint8_t)0x80;
static const uint64_t MSBALL = (uint8_t)~0x7F;

static const uint64_t N1 = 128;  // 2 ^ 7
static const uint64_t N2 = 16384;
static const uint64_t N3 = 2097152;
static const uint64_t N4 = 268435456;
static const uint64_t N5 = 34359738368;
static const uint64_t N6 = 4398046511104;
static const uint64_t N7 = 562949953421312;
static const uint64_t N8 = 72057594037927936;
static const uint64_t N9 = 9223372036854775808U;

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
    : n < N9 ? 9
    :         10
  );
}
// clang-format on

uint8_t* varint_encode(uint64_t n, uint8_t* buf) {
  while (n & MSBALL) {
    *(buf++) = (n & 0xFF) | MSB;
    n = n >> 7;
  }
  *buf++ = (uint8_t)n;
  return buf;
}

uint8_t* varint_decode(uint8_t* buf, uint64_t* value) {
  uint64_t result = 0;
  int bits = 0;
  uint64_t ll;
  while (*buf & MSB) {
    ll = *buf;
    result += ((ll & 0x7F) << bits);
    buf++;
    bits += 7;
  }
  ll = *buf++;
  result += ((ll & 0x7F) << bits);
  *value = result;
  return buf;
}