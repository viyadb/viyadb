#ifndef VIYA_UTIL_CRC32_H_
#define VIYA_UTIL_CRC32_H_

#include <stddef.h>
#include <stdint.h>

/* CRC-32 (Ethernet, ZIP, etc.) polynomial in reversed bit order. */
#define POLY 0xedb88320

/* This one is a Java compatible implementation */
uint32_t crc32(uint32_t crc, const unsigned char *buf, size_t len) {
  int k;
  crc = ~crc;
  while (len--) {
    crc ^= *buf++;
    for (k = 0; k < 8; k++)
      crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
  }
  return ~crc;
}

#endif // VIYA_UTIL_CRC32_H_
