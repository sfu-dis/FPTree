#pragma once

#include <cstring>
#include <iostream>
#include <cmath>

#define NDEBUG
#include <cassert>

#define MAX_LEAF_SIZE 48
#define NUM_OF_BITS (8 * sizeof(unsigned char))

const static size_t num_bytes = ceil(float(MAX_LEAF_SIZE) / NUM_OF_BITS);
const size_t iter = (MAX_LEAF_SIZE % 8 == 0) ? num_bytes : num_bytes - 1;
const static size_t num_bits = MAX_LEAF_SIZE;
const static size_t offset_bit = MAX_LEAF_SIZE - (8 * iter);
const static unsigned char residual = (255 << offset_bit);

class Bitset {

private:
      unsigned char bits[num_bytes] = {};

public:

      Bitset() {
      }

      ~Bitset() {
      }

      Bitset(const Bitset& bts) {
        std::copy(bts.bits, bts.bits + num_bytes, bits);
      }

      Bitset& operator=(const Bitset& bts) {
        std::copy(bts.bits, bts.bits + num_bytes, bits);
      }

      void set(const size_t pos, bool val) {
        assert(pos < MAX_LEAF_SIZE && "Bitset access out of bound!");
        if (val)
          bits[pos / NUM_OF_BITS] |= (1 << (pos % NUM_OF_BITS));
        else
          bits[pos / NUM_OF_BITS] &= ~(1 << (pos % NUM_OF_BITS));
      }

      void set(const size_t pos) {
        bits[pos / NUM_OF_BITS] |= (1 << (pos % NUM_OF_BITS));
      }

      void reset() {
        for (size_t i = 0; i < num_bytes; i++)
          bits[i] &= 0x0;
      }

      size_t size() const {
        return num_bits;
      }

      bool test(const size_t pos) const {
        return ( (bits[pos / NUM_OF_BITS] & (1U << (pos % NUM_OF_BITS))) != 0 );
      }

      void flip() {
        for (size_t i = 0; i < iter; i++)
          bits[i] ^= 255; 
        for (size_t i = 0; i < offset_bit; i++) {
          bits[iter] ^= (1 << (i % NUM_OF_BITS));
        }
      }

      bool all() {
        for (size_t i = 0; i < iter; i++)
          if (bits[i] != 255)
            return false;
        for (size_t i = iter * 8; i < iter * 8 + offset_bit; i++) {
          if (!test(i))  
            return false;
        }
        return true;
      }

      size_t count() {
        size_t count = 0, i, j;
        unsigned char c;
        for (i = 0; i < iter; i++) {
          c = bits[i];
          while (c != 0) {
            count += c & 0x1;
            c >>= 1;
          }
        }
        c = bits[iter]; 
        for (i = 0; i < offset_bit; i++) {
          count += c & 0x1;
            c >>= 1;
        }
        return count;
      }

      size_t _Find_first() {
        size_t count;
        unsigned char c;
        for (size_t i = 0; i < iter; i++) {
          c = bits[i];
          count = 0;
          while(c != 0) {
            if (test(i * 8 + count) & 1)
              return 8 * i + count;
            count += 1;
          }
        }
        for (size_t j = 0; j < offset_bit; j++) {
          if (test(iter * 8 + j) & 1)
              return iter * 8 + j;
        }
        return MAX_LEAF_SIZE;
      }

      void print_bits() {
        for (int j = num_bytes-1; j >= 0; j--) 
        {
          int i;
          for (i = 0; i < 8; i++) {
              printf("%d", !!((bits[j] << i) & 0x80));
          }
        }
        std::cout << "\n";
      }
};