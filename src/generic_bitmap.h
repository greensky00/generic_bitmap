/**
 * Copyright (C) 2019-present Jung-Sang Ahn <jungsang.ahn@gmail.com>
 * All rights reserved.
 *
 * https://github.com/greensky00
 *
 * Generic Bitmap
 * Version: 0.1.0
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <mutex>
#include <thread>

#include <stdint.h>
#include <string.h>

class GenericBitmap {
public:
    /**
     * Default constructor: initialize to 0.
     */
    GenericBitmap(uint64_t num_bits,
                  size_t num_threads = 0)
        : numThreads(num_threads)
        , numBits(num_bits)
    {
        // Ceiling.
        memorySizeByte = (numBits + 7) / 8;
        myBitmap = (uint8_t*)calloc(1, memorySizeByte);

        init();
    }

    /**
     * Copy from given memory blob.
     */
    GenericBitmap(void* memory_ptr,
                  size_t memory_ptr_size,
                  size_t num_bits,
                  size_t num_threads = 0)
        : numThreads(num_threads)
        , numBits(num_bits)
        , memorySizeByte(memory_ptr_size)
    {
        myBitmap = (uint8_t*)calloc(1, memorySizeByte);
        memcpy(myBitmap, memory_ptr, memorySizeByte);

        init();
    }

    /**
     * Destructor.
     */
    ~GenericBitmap() {
        free(myBitmap);
        delete[] locks;
    }

    /**
     * Return the size of bitmap (number of bits).
     */
    size_t size() const { return numBits; }

    /**
     * Return the size of allocated memory region (in byte).
     */
    size_t getMemorySize() const { return memorySizeByte; }

    /**
     * Return the memory address of allocated memory region.
     */
    void* getPtr() const { return myBitmap; }

    /**
     * Get the bitmap value of given index.
     */
    inline bool get(uint64_t idx) {
        uint64_t lock_idx = 0, offset = 0, byte_idx = 0;
        parse(idx, lock_idx, offset, byte_idx);

        std::lock_guard<std::mutex> l(locks[lock_idx]);
        return getInternal(byte_idx, offset);
    }

    /**
     * Set the bitmap value of given index.
     */
    inline bool set(uint64_t idx, bool val) {
        uint64_t lock_idx = 0, offset = 0, byte_idx = 0;
        parse(idx, lock_idx, offset, byte_idx);

        std::lock_guard<std::mutex> l(locks[lock_idx]);
        // NOTE: bool -> int conversion is defined in C++ standard.
        return setInternal(offset, byte_idx, val);
    }

private:
    void init() {
        masks8[0] = 0x80;
        masks8[1] = 0x40;
        masks8[2] = 0x20;
        masks8[3] = 0x10;
        masks8[4] = 0x08;
        masks8[5] = 0x04;
        masks8[6] = 0x02;
        masks8[7] = 0x01;

        // `numThreads` should be 2^n.
        if (!numThreads) {
            numThreads = 1;
            size_t num_cores = std::thread::hardware_concurrency();
            while (numThreads < num_cores) numThreads *= 2;
        }

        // TODO:
        //   To support partitioned lock, need to resolve
        //   aligned memory update issue.
        //   Until then, just use global latch.
        numThreads = 1;
        locks = new std::mutex[numThreads];

        for (size_t prev = 0; prev < 256; ++prev) {
            for (size_t offset = 0; offset < 8; ++offset) {
                uint8_t mask = masks8[offset];
                calcTable[prev][offset][0] = prev & (~mask);
                calcTable[prev][offset][1] = prev | mask;
            }
        }
    }

    inline void parse(uint64_t idx,
                      uint64_t& lock_idx_out,
                      uint64_t& offset_out,
                      uint64_t& byte_idx_out)
    {
        lock_idx_out = idx & (numThreads - 1);
        offset_out = idx & 0x7;
        byte_idx_out = idx >> 3;
    }

    inline bool getInternal(uint64_t byte_idx,
                            uint64_t offset)
    {
        uint8_t val = myBitmap[byte_idx];
        return val & masks8[offset];
    }

    inline bool setInternal(uint64_t offset,
                            uint64_t byte_idx,
                            uint8_t val)
    {
        uint8_t mask = masks8[offset];
        uint8_t prev = myBitmap[byte_idx];
        myBitmap[byte_idx] = calcTable[prev][offset][val];
        return prev & mask;
    }

    size_t numThreads;
    uint64_t numBits;
    uint64_t memorySizeByte;
    uint8_t* myBitmap;
    uint8_t calcTable[256][8][2];
    uint8_t masks8[8];
    std::mutex* locks;
};

