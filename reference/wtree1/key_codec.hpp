#pragma once

// SIMD-optimized chunked key encoder + decoder.
//
// Encoding: every 3 input bytes → 4 output bytes (3 data + 1 control).
//   control = 0xFF: continuation (more chunks follow)
//   control = 0..3: terminator (N effective data bytes in this chunk)
//
// Sort order via memcmp is preserved:
//   - Continuation byte 0xFF > any terminator (0..3) > any data byte
//     in the right boundary cases (since data byte at position 3 of a
//     non-final chunk would be 0xFF, but at the chunk boundary the
//     terminator's <0xFF byte makes the SHORTER key sort first).
//   - "XY" vs "XYZ": "XY" → [X,Y,0,02], "XYZ" → [X,Y,Z,03].  For Z>0:
//     byte 2 differs (0x00 < Z), "XY" < "XYZ".  For Z=0: byte 2 ties,
//     byte 3 differs (0x02 < 0x03), "XY" < "XYZ".
//
// All keys become multiples of 4 bytes — perfect fit for the btree's
// u32-routed inner nodes, no shorts handling needed.

#include <arm_neon.h>
#include <cstdint>
#include <cstring>

namespace psitri::wtree {

// Encoded length given input length.  Always a multiple of 4.
constexpr inline uint16_t encoded_len(uint16_t raw_len) noexcept {
    if (raw_len == 0) return 4;
    return uint16_t(((raw_len + 2) / 3) * 4);
}

// Maximum encoded length given a max input length.
constexpr inline uint16_t encoded_len_max(uint16_t max_raw_len) noexcept {
    return encoded_len(max_raw_len);
}

// Encode raw key bytes into chunked form.
// out must have ≥ encoded_len(in_len) bytes.
inline void encode_key(const uint8_t* in, uint16_t in_len,
                        uint8_t* out) noexcept {
    uint16_t pos = 0;
    uint16_t op  = 0;

    // SIMD path: process 12 input bytes → 16 output bytes per NEON op.
    // We need to know this chunk is NOT the terminator, so require strictly
    // more than 12 bytes remaining.
    while (uint32_t(in_len) - pos > 12) {
        const uint8x16_t src = vld1q_u8(in + pos);
        // Shuffle indices: bytes 0,1,2 from src into out[0,1,2]; idx=16 →
        // vqtbl1q_u8 returns 0 (we OR 0xFF on those positions next).
        const uint8x16_t idx = {0,1,2,16, 3,4,5,16, 6,7,8,16, 9,10,11,16};
        const uint8x16_t data = vqtbl1q_u8(src, idx);
        // Mark continuation positions (bytes 3,7,11,15) with 0xFF.
        const uint8x16_t mark = {0,0,0,0xFF, 0,0,0,0xFF, 0,0,0,0xFF, 0,0,0,0xFF};
        const uint8x16_t result = vorrq_u8(data, mark);
        vst1q_u8(out + op, result);
        pos += 12;
        op  += 16;
    }

    // Scalar tail: continuation chunks until we have 0..3 bytes left.
    while (uint32_t(in_len) - pos > 3) {
        out[op + 0] = in[pos + 0];
        out[op + 1] = in[pos + 1];
        out[op + 2] = in[pos + 2];
        out[op + 3] = 0xFF;
        pos += 3;
        op  += 4;
    }

    // Terminator chunk: 0..3 effective bytes.
    const uint16_t term = uint16_t(in_len - pos);
    out[op + 0] = (term > 0) ? in[pos + 0] : 0;
    out[op + 1] = (term > 1) ? in[pos + 1] : 0;
    out[op + 2] = (term > 2) ? in[pos + 2] : 0;
    out[op + 3] = uint8_t(term);
}

// Decode encoded key back into raw bytes.
// out must have ≥ (in_len / 4) * 3 bytes (worst case).
// Returns the raw byte count.
inline uint16_t decode_key(const uint8_t* in, uint16_t in_len,
                            uint8_t* out) noexcept {
    uint16_t pos = 0;
    uint16_t op  = 0;

    // SIMD path: process 16 encoded bytes → 12 raw bytes per op, ONLY when
    // we can confirm all 4 chunks are continuations (control bytes 0xFF).
    while (uint32_t(in_len) - pos >= 32) {
        const uint8x16_t src = vld1q_u8(in + pos);
        // Check that bytes 3,7,11,15 are all 0xFF (continuation markers).
        // If any is not, fall through to scalar (terminator found within).
        const uint8_t c3 = vgetq_lane_u8(src, 3);
        const uint8_t c7 = vgetq_lane_u8(src, 7);
        const uint8_t cB = vgetq_lane_u8(src, 11);
        const uint8_t cF = vgetq_lane_u8(src, 15);
        if (c3 != 0xFF || c7 != 0xFF || cB != 0xFF || cF != 0xFF) break;
        // Shuffle bytes 0,1,2,4,5,6,8,9,10,12,13,14 to out[0..11].
        const uint8x16_t idx = {0,1,2, 4,5,6, 8,9,10, 12,13,14, 16,16,16,16};
        const uint8x16_t data = vqtbl1q_u8(src, idx);
        vst1q_u8(out + op, data);   // writes 16 bytes; only first 12 matter
        pos += 16;
        op  += 12;
    }

    // Scalar tail.
    while (pos < in_len) {
        const uint8_t ctrl = in[pos + 3];
        if (ctrl == 0xFF) {
            out[op + 0] = in[pos + 0];
            out[op + 1] = in[pos + 1];
            out[op + 2] = in[pos + 2];
            op  += 3;
            pos += 4;
        } else {
            // Terminator.
            for (uint16_t i = 0; i < ctrl; ++i) {
                out[op + i] = in[pos + i];
            }
            op  += ctrl;
            pos += 4;
            break;
        }
    }
    return op;
}

}  // namespace psitri::wtree
