/*
 * Copyright (C) 2016 Raphael S. Carvalho
 *
 * This program can be distributed under the terms of the GNU GPL.
 * See the file COPYING.
 */

// compile: g++ --std=c++14 compressors_test.cc -llz4 -lsnappy -lz -lboost_system

#include <memory>
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <string.h>
#include <algorithm>
#include <exception>
#include <chrono>
#include "temporary_buf.hh"
#include "custom_assert.hh"

#include <lz4.h>
#include <snappy-c.h>
#include <zlib.h>

enum class compressor_type {
    none,
    lz4,
    deflate,
    snappy,
};

class compressor {
public:
    virtual const char* name() = 0;
    virtual size_t compress(const char* input, size_t input_len, char* output, size_t output_len) = 0;
    // return bytes stored in output
    virtual size_t uncompress(const char* input, size_t input_len, char* output, size_t output_len) = 0;
    // return bytes used in input to generate output
    virtual size_t uncompress_fast(const char* input, size_t input_len, char* output, size_t original_size) = 0;
    virtual size_t compress_max_size(size_t input_len) = 0;
};

class lz4_compressor : public compressor {
    virtual const char* name() override {
        return "lz4";
    }

    virtual size_t compress(const char* input, size_t input_len, char* output, size_t output_len) override {
        if (output_len < LZ4_COMPRESSBOUND(input_len)) {
            throw std::runtime_error("LZ4 compression failure: length of output is too small");
        }

#ifdef HAVE_LZ4_COMPRESS_DEFAULT
        auto ret = LZ4_compress_default(input, output, input_len, LZ4_compressBound(input_len));
#else
        auto ret = LZ4_compress(input, output, input_len);
#endif
        if (ret == 0) {
            throw std::runtime_error("LZ4 compression failure: LZ4_compress() failed");
        }
        return ret;
    }

    virtual size_t uncompress(const char* input, size_t input_len, char* output, size_t output_len) override {
        auto ret = LZ4_decompress_safe(input, output, input_len, output_len);
        if (ret < 0) {
            throw std::runtime_error("LZ4 uncompression failure");
        }
        return ret;
    }

    virtual size_t uncompress_fast(const char* input, size_t input_len, char* output, size_t original_size) override {
        auto ret = LZ4_decompress_fast(input, output, original_size);
        if (ret < 0) {
            throw std::runtime_error("LZ4 fast uncompression failure");
        }
        return ret;
    }

    virtual size_t compress_max_size(size_t input_len) {
        return LZ4_COMPRESSBOUND(input_len);
    }
};

class deflate_compressor : public compressor {
    virtual const char* name() override {
        return "deflate";
    }

    virtual size_t compress(const char* input, size_t input_len, char* output, size_t output_len) override {
        z_stream zs;
        zs.zalloc = Z_NULL;
        zs.zfree = Z_NULL;
        zs.opaque = Z_NULL;
        zs.avail_in = 0;
        zs.next_in = Z_NULL;
        if (deflateInit(&zs, Z_DEFAULT_COMPRESSION) != Z_OK) {
            throw std::runtime_error("deflate compression init failure");
        }
        zs.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(input));
        zs.avail_in = input_len;
        zs.next_out = reinterpret_cast<unsigned char*>(output);
        zs.avail_out = output_len;
        auto res = deflate(&zs, Z_FINISH);
        deflateEnd(&zs);
        if (res == Z_STREAM_END) {
            return output_len - zs.avail_out;
        } else {
            throw std::runtime_error("deflate compression failure");
        }
    }

    virtual size_t uncompress(const char* input, size_t input_len, char* output, size_t output_len) override {
        z_stream zs;
        zs.zalloc = Z_NULL;
        zs.zfree = Z_NULL;
        zs.opaque = Z_NULL;
        zs.avail_in = 0;
        zs.next_in = Z_NULL;
        if (inflateInit(&zs) != Z_OK) {
            throw std::runtime_error("deflate uncompression init failure");
        }
        // yuck, zlib is not const-correct, and also uses unsigned char while we use char :-(
        zs.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(input));
        zs.avail_in = input_len;
        zs.next_out = reinterpret_cast<unsigned char*>(output);
        zs.avail_out = output_len;
        auto res = inflate(&zs, Z_FINISH);
        inflateEnd(&zs);
        if (res == Z_STREAM_END) {
            return output_len - zs.avail_out;
        } else {
            throw std::runtime_error("deflate uncompression failure");
        }
    }

    virtual size_t uncompress_fast(const char* input, size_t input_len, char* output, size_t original_size) override {
        z_stream zs;
        zs.zalloc = Z_NULL;
        zs.zfree = Z_NULL;
        zs.opaque = Z_NULL;
        zs.avail_in = 0;
        zs.next_in = Z_NULL;
        if (inflateInit(&zs) != Z_OK) {
            throw std::runtime_error("deflate uncompression init failure");
        }
        // yuck, zlib is not const-correct, and also uses unsigned char while we use char :-(
        zs.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(input));
        zs.avail_in = input_len;
        zs.next_out = reinterpret_cast<unsigned char*>(output);
        zs.avail_out = original_size;
        auto res = inflate(&zs, Z_FINISH);
        inflateEnd(&zs);
        if (res == Z_STREAM_END) {
            assert(zs.total_out == original_size);
            return zs.total_in;
        } else {
            throw std::runtime_error("deflate uncompression failure");
        }
    }

    virtual size_t compress_max_size(size_t input_len) {
        z_stream zs;
        zs.zalloc = Z_NULL;
        zs.zfree = Z_NULL;
        zs.opaque = Z_NULL;
        zs.avail_in = 0;
        zs.next_in = Z_NULL;
        if (deflateInit(&zs, Z_DEFAULT_COMPRESSION) != Z_OK) {
            throw std::runtime_error("deflate compression init failure");
        }
        auto res = deflateBound(&zs, input_len);
        deflateEnd(&zs);
        return res;
    }
};

class snappy_compressor : public compressor {
    virtual const char* name() override {
        return "snappy";
    }

    virtual size_t compress(const char* input, size_t input_len, char* output, size_t output_len) override {
        auto ret = snappy_compress(input, input_len, output, &output_len);
        if (ret != SNAPPY_OK) {
            auto f = (boost::format("snappy compression failure: %1%") % error_msg(ret));
            throw std::runtime_error(f.str());
        }
        return output_len;
    }

    virtual size_t uncompress(const char* input, size_t input_len, char* output, size_t output_len) override {
        auto ret = snappy_uncompress(input, input_len, output, &output_len);
        if (ret != SNAPPY_OK) {
            auto f = (boost::format("snappy uncompression failure: %1%") % error_msg(ret));
            throw std::runtime_error(f.str());
        }
        return output_len;
    }

    virtual size_t uncompress_fast(const char* input, size_t input_len, char* output, size_t original_size) override {
        throw std::runtime_error("snappy uncompress_fast(): operation not supported");
    }

    virtual size_t compress_max_size(size_t input_len) {
        return snappy_max_compressed_length(input_len);
    }
private:
    static const char* error_msg(snappy_status s) {
        switch(s) {
        case SNAPPY_INVALID_INPUT:
            return "invalid input";
        case SNAPPY_BUFFER_TOO_SMALL:
            return "buffer too small";
        default:
            return "unknown";
        }
    }
};

static std::unique_ptr<compressor> make_compressor(compressor_type c) {
    switch (c) {
    case compressor_type::lz4:
        return std::make_unique<lz4_compressor>();
    case compressor_type::deflate:
        return std::make_unique<deflate_compressor>();
    case compressor_type::snappy:
        return std::make_unique<snappy_compressor>();
    default:
        throw std::runtime_error("compressor not available");
    }
}

static void compressor_test(compressor_type t) {
    static constexpr size_t chunk_length = 4*1024;
    auto c = make_compressor(t);
    bool failure = false;
    std::cout << "testing " << c->name() << "...\n";

    try {
    {   // basic compression/decompression test
        auto input = temporary_buf<char>::random(chunk_length);
        auto compressed = temporary_buf<char>(c->compress_max_size(chunk_length));
        auto uncompressed = temporary_buf<char>(chunk_length);
        auto s = c->compress(input.get(), input.size(), compressed.get(), compressed.size());
        compressed.trim(s);
        s = c->uncompress(compressed.get(), compressed.size(), uncompressed.get(), uncompressed.size());
        assert(s == chunk_length);
        uncompressed.trim(s);
        assert(input == uncompressed);
    }
    {   // generate a buffer with two compressed chunks and decompress both of
        // them only using decompressed size (chunk_length).
        auto first_chunk = temporary_buf<char>::random(chunk_length);
        auto first_compressed_chunk = temporary_buf<char>(c->compress_max_size(chunk_length));
        auto ret = c->compress(first_chunk.get(), first_chunk.size(), first_compressed_chunk.get(), first_compressed_chunk.size());
        first_compressed_chunk.trim(ret);

        auto second_chunk = temporary_buf<char>::random(chunk_length);
        auto second_compressed_chunk = temporary_buf<char>(c->compress_max_size(chunk_length));
        ret = c->compress(second_chunk.get(), second_chunk.size(), second_compressed_chunk.get(), second_compressed_chunk.size());
        second_compressed_chunk.trim(ret);

        auto compressed_chunks = first_compressed_chunk + second_compressed_chunk;
        assert(compressed_chunks.size() == (first_compressed_chunk.size() + second_compressed_chunk.size()));
        assert(memcmp(compressed_chunks.get(), first_compressed_chunk.get(), first_compressed_chunk.size()) == 0);
        assert(memcmp(compressed_chunks.get() + first_compressed_chunk.size(), second_compressed_chunk.get(), second_compressed_chunk.size()) == 0);

        auto first_uncompressed_chunk = temporary_buf<char>(chunk_length + 4);
        *(uint32_t*)(first_uncompressed_chunk.get()+chunk_length) = 0xDEADBEEF; // used to check for overflow.
        ret = c->uncompress_fast(compressed_chunks.get(), compressed_chunks.size(), first_uncompressed_chunk.get(), chunk_length);
        assert(ret == first_compressed_chunk.size());
        assert(first_uncompressed_chunk == first_chunk);
        assert(*(uint32_t*)(first_uncompressed_chunk.get()+chunk_length) == 0xDEADBEEF);

        auto second_uncompressed_chunk = temporary_buf<char>(chunk_length + 1);
        *(uint32_t*)(first_uncompressed_chunk.get()+chunk_length) = 0xDEADBEEF;
        ret = c->uncompress_fast(compressed_chunks.get() + ret, compressed_chunks.size() - ret, second_uncompressed_chunk.get(), chunk_length);
        assert(ret == second_compressed_chunk.size());
        assert(second_uncompressed_chunk == second_chunk);
        assert(*(uint32_t*)(first_uncompressed_chunk.get()+chunk_length) == 0xDEADBEEF);
    }
    {
        struct stats {
            int64_t lat_count = 0;
            int64_t lat_total = 0;
            int64_t lat_min = std::numeric_limits<int64_t>::max();
            int64_t lat_max = std::numeric_limits<int64_t>::min();
            std::vector<int64_t> lats;

            void update(int64_t lat) {
                lat_count++;
                lat_total += lat;
                lat_min = std::min(lat_min, lat);
                lat_max = std::max(lat_max, lat);
                lats.push_back(lat);
            }
            auto to_print() {
                std::sort(lats.begin(), lats.end());
                int64_t med = lats.size() ? lats[lats.size() / 2] : 0;
                int64_t avg = lat_total / lat_count;
                auto f = boost::format("med: %1%, min: %2%, max: %3%, avg: %4%") % med % lat_min % lat_max % avg;
                return f.str();
            }
        };
        const std::vector<int> chunk_lengths = { 4*1024, 16*1024, 64*1024, 256*1024 };

        for (auto chunk_len : chunk_lengths) {
            std::cout << "chunk lenght: " << chunk_len << std::endl;

            stats with_compressed_length;
            stats without_compressed_length;

            for (auto i = 0; i < 10000; i++) {
                auto data = temporary_buf<char>::random(chunk_len);
                auto compressed = temporary_buf<char>(c->compress_max_size(chunk_len));
                auto ret = c->compress(data.get(), data.size(), compressed.get(), compressed.size());
                compressed.trim(ret);

                auto run = [] (stats& s, auto func) {
                    auto start = std::chrono::high_resolution_clock::now();
                    func();
                    auto passed = std::chrono::high_resolution_clock::now() - start;
                    auto lat = std::chrono::duration_cast<std::chrono::nanoseconds>(passed).count();
                    s.update(lat);
                };

                run(with_compressed_length, [&] {
                    auto uncompressed = temporary_buf<char>(chunk_len);
                    auto ret = c->uncompress(compressed.get(), compressed.size(), uncompressed.get(), uncompressed.size());
                    assert(ret == chunk_len);
                    uncompressed.trim(ret);
                    assert(data == uncompressed);
                });
                run(without_compressed_length, [&] {
                    auto uncompressed = temporary_buf<char>(chunk_len);
                    auto ret = c->uncompress_fast(compressed.get(), c->compress_max_size(chunk_len), uncompressed.get(), chunk_len);
                    assert(ret == compressed.size());
                    assert(data == uncompressed);
                });
            }
            std::cout << "with compressed length:   \t" << with_compressed_length.to_print() << std::endl;
            std::cout << "without compressed length:\t" << without_compressed_length.to_print() << std::endl;
        }
    }
    } catch (const std::exception& e) {
        std::cout << "Caught exception: " << e.what() << std::endl;
        failure = true;
    }

    std::cout << "status: " << (failure ? "failed" : "done") << std::endl << std::endl;
}

int main(void) {
    compressor_test(compressor_type::lz4);
    compressor_test(compressor_type::deflate);
    compressor_test(compressor_type::snappy);

    return 0;
}
