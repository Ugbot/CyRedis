#ifndef CUCKOO_FILTER_HPP
#define CUCKOO_FILTER_HPP

#include <vector>
#include <string>
#include <cstdint>
#include <random>
#include <stdexcept>

namespace cyredis {

/**
 * High-performance Cuckoo Filter implementation
 *
 * A probabilistic data structure that supports adding and removing items
 * with better space efficiency than Bloom filters.
 *
 * Features:
 * - Support for deletion (unlike Bloom filters)
 * - Better space efficiency
 * - Cuckoo hashing for collision resolution
 * - Configurable bucket size
 */
class CuckooFilter {
private:
    static constexpr size_t FINGERPRINT_SIZE = 8;  // bits
    static constexpr size_t MAX_KICKS = 500;
    static constexpr size_t BUCKET_SIZE = 4;  // entries per bucket

    struct Bucket {
        uint8_t fingerprints[BUCKET_SIZE];

        Bucket() {
            for (size_t i = 0; i < BUCKET_SIZE; ++i) {
                fingerprints[i] = 0;
            }
        }

        bool insert(uint8_t fingerprint) {
            for (size_t i = 0; i < BUCKET_SIZE; ++i) {
                if (fingerprints[i] == 0) {
                    fingerprints[i] = fingerprint;
                    return true;
                }
            }
            return false;
        }

        bool remove(uint8_t fingerprint) {
            for (size_t i = 0; i < BUCKET_SIZE; ++i) {
                if (fingerprints[i] == fingerprint) {
                    fingerprints[i] = 0;
                    return true;
                }
            }
            return false;
        }

        bool contains(uint8_t fingerprint) const {
            for (size_t i = 0; i < BUCKET_SIZE; ++i) {
                if (fingerprints[i] == fingerprint) {
                    return true;
                }
            }
            return false;
        }
    };

    std::vector<Bucket> buckets_;
    size_t num_buckets_;
    size_t num_items_;
    std::mt19937 rng_;

    // Hash function for fingerprint
    inline uint8_t fingerprint(const std::string& item) const {
        uint64_t h = 0x9e3779b97f4a7c15ULL;
        for (char c : item) {
            h ^= static_cast<uint64_t>(c);
            h *= 0x100000001b3ULL;
        }
        uint8_t fp = static_cast<uint8_t>((h >> 56) & 0xFF);
        return fp == 0 ? 1 : fp;  // Avoid 0 fingerprint
    }

    // Primary hash
    inline size_t hash1(const std::string& item) const {
        uint64_t h = 0x9e3779b97f4a7c15ULL;
        for (char c : item) {
            h ^= static_cast<uint64_t>(c);
            h *= 0x100000001b3ULL;
        }
        return h % num_buckets_;
    }

    // Secondary hash from fingerprint
    inline size_t hash2(size_t index, uint8_t fp) const {
        uint64_t h = static_cast<uint64_t>(fp) * 0x5bd1e995;
        return (index ^ h) % num_buckets_;
    }

public:
    /**
     * Create a Cuckoo filter
     * @param capacity Expected number of elements
     */
    explicit CuckooFilter(size_t capacity)
        : num_buckets_((capacity + BUCKET_SIZE - 1) / BUCKET_SIZE),
          num_items_(0),
          rng_(std::random_device{}()) {

        buckets_.resize(num_buckets_);
    }

    /**
     * Add an item to the filter
     * @param item Item to add
     * @return true if successfully added, false if filter is full
     */
    bool add(const std::string& item) {
        uint8_t fp = fingerprint(item);
        size_t i1 = hash1(item);
        size_t i2 = hash2(i1, fp);

        // Try inserting in bucket 1
        if (buckets_[i1].insert(fp)) {
            ++num_items_;
            return true;
        }

        // Try inserting in bucket 2
        if (buckets_[i2].insert(fp)) {
            ++num_items_;
            return true;
        }

        // Both buckets full, start cuckoo kicking
        size_t index = (rng_() % 2 == 0) ? i1 : i2;

        for (size_t kicks = 0; kicks < MAX_KICKS; ++kicks) {
            // Randomly pick an entry to kick out
            size_t entry_idx = rng_() % BUCKET_SIZE;
            uint8_t old_fp = buckets_[index].fingerprints[entry_idx];
            buckets_[index].fingerprints[entry_idx] = fp;
            fp = old_fp;

            // Calculate alternate index for kicked-out fingerprint
            index = hash2(index, fp);

            // Try inserting kicked-out fingerprint
            if (buckets_[index].insert(fp)) {
                ++num_items_;
                return true;
            }
        }

        // Filter is effectively full
        return false;
    }

    /**
     * Remove an item from the filter
     * @param item Item to remove
     * @return true if item was found and removed, false otherwise
     */
    bool remove(const std::string& item) {
        uint8_t fp = fingerprint(item);
        size_t i1 = hash1(item);
        size_t i2 = hash2(i1, fp);

        if (buckets_[i1].remove(fp)) {
            --num_items_;
            return true;
        }

        if (buckets_[i2].remove(fp)) {
            --num_items_;
            return true;
        }

        return false;
    }

    /**
     * Check if an item might be in the set
     * @param item Item to check
     * @return true if item might be in set, false if definitely not in set
     */
    bool contains(const std::string& item) const {
        uint8_t fp = fingerprint(item);
        size_t i1 = hash1(item);
        size_t i2 = hash2(i1, fp);

        return buckets_[i1].contains(fp) || buckets_[i2].contains(fp);
    }

    /**
     * Get the number of items in the filter
     */
    size_t size() const {
        return num_items_;
    }

    /**
     * Get the number of buckets
     */
    size_t num_buckets() const {
        return num_buckets_;
    }

    /**
     * Get load factor (0.0 to 1.0)
     */
    double load_factor() const {
        size_t total_slots = num_buckets_ * BUCKET_SIZE;
        return static_cast<double>(num_items_) / total_slots;
    }

    /**
     * Get memory usage in bytes
     */
    size_t memory_usage() const {
        return buckets_.size() * sizeof(Bucket);
    }

    /**
     * Clear all items from the filter
     */
    void clear() {
        buckets_.clear();
        buckets_.resize(num_buckets_);
        num_items_ = 0;
    }
};

} // namespace cyredis

#endif // CUCKOO_FILTER_HPP
