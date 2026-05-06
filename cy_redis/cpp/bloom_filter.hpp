#ifndef BLOOM_FILTER_HPP
#define BLOOM_FILTER_HPP

#include <vector>
#include <string>
#include <cmath>
#include <functional>
#include <cstdint>

namespace cyredis {

/**
 * High-performance Bloom Filter implementation
 *
 * A space-efficient probabilistic data structure for testing set membership.
 * False positives are possible, but false negatives are not.
 *
 * Features:
 * - Multiple hash functions for low collision rates
 * - Configurable false positive rate
 * - Optimized bit operations
 */
class BloomFilter {
private:
    std::vector<uint64_t> bit_array_;
    size_t num_bits_;
    size_t num_hashes_;
    size_t num_items_;

    // Hash functions using MurmurHash3-like algorithm
    inline uint64_t hash1(const std::string& item) const {
        uint64_t h = 0x9e3779b97f4a7c15ULL;
        for (char c : item) {
            h ^= static_cast<uint64_t>(c);
            h *= 0x100000001b3ULL;
        }
        return h;
    }

    inline uint64_t hash2(const std::string& item) const {
        uint64_t h = 0xcbf29ce484222325ULL;
        for (char c : item) {
            h ^= static_cast<uint64_t>(c);
            h *= 0x100000001b3ULL;
        }
        return h;
    }

    // Generate nth hash using double hashing
    inline size_t get_hash(const std::string& item, size_t n) const {
        uint64_t h1 = hash1(item);
        uint64_t h2 = hash2(item);
        return (h1 + n * h2) % num_bits_;
    }

    inline void set_bit(size_t pos) {
        size_t word_index = pos / 64;
        size_t bit_index = pos % 64;
        bit_array_[word_index] |= (1ULL << bit_index);
    }

    inline bool get_bit(size_t pos) const {
        size_t word_index = pos / 64;
        size_t bit_index = pos % 64;
        return (bit_array_[word_index] & (1ULL << bit_index)) != 0;
    }

public:
    /**
     * Create a Bloom filter
     * @param capacity Expected number of elements
     * @param false_positive_rate Target false positive rate (0.0 to 1.0)
     */
    BloomFilter(size_t capacity, double false_positive_rate = 0.01)
        : num_items_(0) {

        // Calculate optimal number of bits: m = -n*ln(p) / (ln(2)^2)
        num_bits_ = static_cast<size_t>(
            -static_cast<double>(capacity) * std::log(false_positive_rate) /
            (std::log(2.0) * std::log(2.0))
        );

        // Calculate optimal number of hash functions: k = (m/n) * ln(2)
        num_hashes_ = static_cast<size_t>(
            (static_cast<double>(num_bits_) / capacity) * std::log(2.0)
        );

        // Ensure at least 1 hash function and at most 20
        if (num_hashes_ < 1) num_hashes_ = 1;
        if (num_hashes_ > 20) num_hashes_ = 20;

        // Allocate bit array (using 64-bit words)
        size_t num_words = (num_bits_ + 63) / 64;
        bit_array_.resize(num_words, 0);
    }

    /**
     * Add an item to the filter
     * @param item Item to add
     */
    void add(const std::string& item) {
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t pos = get_hash(item, i);
            set_bit(pos);
        }
        ++num_items_;
    }

    /**
     * Check if an item might be in the set
     * @param item Item to check
     * @return true if item might be in set, false if definitely not in set
     */
    bool contains(const std::string& item) const {
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t pos = get_hash(item, i);
            if (!get_bit(pos)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Clear all items from the filter
     */
    void clear() {
        std::fill(bit_array_.begin(), bit_array_.end(), 0);
        num_items_ = 0;
    }

    /**
     * Get the number of items added to the filter
     */
    size_t size() const {
        return num_items_;
    }

    /**
     * Get the total number of bits in the filter
     */
    size_t capacity() const {
        return num_bits_;
    }

    /**
     * Get the number of hash functions used
     */
    size_t num_hashes() const {
        return num_hashes_;
    }

    /**
     * Estimate the current false positive rate
     * Formula: (1 - e^(-kn/m))^k
     * where k = num hashes, n = num items, m = num bits
     */
    double estimated_fpr() const {
        if (num_items_ == 0) return 0.0;

        double exponent = -static_cast<double>(num_hashes_ * num_items_) / num_bits_;
        double base = 1.0 - std::exp(exponent);
        return std::pow(base, static_cast<double>(num_hashes_));
    }

    /**
     * Get memory usage in bytes
     */
    size_t memory_usage() const {
        return bit_array_.size() * sizeof(uint64_t);
    }
};

} // namespace cyredis

#endif // BLOOM_FILTER_HPP
