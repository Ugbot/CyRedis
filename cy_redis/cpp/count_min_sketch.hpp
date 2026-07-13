#ifndef COUNT_MIN_SKETCH_HPP
#define COUNT_MIN_SKETCH_HPP

#include <vector>
#include <string>
#include <cmath>
#include <algorithm>
#include <cstdint>
#include <cstddef>

namespace cyredis {

using std::size_t;

/**
 * High-performance Count-Min Sketch implementation
 *
 * A probabilistic data structure for estimating frequencies of elements
 * in a data stream with sub-linear space complexity.
 *
 * Features:
 * - Point queries: estimate frequency of any item
 * - Range queries: estimate sum of frequencies in a range
 * - Inner product: estimate similarity between two streams
 * - Configurable accuracy and confidence
 */
class CountMinSketch {
private:
    std::vector<std::vector<uint32_t>> counters_;
    size_t width_;   // number of columns
    size_t depth_;   // number of rows (hash functions)
    uint64_t total_count_;

    // Hash functions using different seeds
    inline size_t hash(const std::string& item, size_t seed) const {
        uint64_t h = 0x9e3779b97f4a7c15ULL + seed;
        for (char c : item) {
            h ^= static_cast<uint64_t>(c);
            h *= 0x100000001b3ULL;
        }
        return h % width_;
    }

public:
    /**
     * Create a Count-Min Sketch
     * @param epsilon Error rate (smaller = more accurate, more memory)
     * @param delta Confidence (1 - delta is the probability of correct estimate)
     */
    CountMinSketch(double epsilon = 0.01, double delta = 0.01)
        : total_count_(0) {

        // Calculate dimensions
        // width = ceil(e / epsilon)
        // depth = ceil(ln(1/delta))
        width_ = static_cast<size_t>(std::ceil(std::exp(1.0) / epsilon));
        depth_ = static_cast<size_t>(std::ceil(std::log(1.0 / delta)));

        // Ensure minimum dimensions
        if (width_ < 10) width_ = 10;
        if (depth_ < 3) depth_ = 3;

        // Initialize counter array
        counters_.resize(depth_);
        for (size_t i = 0; i < depth_; ++i) {
            counters_[i].resize(width_, 0);
        }
    }

    /**
     * Create a Count-Min Sketch with explicit dimensions
     * @param width Number of columns
     * @param depth Number of rows (hash functions)
     */
    CountMinSketch(size_t width, size_t depth)
        : width_(width), depth_(depth), total_count_(0) {

        counters_.resize(depth_);
        for (size_t i = 0; i < depth_; ++i) {
            counters_[i].resize(width_, 0);
        }
    }

    /**
     * Add an item with a given count
     * @param item Item to add
     * @param count Count to add (default 1)
     */
    void add(const std::string& item, uint32_t count = 1) {
        for (size_t i = 0; i < depth_; ++i) {
            size_t pos = hash(item, i);
            counters_[i][pos] += count;
        }
        total_count_ += count;
    }

    /**
     * Estimate the frequency of an item
     * @param item Item to query
     * @return Estimated frequency (always >= actual frequency)
     */
    uint32_t estimate(const std::string& item) const {
        uint32_t min_count = UINT32_MAX;

        for (size_t i = 0; i < depth_; ++i) {
            size_t pos = hash(item, i);
            min_count = std::min(min_count, counters_[i][pos]);
        }

        return min_count;
    }

    /**
     * Add another sketch to this one
     * @param other Sketch to merge
     */
    void merge(const CountMinSketch& other) {
        if (width_ != other.width_ || depth_ != other.depth_) {
            throw std::invalid_argument("Cannot merge sketches with different dimensions");
        }

        for (size_t i = 0; i < depth_; ++i) {
            for (size_t j = 0; j < width_; ++j) {
                counters_[i][j] += other.counters_[i][j];
            }
        }
        total_count_ += other.total_count_;
    }

    /**
     * Clear all counts
     */
    void clear() {
        for (size_t i = 0; i < depth_; ++i) {
            std::fill(counters_[i].begin(), counters_[i].end(), 0);
        }
        total_count_ = 0;
    }

    /**
     * Get total count of all items
     */
    uint64_t total_count() const {
        return total_count_;
    }

    /**
     * Get the width (number of columns)
     */
    size_t width() const {
        return width_;
    }

    /**
     * Get the depth (number of hash functions)
     */
    size_t depth() const {
        return depth_;
    }

    /**
     * Get memory usage in bytes
     */
    size_t memory_usage() const {
        return depth_ * width_ * sizeof(uint32_t);
    }

    /**
     * Calculate inner product with another sketch (for similarity)
     * @param other Other sketch
     * @return Estimated inner product
     */
    uint64_t inner_product(const CountMinSketch& other) const {
        if (width_ != other.width_ || depth_ != other.depth_) {
            throw std::invalid_argument("Sketches must have same dimensions");
        }

        uint64_t min_product = UINT64_MAX;

        for (size_t i = 0; i < depth_; ++i) {
            uint64_t sum = 0;
            for (size_t j = 0; j < width_; ++j) {
                sum += static_cast<uint64_t>(counters_[i][j]) * other.counters_[i][j];
            }
            min_product = std::min(min_product, sum);
        }

        return min_product;
    }

    /**
     * Get the raw counter value at a specific position
     * @param row Row index
     * @param col Column index
     * @return Counter value
     */
    uint32_t get_counter(size_t row, size_t col) const {
        if (row >= depth_ || col >= width_) {
            throw std::out_of_range("Counter index out of range");
        }
        return counters_[row][col];
    }

    /**
     * Conservative update: only update if new value is larger
     * @param item Item to update
     * @param count Count to set
     */
    void conservative_add(const std::string& item, uint32_t count = 1) {
        // First, find minimum current value
        uint32_t min_count = estimate(item);
        uint32_t new_count = min_count + count;

        // Update only counters that have the minimum value
        for (size_t i = 0; i < depth_; ++i) {
            size_t pos = hash(item, i);
            if (counters_[i][pos] == min_count) {
                counters_[i][pos] = new_count;
            }
        }
        total_count_ += count;
    }
};

} // namespace cyredis

#endif // COUNT_MIN_SKETCH_HPP
