//
// Created by zhengjin.wang on 25-4-29.
//

#ifndef COPYABLE_ATOMIC_H
#define COPYABLE_ATOMIC_H
#include <atomic>
template <typename T>
class copyable_atomic {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable.");

public:
    copyable_atomic() noexcept = default;

    copyable_atomic(const copyable_atomic<T>& other) {
        _atomic.store(other._atomic.load());
    }

    explicit constexpr copyable_atomic(T desired) noexcept : _atomic{desired} {}

    T operator=(T desired) noexcept {
        return _atomic.operator=(desired);
    }

    copyable_atomic& operator=(const copyable_atomic<T>& other) {
        _atomic.store(other._atomic.load());
        return *this;
    }

    bool is_lock_free() const {
        return _atomic.is_lock_free();
    }

    operator T() const noexcept {
        return _atomic.load();
    }

    void store(T desired, std::memory_order order = std::memory_order_seq_cst) {
        _atomic.store(desired, order);
    }

    decltype(auto) load(std::memory_order order = std::memory_order_seq_cst) const {
        return _atomic.load(order);
    }

    template <typename... Args>
    decltype(auto) operator++(Args&&... args) {
        return _atomic.operator++(std::forward<Args>(args)...);
    }

    template <typename... Args>
    decltype(auto) operator--(Args&&... args) {
        return _atomic.operator--(std::forward<Args>(args)...);
    }

    template <typename... Args>
    bool exchange(Args&&... args) {
        return _atomic.exchange(std::forward<Args>(args)...);
    }

    template <typename... Args>
    bool compare_exchange_weak(Args&&... args) {
        return _atomic.compare_exchange_weak(std::forward<Args>(args)...);
    }

    template <typename... Args>
    bool compare_exchange_strong(Args&&... args) {
        return _atomic.compare_exchange_strong(std::forward<Args>(args)...);
    }

private:
    std::atomic<T> _atomic;
};
#endif //COPYABLE_ATOMIC_H
