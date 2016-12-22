/*
 * Copyright (C) 2016 Raphael S. Carvalho
 *
 * This program can be distributed under the terms of the GNU GPL.
 * See the file COPYING.
 */

#pragma once

#include <memory>
#include <cstdlib>
#include <ctime>
#include <string.h>
#include <algorithm>

template <typename T>
class temporary_buf {
    static_assert(sizeof(T) == 1, "must be stream of bytes");
    T* _p;
    size_t _s;
public:
    temporary_buf(size_t s)
        : _p(new T[s])
        , _s(s) {
        memset(_p, 0, s);
    }
    temporary_buf(T* p, size_t s)
        : _p(p)
        , _s(s) {
    }
    temporary_buf(const temporary_buf<T>& other) {
        size_t s = other.size();
        T* buf = new T[s];
        memcpy(buf, other.get(), s);
        _p = buf;
        _s = s;
    }
    temporary_buf(temporary_buf<T>&& other) : _p(other._p), _s(other._s) {
        other._p = nullptr;
        other._s = 0;
    }
    ~temporary_buf() {
        if (_p) {
            delete _p;
        }
    }

    T* get() {
        return _p;
    }
    const T* get() const {
        return _p;
    }
    size_t size() const {
        return _s;
    }
    void trim(size_t pos) {
        _s = pos;
    }
    bool operator==(const temporary_buf<T>& other) {
        size_t s = std::min(size(), other.size());
        return memcmp(get(), other.get(), s) == 0;
    }
    void operator=(const temporary_buf<T>&) = delete;

    static temporary_buf<T> random(size_t s) {
        T* buf = new T[s];
        std::srand(std::time(0));
        for (auto i = 0; i < s; i++) {
            buf[i] = std::rand();
        }
        return temporary_buf<T>(buf, s);
    }
};

template <typename T>
static temporary_buf<T> operator+(const temporary_buf<T>& a, const temporary_buf<T>& b) {
    T* buf = new T[a.size() + b.size()];
    memcpy(buf, a.get(), a.size());
    memcpy(buf + a.size(), b.get(), b.size());
    return temporary_buf<T>(buf, a.size() + b.size());
}
