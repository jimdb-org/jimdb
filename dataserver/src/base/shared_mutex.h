// Copyright 2019 The JIMDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

_Pragma("once");

#include <errno.h>
#include <pthread.h>
#include <utility>

namespace jim {

class __shared_mutex_pthread {
    pthread_rwlock_t m_rwlock_ = PTHREAD_RWLOCK_INITIALIZER;

public:
    __shared_mutex_pthread() = default;
    ~__shared_mutex_pthread() = default;

    __shared_mutex_pthread(const __shared_mutex_pthread&) = delete;
    __shared_mutex_pthread& operator=(const __shared_mutex_pthread&) = delete;

    void lock() { pthread_rwlock_wrlock(&m_rwlock_); }

    void unlock() { pthread_rwlock_unlock(&m_rwlock_); }

    void lock_shared() {
        int ret;
        do {
            ret = pthread_rwlock_rdlock(&m_rwlock_);
        } while (ret == EAGAIN);
    }

    void unlock_shared() { unlock(); }
};

class shared_mutex {
public:
    shared_mutex() = default;
    ~shared_mutex() = default;

    shared_mutex(const shared_mutex&) = delete;
    shared_mutex& operator=(const shared_mutex&) = delete;

    void lock() { m_impl_.lock(); }
    void unlock() { m_impl_.unlock(); }

    // Shared ownership
    void lock_shared() { m_impl_.lock_shared(); }
    void unlock_shared() { m_impl_.unlock_shared(); }

private:
    __shared_mutex_pthread m_impl_;
};

template <typename Mutex>
class shared_lock {
public:
    typedef Mutex mutex_type;

    shared_lock() noexcept : m_pm_(nullptr) {}

    explicit shared_lock(mutex_type& m) : m_pm_(std::addressof(m)) {
        m.lock_shared();
    }

    ~shared_lock() { m_pm_->unlock_shared(); }

    shared_lock(shared_lock const&) = delete;
    shared_lock& operator=(shared_lock const&) = delete;

    void lock() { m_pm_->lock_shared(); }

    void unlock() { m_pm_->unlock_shared(); }

private:
    mutex_type* m_pm_;
};

}  // namespace jim
