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


// C++ implementation of Dmitry Vyukov's non-intrusive lock free unbound MPSC queue
// http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue

_Pragma("once");

#include <atomic>
#include <cassert>
#include <type_traits>

namespace jim {
namespace sdk {

template<typename T>
class JimQueue {
public:

    JimQueue() :
        head_(reinterpret_cast<MpscNode*>(new MpscNodeAligned)),
        tail_(head_.load(std::memory_order_relaxed))
    {
        MpscNode* node = head_.load(std::memory_order_relaxed);
        node->next.store(nullptr, std::memory_order_relaxed);
    }

    JimQueue(const JimQueue&) = delete;
    JimQueue& operator=(const JimQueue&) = delete;

    ~JimQueue() {
        T t;
        while (this->pop(t)) {
            ;//delete t;
        }

        MpscNode* node = head_.load(std::memory_order_relaxed);
        delete node;
    }

    void push(const T& t) {
        MpscNode* node = reinterpret_cast<MpscNode*>(new MpscNodeAligned);
        node->data = t;
        node->next.store(nullptr, std::memory_order_relaxed);

        MpscNode* prev_head = head_.exchange(node, std::memory_order_acq_rel);
        prev_head->next.store(node, std::memory_order_release);
    }

    bool pop(T& t) {
        MpscNode* tail = tail_.load(std::memory_order_relaxed);
        MpscNode* next = tail->next.load(std::memory_order_acquire);

        if (next == nullptr) {
            return false;
        }

        t = next->data;
        tail_.store(next, std::memory_order_release);

        delete tail;
        return true;
    }


private:
    struct MpscNode {
        T data;
        std::atomic<MpscNode*> next;
    };

    //typedef typename std::aligned_storage<sizeof(MpscNode), std::alignment_of<MpscNode>::value>::type MpscNodeAligned;
    using MpscNodeAligned = typename std::aligned_storage<sizeof(MpscNode), std::alignment_of<MpscNode>::value>::type;

    std::atomic<MpscNode*> head_;
    std::atomic<MpscNode*> tail_;

};

} // namespace sdk
} // namespace jim
