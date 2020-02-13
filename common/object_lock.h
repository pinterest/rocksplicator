/// Copyright 2016 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author bol (bol@pinterest.com)
//

#pragma once

#include <boost/intrusive/list.hpp>
#if __GNUC__ >= 8
#include <folly/concurrency/CacheLocality.h>
#else
#include <folly/detail/CacheLocality.h>
#endif
#include <folly/MPMCQueue.h>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>

namespace common {

/*
 * ObjectLock is a thread safe data structure to Lock() and Unlock() objects.
 * The potential use case is that the object space is huge, but only a few
 * of them need to be locked simultaneously.
 */
template <typename ObjectType, typename HasherType = std::hash<ObjectType>,
          typename MutexType = std::mutex>
class ObjectLock {
 private:
  struct Node : public boost::intrusive::list_base_hook<> {
    ObjectType object;
    MutexType nodeMutex;
    std::size_t refCount;

    Node() : object(), nodeMutex(), refCount(0) {}
  };

  struct Bucket {
#if __GNUC__ >= 8
    alignas(folly::hardware_destructive_interference_size) MutexType bucketMutex;
    alignas(folly::hardware_destructive_interference_size) boost::intrusive::list<Node> nodes;
#else
    MutexType bucketMutex FOLLY_ALIGN_TO_AVOID_FALSE_SHARING;
    boost::intrusive::list<Node> nodes FOLLY_ALIGN_TO_AVOID_FALSE_SHARING;
#endif

    template <typename ALLOC>
    void Lock(const ObjectType& object, ALLOC&& alloc) {
      typename boost::intrusive::list<Node>::iterator itor;
      {
        std::lock_guard<MutexType> g(bucketMutex);
        itor = FindNodeLocked(object);

        if (itor == nodes.end()) {
          auto p = alloc();
          p->object = object;
          nodes.push_front(*p);
          itor = nodes.begin();
        }

        assert(itor != nodes.end());
        ++itor->refCount;
      }
      itor->nodeMutex.lock();
    }

    // Return true if successfully acquired the lock. Otherwise, return false.
    template <typename ALLOC>
    bool TryLock(const ObjectType& object, ALLOC&& alloc) {
      if (!bucketMutex.try_lock()) {
        return false;
      }

      std::lock_guard<MutexType> g(bucketMutex, std::adopt_lock);
      auto itor = FindNodeLocked(object);

      if (itor == nodes.end()) {
        // Currently the object is not locked by anyone, lock() won't block us
        auto p = alloc();
        p->object = object;
        ++p->refCount;
        p->nodeMutex.lock();

        nodes.push_front(*p);
        return true;
      }

      // Highly likely someone else is holding the lock for the object, don't
      // bother to try_lock it.
      return false;
    }

    template <typename DEALLOC>
    void Unlock(const ObjectType& object, DEALLOC&& dealloc) {
      std::lock_guard<MutexType> g(bucketMutex);
      auto itor = FindNodeLocked(object);
      assert(itor != nodes.end() && itor->refCount > 0);
      itor->nodeMutex.unlock();
      --itor->refCount;
      if (itor->refCount == 0) {
        Node* p = &(*itor);
        nodes.erase(itor);
        dealloc(p);
      }
    }

   private:
    typename boost::intrusive::list<Node>::iterator FindNodeLocked(  // NOLINT
        const ObjectType& object) {
      auto itor = nodes.begin();
      while (itor != nodes.end()) {
        if (object == itor->object) {
          return itor;
        }

        ++itor;
      }

      return itor;
    }
  };

 public:
  // copy or move not allowed
  ObjectLock(const ObjectLock&) = delete;
  ObjectLock(ObjectLock&&) = delete;
  ObjectLock& operator=(const ObjectLock&) = delete;
  ObjectLock& operator=(ObjectLock&&) = delete;

  explicit ObjectLock(const std::size_t maxOutstandingLocksHint = 128)
      : nBucket_(maxOutstandingLocksHint << 2),
        buckets_(new Bucket[nBucket_]),
        nodePool_(maxOutstandingLocksHint) {
    assert(nBucket_ > 0);
  }

  ~ObjectLock() {
    for (std::size_t i = 0; i < nBucket_; ++i) {
      std::lock_guard<MutexType> g(buckets_[i].bucketMutex);
      assert(buckets_[i].nodes.empty());
    }

    Node* node;
    while (nodePool_.read(node)) {
      delete node;
    }
  }

  void Lock(const ObjectType& object) {
    auto idx = hasher_(object) % nBucket_;
    buckets_[idx].Lock(object, [this]() {
      return AllocNode();
    });
  }

  bool TryLock(const ObjectType& object) {
    auto idx = hasher_(object) % nBucket_;
    return buckets_[idx].TryLock(object, [this]() {
      return AllocNode();
    });
  }

  void Unlock(const ObjectType& object) {
    auto idx = hasher_(object) % nBucket_;
    buckets_[idx].Unlock(object, [this](Node * node) {
      DeallocNode(node);
    });
  }

 private:
  Node* AllocNode() {
    Node* node;
    if (nodePool_.read(node)) {
      return node;
    }

    return new Node;
  }

  void DeallocNode(Node* node) {
    if (!nodePool_.write(node)) {
      delete node;
    }
  }

  const std::size_t nBucket_;
  std::unique_ptr<Bucket[]> buckets_;
  folly::MPMCQueue<Node*> nodePool_;
  static HasherType hasher_;
};

template <typename ObjectType, typename HasherType, typename MutexType>
HasherType ObjectLock<ObjectType, HasherType, MutexType>::hasher_;

}  // namespace common
