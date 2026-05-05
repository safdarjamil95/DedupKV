//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <cassert>
#include <cstdint>

#include "monitoring/perf_level_imp.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

namespace {

ThreadLocalPtr perf_level_holder;

void SetPerfLevelValue(PerfLevel level) {
  perf_level_holder.Reset(reinterpret_cast<void*>(
      static_cast<uintptr_t>(level) + static_cast<uintptr_t>(1)));
}

PerfLevel GetPerfLevelValue() {
  auto raw = reinterpret_cast<uintptr_t>(perf_level_holder.Get());
  if (raw == 0) {
    return kEnableCount;
  }
  return static_cast<PerfLevel>(raw - 1);
}

}  // namespace

void SetPerfLevel(PerfLevel level) {
  assert(level > kUninitialized);
  assert(level < kOutOfBounds);
  SetPerfLevelValue(level);
}

PerfLevel GetPerfLevel() { return GetPerfLevelValue(); }

}  // namespace ROCKSDB_NAMESPACE
