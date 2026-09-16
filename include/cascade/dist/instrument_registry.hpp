// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <vector>

#include "cascade/core/flat_hash_map.hpp"
#include "cascade/proto/symbol.hpp"

namespace cascade::dist {

/// Where an instrument lives: which shard owns its book, and its index within it.
struct InstrumentLocation {
  std::uint32_t shard_id{0};
  std::uint32_t book_index{0};
};

/// The security master: every instrument the plant will carry, and where its book is.
///
/// Built once before the session opens and read-only thereafter, which is what makes
/// it safe for every fan-out thread to resolve subscriptions against it concurrently
/// with the shards building books. A registry that grew during the session would put a
/// rehash on the one structure both tiers need, and the lock required to make that safe
/// would sit squarely on the subscription path.
///
/// Real venues publish exactly this, ahead of the open, for exactly this reason.
class InstrumentRegistry {
 public:
  InstrumentRegistry() : locations_(0, 8192) {}

  /// Startup only.
  void add(Symbol symbol, InstrumentLocation location) {
    if (locations_.find(symbol.raw())) return;
    locations_.insert_or_assign(symbol.raw(), location);
    symbols_.push_back(symbol);
  }

  const InstrumentLocation* find(Symbol symbol) const {
    return locations_.find(symbol.raw());
  }

  std::size_t size() const noexcept { return symbols_.size(); }
  const std::vector<Symbol>& symbols() const noexcept { return symbols_; }

 private:
  FlatHashMap<std::uint64_t, InstrumentLocation, SymbolRawHash> locations_;
  std::vector<Symbol> symbols_;
};

}  // namespace cascade::dist
