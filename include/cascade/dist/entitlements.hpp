// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "cascade/core/flat_hash_map.hpp"
#include "cascade/proto/symbol.hpp"

namespace cascade::dist {

/// A trading venue. Entitlements are granted per venue, because that is the unit an
/// exchange actually licenses: a firm pays for the Nasdaq feed, not for AAPL.
using VenueId = std::uint8_t;
inline constexpr VenueId kMaxVenues = 32;  ///< Bounded by the 32-bit entitlement mask.
inline constexpr VenueId kUnknownVenue = 0xFF;

/// What a client is allowed to do.
struct ClientEntitlement {
  std::string client_id;
  std::uint32_t venue_mask{0};        ///< Bit per venue.
  std::uint32_t max_subscriptions{0};
  bool allow_incremental{false};      ///< Un-conflated delivery is a premium tier: it
                                      ///< costs the plant a full publication log.
  bool allow_trades{true};
};

/// Permissioning for market data.
///
/// This is not decoration. Redistributing a venue's data to a client that has not
/// licensed it is a contractual violation with real financial consequences, and it is
/// the reason market-data systems carry an entitlement layer at all.
///
/// Two properties drive the design:
///
///   * **Checks must be cheap enough to run on every message, not just at subscribe.**
///     Checking only at subscribe time means a revocation does not take effect until
///     the client reconnects, which can be hours. Resolution is therefore a 32-bit
///     mask test against a value cached on the connection — a single AND.
///
///   * **Revocation must propagate promptly.** Entitlements change while the market is
///     open (a firm's contract lapses, a trial ends), so the authoritative table is
///     mutable behind a reader-writer lock, and connections re-resolve their mask on a
///     version bump rather than holding a stale copy forever.
class EntitlementTable {
 public:
  EntitlementTable() : symbol_venue_(0, 4096) {}

  // --- administration (control plane; not on the hot path) -----------------

  void register_venue(VenueId venue, const std::string& name) {
    std::unique_lock<std::shared_mutex> guard(mutex_);
    if (venue >= kMaxVenues) throw std::invalid_argument("venue id out of range");
    if (venue_names_.size() <= venue) venue_names_.resize(venue + 1u);
    venue_names_[venue] = name;
    ++version_;
  }

  void assign_symbol(Symbol symbol, VenueId venue) {
    std::unique_lock<std::shared_mutex> guard(mutex_);
    symbol_venue_.insert_or_assign(symbol.raw(), venue);
    ++version_;
  }

  void grant(const std::string& token, ClientEntitlement entitlement) {
    std::unique_lock<std::shared_mutex> guard(mutex_);
    clients_[token] = std::move(entitlement);
    ++version_;
  }

  /// Withdraw a client's access. Connections notice on their next version check and
  /// are disconnected with `kEntitlementRevoked` — not left running on a stale mask.
  void revoke(const std::string& token) {
    std::unique_lock<std::shared_mutex> guard(mutex_);
    clients_.erase(token);
    ++version_;
  }

  /// Bumped on every change, so a connection can tell whether its cached mask is
  /// still current with one relaxed load instead of taking the lock per message.
  std::uint64_t version() const noexcept {
    return version_.load(std::memory_order_acquire);
  }

  // --- resolution (connection setup and revocation checks) -----------------

  /// Resolve a login token. Returns false if the token is unknown.
  bool resolve(const std::string& token, ClientEntitlement& out) const {
    std::shared_lock<std::shared_mutex> guard(mutex_);
    const auto it = clients_.find(token);
    if (it == clients_.end()) return false;
    out = it->second;
    return true;
  }

  /// Which venue lists an instrument. `kUnknownVenue` if we have never heard of it —
  /// which is a rejection, not a default-allow. Failing open here would hand out data
  /// for any symbol a client cared to guess.
  VenueId venue_for(Symbol symbol) const {
    std::shared_lock<std::shared_mutex> guard(mutex_);
    const VenueId* found = symbol_venue_.find(symbol.raw());
    return found ? *found : kUnknownVenue;
  }

  std::string venue_name(VenueId venue) const {
    std::shared_lock<std::shared_mutex> guard(mutex_);
    if (venue < venue_names_.size()) return venue_names_[venue];
    return "unknown";
  }

  std::size_t client_count() const {
    std::shared_lock<std::shared_mutex> guard(mutex_);
    return clients_.size();
  }

  // --- the hot check -------------------------------------------------------

  /// Is a venue within a resolved mask? One AND — cheap enough to run per message,
  /// which is what makes prompt revocation possible at all.
  static CASCADE_ALWAYS_INLINE bool permits(std::uint32_t venue_mask,
                                            VenueId venue) noexcept {
    if (venue >= kMaxVenues) return false;
    return (venue_mask & (1u << venue)) != 0;
  }

  static std::uint32_t mask_of(std::initializer_list<VenueId> venues) noexcept {
    std::uint32_t mask = 0;
    for (VenueId venue : venues) {
      if (venue < kMaxVenues) mask |= (1u << venue);
    }
    return mask;
  }

 private:
  mutable std::shared_mutex mutex_;
  std::atomic<std::uint64_t> version_{1};
  std::unordered_map<std::string, ClientEntitlement> clients_;
  FlatHashMap<std::uint64_t, VenueId, SymbolRawHash> symbol_venue_;
  std::vector<std::string> venue_names_;
};

}  // namespace cascade::dist
