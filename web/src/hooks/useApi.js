import { useState, useEffect } from "react";
import axios from "axios";

const api = axios.create({ baseURL: "/api" });

// ─── In-memory response cache with TTL ───────────────────────────────────────
// Module-level so cache survives React re-renders and is shared across all hook
// instances in the same page session.
const _cache = new Map(); // key → { data, expiresAt }

const TTL = {
  health: 60_000,      // 1 min  — health data changes infrequently
  top100: 5 * 60_000,  // 5 min  — snapshot for a given date is immutable
  graph:  3 * 60_000,  // 3 min  — graph data is stable within a session
};

function cacheGet(key) {
  const entry = _cache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) { _cache.delete(key); return null; }
  return entry.data;
}

function cacheSet(key, data, ttlMs) {
  _cache.set(key, { data, expiresAt: Date.now() + ttlMs });
}

// ─── Persistent date — survives lazy-route unmount/remount ───────────────────
// Stored at module level so navigating to /wallet/:address and back does not
// reset the user's chosen date. Initialised from cache when health is fresh.
let _persistedDate = "";
export function getPersistedDate() { return _persistedDate; }
export function persistDate(d) { _persistedDate = d; }

// ─── Health singleton ─────────────────────────────────────────────────────────
// All useHealth() callers share a single in-flight request so /api/health is
// never fetched more than once per page load (or once per TTL expiry).
let _healthPromise = null;

function fetchHealth() {
  const cached = cacheGet("health");
  if (cached) return Promise.resolve(cached);
  if (!_healthPromise) {
    _healthPromise = api
      .get("/health")
      .then((res) => {
        cacheSet("health", res.data, TTL.health);
        _healthPromise = null;
        return res.data;
      })
      .catch((err) => {
        _healthPromise = null; // allow retry on next call
        throw err;
      });
  }
  return _healthPromise;
}

// ─── Debounce helper ──────────────────────────────────────────────────────────
function useDebouncedValue(value, delay) {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(t);
  }, [value, delay]);
  return debounced;
}

// ─── useHealth ────────────────────────────────────────────────────────────────
// Returns { data, loading, error }.
// Initialises synchronously from cache when available (no flicker on revisit).
export function useHealth() {
  const [data, setData] = useState(() => cacheGet("health"));
  const [loading, setLoading] = useState(!cacheGet("health"));
  const [error, setError] = useState(null);

  useEffect(() => {
    // Already have fresh data — nothing to do
    if (cacheGet("health")) {
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);

    fetchHealth()
      .then((result) => {
        if (!cancelled) {
          setData(result);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err.response?.data?.detail || err.message || "API unreachable");
          setLoading(false);
        }
      });

    return () => { cancelled = true; };
  }, []);

  return { data, loading, error };
}

// ─── useTop100 ────────────────────────────────────────────────────────────────
// Caches responses by (date, page, pageSize). On a cache hit the state is
// updated synchronously inside the effect with no network request.
export function useTop100(date, page = 1, pageSize = 20) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!date) return;

    const key = `top100:${date}:${page}:${pageSize}`;
    const cached = cacheGet(key);
    if (cached) {
      setData(cached);
      setLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    api
      .get("/top100", { params: { date, page, page_size: pageSize } })
      .then((res) => {
        cacheSet(key, res.data, TTL.top100);
        if (!cancelled) setData(res.data);
      })
      .catch((err) => {
        if (!cancelled) setError(err.response?.data?.detail || err.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [date, page, pageSize]);

  return { data, loading, error };
}

// ─── useWalletGraph ───────────────────────────────────────────────────────────
// Debounces minVolume by 350ms (slider drag → no burst of requests).
// Caches responses by (address, minVolume, limit).
export function useWalletGraph(address, minVolume = 0.1, limit = 500) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const debouncedMinVolume = useDebouncedValue(minVolume, 350);

  useEffect(() => {
    if (!address) return;

    const key = `graph:${address}:${debouncedMinVolume}:${limit}`;
    const cached = cacheGet(key);
    if (cached) {
      setData(cached);
      setLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    api
      .get(`/wallet/${address}/graph`, {
        params: { min_volume: debouncedMinVolume, limit },
      })
      .then((res) => {
        cacheSet(key, res.data, TTL.graph);
        if (!cancelled) setData(res.data);
      })
      .catch((err) => {
        if (!cancelled) setError(err.response?.data?.detail || err.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [address, debouncedMinVolume, limit]);

  return { data, loading, error };
}
