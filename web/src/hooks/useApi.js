import { useState, useEffect } from "react";
import axios from "axios";

const api = axios.create({ baseURL: "/api" });

/**
 * Fetch top-100 snapshot for a given date with pagination.
 */
export function useTop100(date, page = 1, pageSize = 20) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!date) return;

    let cancelled = false;
    setLoading(true);
    setError(null);

    api
      .get("/top100", { params: { date, page, page_size: pageSize } })
      .then((res) => {
        if (!cancelled) setData(res.data);
      })
      .catch((err) => {
        if (!cancelled) setError(err.response?.data?.detail || err.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [date, page, pageSize]);

  return { data, loading, error };
}

/**
 * Fetch wallet graph edges and nodes.
 */
export function useWalletGraph(address, minVolume = 0.1, limit = 500) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!address) return;

    let cancelled = false;
    setLoading(true);
    setError(null);

    api
      .get(`/wallet/${address}/graph`, {
        params: { min_volume: minVolume, limit },
      })
      .then((res) => {
        if (!cancelled) setData(res.data);
      })
      .catch((err) => {
        if (!cancelled) setError(err.response?.data?.detail || err.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [address, minVolume, limit]);

  return { data, loading, error };
}

/**
 * Fetch API health / latest snapshot date.
 */
export function useHealth() {
  const [data, setData] = useState(null);

  useEffect(() => {
    api
      .get("/health")
      .then((res) => setData(res.data))
      .catch(() => {});
  }, []);

  return data;
}
