import { useState, useMemo, useCallback, useEffect, memo } from "react";
import { useNavigate } from "react-router-dom";
import { useTop100, useHealth, getPersistedDate, persistDate } from "../hooks/useApi";
import DatePicker from "./DatePicker";

const PAGE_SIZE = 20;

// ─── Skeleton placeholder shown before the first real response ────────────────
const SKELETON_WIDTHS = [40, 120, 60, 90, 50, 50, 80, 80];
const SKELETON_COLS   = ["Rank", "Wallet", "Txns", "Volume", "Sent", "Recv", "Sent ETH", "Recv ETH"];

function SkeletonTable() {
  return (
    <table>
      <thead>
        <tr>
          {SKELETON_COLS.map((h) => (
            <th key={h}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {Array.from({ length: 8 }).map((_, i) => (
          <tr key={i} className="skeleton-row">
            {SKELETON_WIDTHS.map((w, j) => (
              <td key={j}>
                <div className="skeleton" style={{ width: w }} />
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function truncateAddr(addr) {
  if (!addr || addr.length <= 12) return addr;
  return `${addr.slice(0, 6)}...${addr.slice(-4)}`;
}

function formatEth(val) {
  const num = parseFloat(val);
  if (isNaN(num)) return val;
  return num.toFixed(4) + " ETH";
}

// ─── Component ────────────────────────────────────────────────────────────────
function Top100Table() {
  const navigate  = useNavigate();
  // Initialise from module-level persisted date so Back navigation restores the
  // last chosen date without a re-fetch (cache will serve it instantly).
  const [date, setDate]       = useState(() => getPersistedDate());
  const [page, setPage]       = useState(1);
  const [sortCol, setSortCol] = useState("rank");
  const [sortAsc, setSortAsc] = useState(true);

  // ── Health: hoisted here so we own the default-date lifecycle ──────────────
  // useHealth() initialises from module-level cache synchronously when available,
  // so returning users see the date input populated with no visible loading flash.
  const { data: health, loading: healthLoading } = useHealth();

  // Auto-set date once health resolves and no date is chosen yet.
  // Also persists so a subsequent remount skips this async step entirely.
  useEffect(() => {
    if (!date && health?.last_snapshot_date) {
      setDate(health.last_snapshot_date);
      persistDate(health.last_snapshot_date);
    }
  }, [date, health?.last_snapshot_date]);

  // ── Data fetch ──────────────────────────────────────────────────────────────
  const { data, loading, error } = useTop100(date, page, PAGE_SIZE);

  // ── Handlers ────────────────────────────────────────────────────────────────
  const handleDateChange = useCallback((d) => {
    setDate(d);
    persistDate(d);   // keep in sync so Back navigation restores this date
    setPage(1);
  }, []);

  const handleSort = useCallback(
    (col) => {
      if (sortCol === col) {
        setSortAsc((prev) => !prev);
      } else {
        setSortCol(col);
        setSortAsc(col === "rank");
      }
    },
    [sortCol]
  );

  const handlePrev = useCallback(() => setPage((p) => p - 1), []);
  const handleNext = useCallback(() => setPage((p) => p + 1), []);

  const copyAddress = useCallback((e, addr) => {
    e.stopPropagation();
    navigator.clipboard?.writeText(addr);
  }, []);

  const sortIndicator = useCallback(
    (col) => {
      if (sortCol !== col) return "";
      return sortAsc ? " \u25B2" : " \u25BC";
    },
    [sortCol, sortAsc]
  );

  // ── Sorted rows (memoised) ──────────────────────────────────────────────────
  const items = useMemo(() => {
    if (!data?.items) return [];
    const sorted = [...data.items];
    sorted.sort((a, b) => {
      let aVal = a[sortCol];
      let bVal = b[sortCol];
      if (typeof aVal === "string" && !isNaN(parseFloat(aVal))) {
        aVal = parseFloat(aVal);
        bVal = parseFloat(bVal);
      }
      if (aVal < bVal) return sortAsc ? -1 : 1;
      if (aVal > bVal) return sortAsc ? 1 : -1;
      return 0;
    });
    return sorted;
  }, [data?.items, sortCol, sortAsc]);

  const totalPages = data ? Math.ceil(data.total_count / PAGE_SIZE) : 0;

  // ── Loading-state logic ─────────────────────────────────────────────────────
  // isInitialLoading: no table content yet → show skeleton
  const isInitialLoading = healthLoading || (loading && !data);
  // isReloading: table content exists, fetching an update → dim + loading bar
  const isReloading = loading && !!data;

  // ── Render ──────────────────────────────────────────────────────────────────
  return (
    <>
      <DatePicker
        value={date}
        onChange={handleDateChange}
        defaultDate={health?.last_snapshot_date || ""}
        loading={healthLoading}
      />

      {/* Thin animated progress bar while paginating / changing date */}
      {isReloading && <div className="loading-bar" />}

      {/* Skeleton rows on initial load */}
      {isInitialLoading && <SkeletonTable />}

      {/* Error (only shown once we have a date) */}
      {!isInitialLoading && error && (
        <div className="error">Error: {error}</div>
      )}

      {/* Table — kept mounted while reloading to avoid layout jump */}
      {!isInitialLoading && data && (
        <>
          <div style={{ position: "relative" }}>
            {/* Invisible click-blocker prevents interaction during reload */}
            {isReloading && <div className="table-overlay" />}

            <table
              style={{
                opacity: isReloading ? 0.5 : 1,
                transition: "opacity 0.15s",
              }}
            >
              <thead>
                <tr>
                  <th onClick={() => handleSort("rank")}>
                    Rank{sortIndicator("rank")}
                  </th>
                  <th>Wallet</th>
                  <th onClick={() => handleSort("total_txns")}>
                    Txns{sortIndicator("total_txns")}
                  </th>
                  <th onClick={() => handleSort("total_volume")}>
                    Volume{sortIndicator("total_volume")}
                  </th>
                  <th onClick={() => handleSort("sent_count")}>
                    Sent{sortIndicator("sent_count")}
                  </th>
                  <th onClick={() => handleSort("recv_count")}>
                    Recv{sortIndicator("recv_count")}
                  </th>
                  <th onClick={() => handleSort("sent_eth")}>
                    Sent ETH{sortIndicator("sent_eth")}
                  </th>
                  <th onClick={() => handleSort("recv_eth")}>
                    Recv ETH{sortIndicator("recv_eth")}
                  </th>
                </tr>
              </thead>
              <tbody>
                {items.map((w) => (
                  <tr
                    key={w.wallet_address}
                    onClick={() => navigate(`/wallet/${w.wallet_address}`)}
                  >
                    <td>{w.rank}</td>
                    <td
                      className="wallet-addr"
                      onClick={(e) => copyAddress(e, w.wallet_address)}
                      title={`${w.wallet_address}\nClick to copy`}
                    >
                      {truncateAddr(w.wallet_address)}
                    </td>
                    <td>{w.total_txns.toLocaleString()}</td>
                    <td>{formatEth(w.total_volume)}</td>
                    <td>{w.sent_count.toLocaleString()}</td>
                    <td>{w.recv_count.toLocaleString()}</td>
                    <td>{formatEth(w.sent_eth)}</td>
                    <td>{formatEth(w.recv_eth)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="pagination">
            <button disabled={page <= 1} onClick={handlePrev}>
              Previous
            </button>
            <span>
              Page {page} of {totalPages}
            </span>
            <button disabled={page >= totalPages} onClick={handleNext}>
              Next
            </button>
          </div>
        </>
      )}

      {!isInitialLoading && data?.items?.length === 0 && !loading && (
        <div className="loading">No snapshot data for {date}</div>
      )}
    </>
  );
}

export default memo(Top100Table);
