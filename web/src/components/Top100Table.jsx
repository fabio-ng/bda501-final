import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useTop100 } from "../hooks/useApi";
import DatePicker from "./DatePicker";

const PAGE_SIZE = 20;

/**
 * Truncate wallet address for display: 0x1234...abcd
 */
function truncateAddr(addr) {
  if (!addr || addr.length <= 12) return addr;
  return `${addr.slice(0, 6)}...${addr.slice(-4)}`;
}

/**
 * Format ETH value: truncate to 4 decimals + " ETH"
 */
function formatEth(val) {
  const num = parseFloat(val);
  if (isNaN(num)) return val;
  return num.toFixed(4) + " ETH";
}

export default function Top100Table() {
  const navigate = useNavigate();
  const [date, setDate] = useState("");
  const [page, setPage] = useState(1);
  const [sortCol, setSortCol] = useState("rank");
  const [sortAsc, setSortAsc] = useState(true);

  const { data, loading, error } = useTop100(date, page, PAGE_SIZE);

  const handleSort = (col) => {
    if (sortCol === col) {
      setSortAsc(!sortAsc);
    } else {
      setSortCol(col);
      setSortAsc(col === "rank"); // default ascending for rank
    }
  };

  // Sort items locally (within current page)
  const items = data?.items ? [...data.items] : [];
  items.sort((a, b) => {
    let aVal = a[sortCol];
    let bVal = b[sortCol];
    // Numeric sort for number-like fields
    if (typeof aVal === "string" && !isNaN(parseFloat(aVal))) {
      aVal = parseFloat(aVal);
      bVal = parseFloat(bVal);
    }
    if (aVal < bVal) return sortAsc ? -1 : 1;
    if (aVal > bVal) return sortAsc ? 1 : -1;
    return 0;
  });

  const totalPages = data ? Math.ceil(data.total_count / PAGE_SIZE) : 0;

  const copyAddress = (e, addr) => {
    e.stopPropagation();
    navigator.clipboard?.writeText(addr);
  };

  const sortIndicator = (col) => {
    if (sortCol !== col) return "";
    return sortAsc ? " \u25B2" : " \u25BC";
  };

  return (
    <>
      <DatePicker value={date} onChange={(d) => { setDate(d); setPage(1); }} />

      {loading && <div className="loading">Loading...</div>}
      {error && <div className="error">Error: {error}</div>}

      {data && !loading && (
        <>
          <table>
            <thead>
              <tr>
                <th onClick={() => handleSort("rank")}>Rank{sortIndicator("rank")}</th>
                <th>Wallet</th>
                <th onClick={() => handleSort("total_txns")}>Txns{sortIndicator("total_txns")}</th>
                <th onClick={() => handleSort("total_volume")}>Volume{sortIndicator("total_volume")}</th>
                <th onClick={() => handleSort("sent_count")}>Sent{sortIndicator("sent_count")}</th>
                <th onClick={() => handleSort("recv_count")}>Recv{sortIndicator("recv_count")}</th>
                <th onClick={() => handleSort("sent_eth")}>Sent ETH{sortIndicator("sent_eth")}</th>
                <th onClick={() => handleSort("recv_eth")}>Recv ETH{sortIndicator("recv_eth")}</th>
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

          <div className="pagination">
            <button disabled={page <= 1} onClick={() => setPage(page - 1)}>
              Previous
            </button>
            <span>
              Page {page} of {totalPages}
            </span>
            <button disabled={page >= totalPages} onClick={() => setPage(page + 1)}>
              Next
            </button>
          </div>
        </>
      )}

      {data && data.items.length === 0 && !loading && (
        <div className="loading">No snapshot data for {date}</div>
      )}
    </>
  );
}
