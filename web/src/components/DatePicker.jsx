import { memo } from "react";

/**
 * Date input for snapshot selection.
 *
 * Props:
 *   value       — controlled date string (YYYY-MM-DD)
 *   onChange    — called with new date string
 *   defaultDate — max date (latest available snapshot), used as placeholder/max
 *   loading     — true while the parent is still fetching the default date
 */
function DatePicker({ value, onChange, defaultDate = "", loading = false }) {
  return (
    <div className="controls">
      {/* label is inline-flex (see .controls label in index.css):
          [Snapshot date:] [<input date>] [Fetching…?]
          all three items aligned center with 12px gap               */}
      <label>
        <span>Snapshot date:</span>
        <input
          type="date"
          value={value || defaultDate}
          onChange={(e) => onChange(e.target.value)}
          max={defaultDate || undefined}
          disabled={loading && !defaultDate}
        />
        {loading && !value && (
          <span className="date-loading-hint">Fetching latest date…</span>
        )}
      </label>
    </div>
  );
}

export default memo(DatePicker);
