import { useHealth } from "../hooks/useApi";

/**
 * Date input that defaults to the latest available snapshot date.
 */
export default function DatePicker({ value, onChange }) {
  const health = useHealth();
  const defaultDate = health?.last_snapshot_date || "";

  // Set default when health data loads
  if (!value && defaultDate) {
    // Defer to avoid state-update-during-render
    setTimeout(() => onChange(defaultDate), 0);
  }

  return (
    <div className="controls">
      <label>
        Snapshot date:
        <input
          type="date"
          value={value || defaultDate}
          onChange={(e) => onChange(e.target.value)}
          max={defaultDate || undefined}
        />
      </label>
    </div>
  );
}
