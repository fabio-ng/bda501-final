/**
 * Transform API graph response into D3-compatible node/link format.
 */
export function transformApiResponse(apiData, centerAddress) {
  const center = centerAddress.toLowerCase();

  const nodes = apiData.nodes.map((n) => ({
    id: n.address,
    isCenter: n.address === center,
  }));

  const links = apiData.edges.map((e) => ({
    source: e.from_wallet,
    target: e.to_wallet,
    volume: parseFloat(e.total_volume),
    txCount: e.tx_count,
  }));

  return { nodes, links };
}

/**
 * Map volume to edge width using log scale.
 * @param {number} volume - Edge total volume in ETH
 * @param {number} minVol - Minimum volume in the dataset
 * @param {number} maxVol - Maximum volume in the dataset
 * @returns {number} Pixel width (1–8)
 */
export function edgeWidth(volume, minVol, maxVol) {
  if (maxVol <= minVol || volume <= 0) return 1;
  const logMin = Math.log(Math.max(minVol, 0.001));
  const logMax = Math.log(Math.max(maxVol, 0.002));
  const logVal = Math.log(Math.max(volume, 0.001));
  const t = (logVal - logMin) / (logMax - logMin);
  return 1 + t * 7; // range: 1px to 8px
}

/**
 * Determine edge color based on direction relative to center wallet.
 * @param {string} source - Sender address
 * @param {string} centerAddr - Focused wallet address
 * @returns {string} CSS color
 */
export function edgeColor(source, centerAddr) {
  // Outbound from center = blue, inbound to center = green
  // Normalize both sides to lowercase — API may return mixed-case addresses
  const src = typeof source === "string" ? source.toLowerCase() : String(source).toLowerCase();
  return src === centerAddr.toLowerCase() ? "#58a6ff" : "#3fb950";
}

/**
 * Format volume for tooltip display.
 */
export function formatVolume(volume) {
  if (volume >= 1000) return volume.toFixed(1) + " ETH";
  if (volume >= 1) return volume.toFixed(4) + " ETH";
  return volume.toFixed(6) + " ETH";
}
