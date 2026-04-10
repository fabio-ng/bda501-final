import { useEffect, useRef, useState, useMemo, useCallback, memo } from "react";
import { useParams, Link } from "react-router-dom";
import { select } from "d3-selection";
import { forceSimulation, forceLink, forceManyBody, forceCenter, forceCollide } from "d3-force";
import { zoom as d3Zoom } from "d3-zoom";
import { drag as d3Drag } from "d3-drag";
import { useWalletGraph } from "../hooks/useApi";
import {
  transformApiResponse,
  edgeWidth,
  edgeColor,
  formatVolume,
} from "../utils/graphLayout";

const WIDTH  = 1160;
const HEIGHT = 700;

function WalletGraph() {
  const { address }  = useParams();
  const svgRef       = useRef(null);
  const tooltipRef   = useRef(null);
  const simulationRef = useRef(null);
  const [minVolume, setMinVolume] = useState(0.1);

  const { data, loading, error } = useWalletGraph(address, minVolume, 500);

  // Safe address display — guard against short/malformed addresses
  const displayAddr = useMemo(() => {
    if (!address || address.length <= 14) return address || "";
    return `${address.slice(0, 8)}...${address.slice(-6)}`;
  }, [address]);

  // Memoised transform — D3 effect only re-runs when the API response changes
  const graph = useMemo(() => {
    if (!data || !address) return null;
    return transformApiResponse(data, address);
  }, [data, address]);

  const handleMinVolumeChange = useCallback((e) => {
    setMinVolume(parseFloat(e.target.value));
  }, []);

  // ── D3 render effect ────────────────────────────────────────────────────────
  useEffect(() => {
    if (!graph || !svgRef.current) return;

    const { nodes, links } = graph;
    if (nodes.length === 0) return;

    const volumes = links.map((l) => l.volume);
    const minVol  = Math.min(...volumes, 0.001);
    const maxVol  = Math.max(...volumes, 0.001);

    const svg = select(svgRef.current);

    // Full cleanup before re-render
    if (simulationRef.current) {
      simulationRef.current.stop();
      simulationRef.current = null;
    }
    svg.on(".zoom", null);
    svg.selectAll("*").remove();

    const g = svg.append("g");

    const zoom = d3Zoom().scaleExtent([0.1, 5]).on("zoom", (event) => {
      g.attr("transform", event.transform);
    });
    svg.call(zoom);

    const simulation = forceSimulation(nodes)
      .force("link", forceLink(links).id((d) => d.id).distance(100))
      .force("charge", forceManyBody().strength(-200))
      .force("center", forceCenter(WIDTH / 2, HEIGHT / 2))
      .force("collide", forceCollide(20));

    simulationRef.current = simulation;

    const link = g
      .append("g")
      .selectAll("line")
      .data(links)
      .join("line")
      .attr("stroke", (d) => edgeColor(d.source.id || d.source, address))
      .attr("stroke-width", (d) => edgeWidth(d.volume, minVol, maxVol))
      .attr("stroke-opacity", 0.6);

    const node = g
      .append("g")
      .selectAll("circle")
      .data(nodes)
      .join("circle")
      .attr("r", (d) => (d.isCenter ? 12 : 6))
      .attr("fill", (d) => (d.isCenter ? "#f0883e" : "#8b949e"))
      .attr("stroke", "#0f1117")
      .attr("stroke-width", 1.5)
      .call(drag(simulation));

    const tooltip = select(tooltipRef.current);

    node
      .on("mouseover", (event, d) => {
        tooltip
          .style("display", "block")
          .style("left", event.offsetX + 12 + "px")
          .style("top", event.offsetY - 10 + "px")
          .html(
            `<span class="wallet-addr">${d.isCenter ? `${d.id} (center)` : d.id}</span>`
          );
      })
      .on("mouseout", () => tooltip.style("display", "none"));

    link
      .on("mouseover", (event, d) => {
        tooltip
          .style("display", "block")
          .style("left", event.offsetX + 12 + "px")
          .style("top", event.offsetY - 10 + "px")
          .html(
            `<strong>${formatVolume(d.volume)}</strong><br/>${d.txCount.toLocaleString()} txns`
          );
      })
      .on("mouseout", () => tooltip.style("display", "none"));

    simulation.on("tick", () => {
      link
        .attr("x1", (d) => d.source.x)
        .attr("y1", (d) => d.source.y)
        .attr("x2", (d) => d.target.x)
        .attr("y2", (d) => d.target.y);
      node.attr("cx", (d) => d.x).attr("cy", (d) => d.y);
    });

    return () => {
      simulation.stop();
      simulationRef.current = null;
      svg.on(".zoom", null);
    };
  }, [graph, address]);

  // ── Render ──────────────────────────────────────────────────────────────────
  return (
    <>
      <Link to="/" className="back-link">
        &larr; Back to Top 100
      </Link>

      <h2 style={{ marginBottom: 12 }}>
        Wallet Graph: <span className="wallet-addr">{displayAddr}</span>
      </h2>

      {data && (
        <p style={{ color: "#8b949e", marginBottom: 8, fontSize: "0.85rem" }}>
          {data.nodes.length} nodes, {data.edges.length} edges
          {data.period_start && ` | ${data.period_start} to ${data.period_end}`}
        </p>
      )}

      {/* label is inline-flex (see .controls label in index.css):
          [Min volume (ETH):] [<range>] [0.1 ETH]  — all aligned center, 12px gap */}
      <div className="controls">
        <label>
          <span>Min volume (ETH):</span>
          <input
            type="range"
            min="0"
            max="10"
            step="0.1"
            value={minVolume}
            onChange={handleMinVolumeChange}
          />
          <span>{minVolume} ETH</span>
        </label>
      </div>

      {error && <div className="error">Error: {error}</div>}

      {data && !loading && data.edges.length === 0 && (
        <div className="loading">
          No edges found for this wallet (try lowering min volume)
        </div>
      )}

      {/* Graph container — always rendered; overlay covers it while loading */}
      <div className="graph-container" style={{ position: "relative" }}>
        <svg
          ref={svgRef}
          width={WIDTH}
          height={HEIGHT}
          style={{ opacity: loading ? 0.35 : 1, transition: "opacity 0.2s" }}
        />

        {loading && (
          <div className="graph-loading-overlay">
            <div className="spinner" />
            <span>{data ? "Refreshing graph…" : "Loading graph…"}</span>
          </div>
        )}

        <div ref={tooltipRef} className="tooltip" style={{ display: "none" }} />
      </div>

      <div style={{ marginTop: 12, fontSize: "0.8rem", color: "#8b949e" }}>
        <span style={{ color: "#58a6ff" }}>---</span> Outbound &nbsp;
        <span style={{ color: "#3fb950" }}>---</span> Inbound &nbsp;
        <span style={{ color: "#f0883e" }}>&#9679;</span> Center wallet
      </div>
    </>
  );
}

function drag(simulation) {
  return d3Drag()
    .on("start", (event, d) => {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      d.fx = d.x;
      d.fy = d.y;
    })
    .on("drag", (event, d) => {
      d.fx = event.x;
      d.fy = event.y;
    })
    .on("end", (event, d) => {
      if (!event.active) simulation.alphaTarget(0);
      d.fx = null;
      d.fy = null;
    });
}

export default memo(WalletGraph);
