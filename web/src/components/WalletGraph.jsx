import { useEffect, useRef, useState } from "react";
import { useParams, Link } from "react-router-dom";
import * as d3 from "d3";
import { useWalletGraph } from "../hooks/useApi";
import {
  transformApiResponse,
  edgeWidth,
  edgeColor,
  formatVolume,
} from "../utils/graphLayout";

const WIDTH = 1160;
const HEIGHT = 700;

export default function WalletGraph() {
  const { address } = useParams();
  const svgRef = useRef(null);
  const tooltipRef = useRef(null);
  const [minVolume, setMinVolume] = useState(0.1);

  const { data, loading, error } = useWalletGraph(address, minVolume, 500);

  useEffect(() => {
    if (!data || !svgRef.current) return;

    const { nodes, links } = transformApiResponse(data, address);
    if (nodes.length === 0) return;

    // Volume range for edge width scaling
    const volumes = links.map((l) => l.volume);
    const minVol = Math.min(...volumes, 0.001);
    const maxVol = Math.max(...volumes, 0.001);

    // Clear previous render
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    // Container group for zoom/pan
    const g = svg.append("g");

    // Zoom behavior
    const zoom = d3.zoom().scaleExtent([0.1, 5]).on("zoom", (event) => {
      g.attr("transform", event.transform);
    });
    svg.call(zoom);

    // Simulation
    const simulation = d3
      .forceSimulation(nodes)
      .force(
        "link",
        d3
          .forceLink(links)
          .id((d) => d.id)
          .distance(100)
      )
      .force("charge", d3.forceManyBody().strength(-200))
      .force("center", d3.forceCenter(WIDTH / 2, HEIGHT / 2))
      .force("collide", d3.forceCollide(20));

    // Edges
    const link = g
      .append("g")
      .selectAll("line")
      .data(links)
      .join("line")
      .attr("stroke", (d) => edgeColor(d.source.id || d.source, address))
      .attr("stroke-width", (d) => edgeWidth(d.volume, minVol, maxVol))
      .attr("stroke-opacity", 0.6);

    // Nodes
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

    // Tooltip
    const tooltip = d3.select(tooltipRef.current);

    // Node hover
    node
      .on("mouseover", (event, d) => {
        const label = d.isCenter ? `${d.id} (center)` : d.id;
        tooltip
          .style("display", "block")
          .style("left", event.offsetX + 12 + "px")
          .style("top", event.offsetY - 10 + "px")
          .html(`<span class="wallet-addr">${label}</span>`);
      })
      .on("mouseout", () => tooltip.style("display", "none"));

    // Edge hover
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

    // Tick
    simulation.on("tick", () => {
      link
        .attr("x1", (d) => d.source.x)
        .attr("y1", (d) => d.source.y)
        .attr("x2", (d) => d.target.x)
        .attr("y2", (d) => d.target.y);
      node.attr("cx", (d) => d.x).attr("cy", (d) => d.y);
    });

    return () => simulation.stop();
  }, [data, address]);

  return (
    <>
      <Link to="/" className="back-link">
        &larr; Back to Top 100
      </Link>

      <h2 style={{ marginBottom: 12 }}>
        Wallet Graph:{" "}
        <span className="wallet-addr">
          {address.slice(0, 8)}...{address.slice(-6)}
        </span>
      </h2>

      {data && (
        <p style={{ color: "#8b949e", marginBottom: 8, fontSize: "0.85rem" }}>
          {data.nodes.length} nodes, {data.edges.length} edges
          {data.period_start &&
            ` | ${data.period_start} to ${data.period_end}`}
        </p>
      )}

      <div className="controls">
        <label>
          Min volume (ETH):
          <input
            type="range"
            min="0"
            max="10"
            step="0.1"
            value={minVolume}
            onChange={(e) => setMinVolume(parseFloat(e.target.value))}
          />
          <span style={{ marginLeft: 8 }}>{minVolume} ETH</span>
        </label>
      </div>

      {loading && <div className="loading">Loading graph...</div>}
      {error && <div className="error">Error: {error}</div>}

      {data && !loading && data.edges.length === 0 && (
        <div className="loading">
          No edges found for this wallet (try lowering min volume)
        </div>
      )}

      <div className="graph-container" style={{ position: "relative" }}>
        <svg ref={svgRef} width={WIDTH} height={HEIGHT} />
        <div
          ref={tooltipRef}
          className="tooltip"
          style={{ display: "none" }}
        />
      </div>

      <div style={{ marginTop: 12, fontSize: "0.8rem", color: "#8b949e" }}>
        <span style={{ color: "#58a6ff" }}>---</span> Outbound &nbsp;
        <span style={{ color: "#3fb950" }}>---</span> Inbound &nbsp;
        <span style={{ color: "#f0883e" }}>&#9679;</span> Center wallet
      </div>
    </>
  );
}

/**
 * D3 drag behavior for nodes.
 */
function drag(simulation) {
  return d3
    .drag()
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
