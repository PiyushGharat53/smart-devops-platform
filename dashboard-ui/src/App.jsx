// App.jsx
import React, { useState, useEffect, useRef, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import axios from "axios";
import {
  Shield, ChevronDown, Activity, AlertTriangle, Clock, Server,
  Cpu, MemoryStick, HardDrive, Wifi, Zap, Loader2, CheckCircle2,
  X, Terminal, Sparkles, Lock, Unlock, Database, Globe, MessagesSquare,
  CreditCard, KeyRound, PlayCircle, WifiOff, CheckCheck,
} from "lucide-react";

/* -------------------------------------------------------------------------- */
/*  BACKEND CONFIG — centralize both URLs here so there's one place to swap  */
/*  environments; wire these to real env vars in your build setup.           */
/* -------------------------------------------------------------------------- */
const BACKEND_HTTP_URL = "https://sentinel-aiops-engine.onrender.com";
const BACKEND_WS_URL = "wss://sentinel-aiops-engine.onrender.com/ws/telemetry";
const WS_RECONNECT_DELAY_MS = 3000;

/* -------------------------------------------------------------------------- */
/*  GLOBAL CSS                                                                */
/* -------------------------------------------------------------------------- */
const GLOBAL_CSS = `
.sso-root, .sso-root * { box-sizing: border-box; }
.sso-root {
  --bg-deep-start: #14113a; --bg-deep-mid: #0f0d22; --bg-deep-end: #090714;
  --border-glow: rgba(99,102,241,0.2); --glass-fill: rgba(255,255,255,0.03);
}
@keyframes sso-spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
@keyframes sso-ping { 0% { transform: scale(1); opacity: 0.6; } 75%, 100% { transform: scale(2.2); opacity: 0; } }
@keyframes sso-pulse-glow {
  0% { box-shadow: 0 0 0px rgba(139,92,246,0.4); }
  50% { box-shadow: 0 0 18px rgba(139,92,246,0.7); }
  100% { box-shadow: 0 0 0px rgba(139,92,246,0.4); }
}
.sso-spin { animation: sso-spin 0.9s linear infinite; }
.sso-ping { animation: sso-ping 1.6s cubic-bezier(0,0,0.2,1) infinite; }
.sso-pulse-glow { animation: sso-pulse-glow 2.4s ease-in-out infinite; }
.sso-scroll::-webkit-scrollbar { width: 8px; height: 8px; }
.sso-scroll::-webkit-scrollbar-track { background: transparent; }
.sso-scroll::-webkit-scrollbar-thumb { background: rgba(139,92,246,0.35); border-radius: 8px; }
.sso-scroll { scrollbar-width: thin; scrollbar-color: rgba(139,92,246,0.35) transparent; }
.sso-btn { cursor: pointer; transition: background-color 0.18s ease; }
.sso-btn:disabled { cursor: not-allowed; opacity: 0.5; }
.sso-workspace-btn:hover, .sso-dropdown-item:hover, .sso-service-row:hover, .sso-incident-row:hover, .sso-dismiss-btn:hover { background: rgba(255,255,255,0.05) !important; }
.sso-heal-btn:hover { background: rgba(139,92,246,0.26) !important; }
.sso-execute-btn:hover { background: rgba(139,92,246,0.35) !important; }
.sso-queue-btn:hover { background: rgba(245,158,11,0.26) !important; }
.sso-trigger-btn:hover { background: rgba(99,102,241,0.32) !important; }
.sso-kpi-grid { display: grid; grid-template-columns: repeat(1, minmax(0, 1fr)); gap: 1rem; }
@media (min-width: 640px) { .sso-kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (min-width: 1024px) { .sso-kpi-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); } }
.sso-main-grid { display: flex; flex-direction: column; gap: 1.5rem; }
@media (min-width: 1024px) {
  .sso-main-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); }
  .sso-col-span-3 { grid-column: span 3 / span 3; }
  .sso-col-span-2 { grid-column: span 2 / span 2; }
}
`;

// Fallback used only if GET /api/workspaces can't be reached — keeps the
// switcher usable even if the backend is briefly unavailable on load.
const FALLBACK_WORKSPACES = [
  { id: "finsight", label: "FinSight Financial Engine", env: "Production", service_ids: ["gateway", "mongo"] },
  { id: "chatbot", label: "Campus Multilingual Chatbot", env: "Staging", service_ids: ["chatbot"] },
  { id: "core", label: "Core Microservices Cluster", env: "All Services", service_ids: null },
];

const SERVICE_ICONS = { gateway: Globe, auth: KeyRound, payments: CreditCard, mongo: Database, nlp: MessagesSquare, chatbot: MessagesSquare, ecommerce: CreditCard };
const STATUS_META = {
  healthy: { color: "#22c55e", label: "Healthy" },
  degraded: { color: "#f59e0b", label: "Degraded" },
  failed: { color: "#ef4444", label: "Failed" },
  healing: { color: "#8b5cf6", label: "Healing…" },
};
const LOG_LEVEL_META = {
  INFO: { color: "#38bdf8" }, ANOMALY: { color: "#f59e0b" }, "AUTO-HEAL": { color: "#a78bfa" }, REMEDIATED: { color: "#22c55e" },
};
const SEVERITY_META = {
  CRITICAL: { color: "#fca5a5", bg: "rgba(239,68,68,0.15)" },
  WARNING: { color: "#fcd34d", bg: "rgba(245,158,11,0.15)" },
  INFO: { color: "#7dd3fc", bg: "rgba(56,189,248,0.15)" },
};

function GlassPanel({ children, style = {}, ...rest }) {
  return (
    <div style={{ borderRadius: "1rem", border: "1px solid var(--border-glow)", background: "var(--glass-fill)", backdropFilter: "blur(16px)", ...style }} {...rest}>
      {children}
    </div>
  );
}

function StatusDot({ status }) {
  const meta = STATUS_META[status] || STATUS_META.failed;
  return (
    <span style={{ position: "relative", display: "inline-flex", height: 10, width: 10 }}>
      {status !== "failed" && <span className="sso-ping" style={{ position: "absolute", height: "100%", width: "100%", borderRadius: "9999px", backgroundColor: meta.color }} />}
      <span style={{ position: "relative", height: 10, width: 10, borderRadius: "9999px", backgroundColor: meta.color }} />
    </span>
  );
}

function KpiCard({ icon: Icon, label, value, sub, accent }) {
  return (
    <GlassPanel style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: "0.75rem" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: "0.2em", color: "#94a3b8" }}>{label}</span>
        <div style={{ padding: 8, borderRadius: 8, backgroundColor: `${accent}1a`, color: accent, display: "flex" }}><Icon size={16} /></div>
      </div>
      <span style={{ fontSize: 30, fontWeight: 600, color: "#ffffff", letterSpacing: "-0.02em" }}>{value}</span>
      {sub && <span style={{ fontSize: 11, color: "#64748b" }}>{sub}</span>}
    </GlassPanel>
  );
}

function Gauge({ icon: Icon, label, value, unit, accent }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 14 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, color: "#cbd5e1" }}><Icon size={14} style={{ color: accent }} /><span>{label}</span></div>
        <span style={{ fontFamily: "monospace", color: "#e2e8f0" }}>{Number(value).toFixed(1)}{unit}</span>
      </div>
      <div style={{ height: 8, borderRadius: 9999, backgroundColor: "rgba(255,255,255,0.05)", overflow: "hidden" }}>
        <motion.div style={{ height: "100%", borderRadius: 9999, backgroundColor: accent }} initial={{ width: 0 }} animate={{ width: `${Math.min(value, 100)}%` }} transition={{ duration: 0.8 }} />
      </div>
    </div>
  );
}

function WorkspaceSwitcher({ workspaces, activeWorkspace, onSelect }) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);

  useEffect(() => {
    function handleClickOutside(e) {
      if (rootRef.current && !rootRef.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  return (
    <div ref={rootRef} style={{ position: "relative" }}>
      <button
        onClick={() => setOpen((o) => !o)}
        className="sso-btn sso-workspace-btn"
        style={{ display: "flex", alignItems: "center", gap: 10, padding: "0.5rem 0.9rem", borderRadius: 12, border: "1px solid rgba(255,255,255,0.08)", background: "rgba(255,255,255,0.03)", color: "#e2e8f0" }}
      >
        <div style={{ textAlign: "left" }}>
          <p style={{ margin: 0, fontSize: 13, fontWeight: 500, color: "#f1f5f9" }}>{activeWorkspace?.label || "Select Workspace"}</p>
          <p style={{ margin: 0, fontSize: 10, color: "#8b5cf6" }}>{activeWorkspace?.env}</p>
        </div>
        <motion.span animate={{ rotate: open ? 180 : 0 }} transition={{ duration: 0.15 }} style={{ display: "flex" }}>
          <ChevronDown size={16} color="#94a3b8" />
        </motion.span>
      </button>

      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ opacity: 0, y: -6, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -6, scale: 0.98 }}
            transition={{ duration: 0.15 }}
            style={{ position: "absolute", top: "calc(100% + 8px)", right: 0, width: 260, borderRadius: 12, border: "1px solid rgba(139,92,246,0.25)", background: "linear-gradient(160deg, #14113a, #0c0a1f)", boxShadow: "0 12px 32px rgba(0,0,0,0.4)", overflow: "hidden", zIndex: 40 }}
          >
            {workspaces.map((ws) => (
              <button
                key={ws.id}
                onClick={() => { onSelect(ws.id); setOpen(false); }}
                className="sso-btn sso-dropdown-item"
                style={{ width: "100%", display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0.7rem 0.9rem", border: "none", background: ws.id === activeWorkspace?.id ? "rgba(139,92,246,0.12)" : "transparent", color: "#e2e8f0", textAlign: "left" }}
              >
                <div>
                  <p style={{ margin: 0, fontSize: 13 }}>{ws.label}</p>
                  <p style={{ margin: 0, fontSize: 10, color: "#64748b" }}>{ws.env}</p>
                </div>
                {ws.id === activeWorkspace?.id && <CheckCircle2 size={14} color="#a78bfa" />}
              </button>
            ))}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function ConnectionBadge({ state }) {
  const meta = {
    connecting: { color: "#fbbf24", label: "Connecting…", icon: Loader2, spin: true },
    live: { color: "#22c55e", label: "Live", icon: CheckCheck, spin: false },
    reconnecting: { color: "#f59e0b", label: "Reconnecting…", icon: Loader2, spin: true },
    offline: { color: "#ef4444", label: "Offline", icon: WifiOff, spin: false },
  }[state] || { color: "#64748b", label: "Unknown", icon: WifiOff, spin: false };
  const Icon = meta.icon;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6, padding: "0.35rem 0.7rem", borderRadius: 9999, border: `1px solid ${meta.color}40`, background: `${meta.color}12`, fontSize: 11, color: meta.color }}>
      <Icon size={12} className={meta.spin ? "sso-spin" : ""} />
      {meta.label}
    </div>
  );
}

export default function App() {
  const [workspaces, setWorkspaces] = useState(FALLBACK_WORKSPACES);
  const [workspaceId, setWorkspaceId] = useState(FALLBACK_WORKSPACES[0].id);
  const [autonomous, setAutonomous] = useState(true);

  // LIVE WEBSOCKET STATE
  const [services, setServices] = useState([]);
  const [incidents, setIncidents] = useState([]);
  const [logs, setLogs] = useState([]);
  const [telemetry, setTelemetry] = useState({ cpu: 0, mem: 0, disk: 0, net: 0 });
  const [deployment, setDeployment] = useState({ status: "idle", stage: "Pipeline Ready & Listening" });
  const [connectionState, setConnectionState] = useState("connecting"); // connecting | live | reconnecting | offline
  const [triggeringPipeline, setTriggeringPipeline] = useState(false);

  const [activeIncident, setActiveIncident] = useState(null);
  const terminalRef = useRef(null);
  const wsRef = useRef(null);
  const reconnectTimerRef = useRef(null);

  const activeWorkspace = workspaces.find((w) => w.id === workspaceId) || workspaces[0];

  // Filter services by the active workspace's declared membership.
  // service_ids === null means "show everything" (the core/all-services workspace).
  const visibleServices = activeWorkspace?.service_ids
    ? services.filter((s) => activeWorkspace.service_ids.includes(s.id))
    : services;

  // Fetch workspace definitions once from the backend; fall back silently
  // to the local constant if the request fails so the UI stays usable.
  useEffect(() => {
    let cancelled = false;
    axios.get(`${BACKEND_HTTP_URL}/api/workspaces`)
      .then((res) => {
        if (cancelled) return;
        const fetched = res.data?.workspaces;
        if (Array.isArray(fetched) && fetched.length > 0) {
          setWorkspaces(fetched);
          setWorkspaceId(fetched[0].id);
        }
      })
      .catch(() => {
        // Keep the fallback constant already in state — no-op.
      });
    return () => { cancelled = true; };
  }, []);

  // Auto-scroll the terminal
  useEffect(() => {
    if (terminalRef.current) terminalRef.current.scrollTop = terminalRef.current.scrollHeight;
  }, [logs]);

  // WEBSOCKET CONNECTION with basic auto-reconnect + connection status
  useEffect(() => {
    let isUnmounted = false;

    function connect() {
      setConnectionState((prev) => (prev === "live" ? "reconnecting" : "connecting"));
      const ws = new WebSocket(BACKEND_WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        if (isUnmounted) return;
        setConnectionState("live");
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          setTelemetry({
            cpu: data.metrics.cpu_usage,
            mem: data.metrics.memory_usage,
            disk: data.metrics.disk_usage,
            net: data.metrics.network_throughput,
          });
          setServices(data.services || []);
          setLogs(data.logs || []);
          setIncidents(data.incidents || []);
          if (data.deployment) setDeployment(data.deployment);
        } catch (err) {
          console.error("Failed to parse telemetry payload", err);
        }
      };

      ws.onerror = (err) => {
        console.error("WebSocket error:", err);
      };

      ws.onclose = () => {
        if (isUnmounted) return;
        setConnectionState("offline");
        reconnectTimerRef.current = setTimeout(connect, WS_RECONNECT_DELAY_MS);
      };
    }

    connect();

    return () => {
      isUnmounted = true;
      if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
      wsRef.current?.close();
    };
  }, []);

  const healService = useCallback(async (id) => {
    setServices((prev) => prev.map((s) => (s.id === id ? { ...s, status: "healing" } : s)));
    try {
      await axios.post(`${BACKEND_HTTP_URL}/api/heal/${id}`);
      // The WebSocket will naturally broadcast the fixed state back to us.
    } catch (error) {
      console.error("Heal failed", error);
      // Revert the optimistic update so the UI doesn't lie about state.
      setServices((prev) => prev.map((s) => (s.id === id ? { ...s, status: "failed" } : s)));
    }
  }, []);

  const triggerManualPreflight = useCallback(async () => {
    setTriggeringPipeline(true);
    try {
      await axios.post(`${BACKEND_HTTP_URL}/api/pipeline/trigger`, {
        project: activeWorkspace?.id || "core",
      });
    } catch (error) {
      console.error("Manual pre-flight trigger failed", error);
    } finally {
      setTriggeringPipeline(false);
    }
  }, [activeWorkspace]);

  const healthyCount = visibleServices.filter((s) => s.status === "healthy").length;
  const healthRate = visibleServices.length > 0 ? ((healthyCount / visibleServices.length) * 100).toFixed(1) : "0.0";
  const activeIncidentCount = incidents.filter((i) => i.status !== "Resolved").length;
  const isPipelineActive = deployment.status !== "idle";

  return (
    <div className="sso-root" style={{ minHeight: "100vh", width: "100%", color: "#f1f5f9", fontFamily: "-apple-system, sans-serif", background: "radial-gradient(circle at 20% 0%, var(--bg-deep-start) 0%, var(--bg-deep-mid) 45%, var(--bg-deep-end) 100%)" }}>
      <style>{GLOBAL_CSS}</style>

      {/* HEADER */}
      <header style={{ position: "sticky", top: 0, zIndex: 30, borderBottom: "1px solid rgba(255,255,255,0.05)", backdropFilter: "blur(16px)" }}>
        <div style={{ maxWidth: 1600, margin: "0 auto", padding: "1rem 1.5rem", display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 12 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div className="sso-pulse-glow" style={{ display: "flex", alignItems: "center", justifyContent: "center", height: 40, width: 40, borderRadius: 12, background: "linear-gradient(135deg, rgba(99,102,241,0.25), rgba(139,92,246,0.15))" }}>
              <Shield size={20} color="#c4b5fd" />
            </div>
            <div>
              <h1 style={{ fontSize: 18, fontWeight: 600, color: "#ffffff", margin: 0 }}>Sentinel SmartOps</h1>
              <p style={{ fontSize: 11, color: "#64748b", margin: 0 }}>Autonomous DevOps &amp; Self-Healing Command Center</p>
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.75rem", flexWrap: "wrap" }}>
            <ConnectionBadge state={connectionState} />
            <WorkspaceSwitcher workspaces={workspaces} activeWorkspace={activeWorkspace} onSelect={setWorkspaceId} />
            <button onClick={() => setAutonomous((a) => !a)} className="sso-btn" style={{ display: "flex", alignItems: "center", gap: 8, padding: "0.5rem 1rem", borderRadius: 12, fontSize: 14, border: autonomous ? "1px solid rgba(139,92,246,0.45)" : "1px solid rgba(245,158,11,0.4)", background: autonomous ? "rgba(139,92,246,0.12)" : "rgba(245,158,11,0.1)", color: autonomous ? "#ddd6fe" : "#fde68a" }}>
              {autonomous ? <Unlock size={14} color="#c4b5fd" /> : <Lock size={14} color="#fcd34d" />}
              <span>{autonomous ? "Autonomous Mode" : "Human-in-the-Loop"}</span>
            </button>
          </div>
        </div>
      </header>

      <main style={{ maxWidth: 1600, margin: "0 auto", padding: "1.5rem", display: "flex", flexDirection: "column", gap: "1.5rem" }}>

        {/* PERMANENT CI/CD PIPELINE DECK — always visible, idle or active */}
        <GlassPanel style={{
          padding: "1.25rem", display: "flex", flexDirection: "column", gap: 12,
          border: deployment.status === "failed" || deployment.status === "rolled_back" ? "1px solid rgba(239,68,68,0.5)"
            : deployment.status === "success" ? "1px solid rgba(34,197,94,0.5)"
            : isPipelineActive ? "1px solid rgba(99,102,241,0.5)"
            : "1px solid var(--border-glow)",
          background: deployment.status === "failed" || deployment.status === "rolled_back" ? "rgba(239,68,68,0.05)"
            : deployment.status === "success" ? "rgba(34,197,94,0.05)"
            : isPipelineActive ? "rgba(99,102,241,0.05)"
            : "var(--glass-fill)",
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 8 }}>
            <h2 style={{ fontSize: 14, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
              <Zap size={15} color={deployment.status === "failed" || deployment.status === "rolled_back" ? "#ef4444" : deployment.status === "success" ? "#22c55e" : "#a5b4fc"} />
              CI/CD Pipeline
            </h2>
            {!isPipelineActive && (
              <button
                onClick={triggerManualPreflight}
                disabled={triggeringPipeline}
                className="sso-btn sso-trigger-btn"
                style={{ display: "flex", alignItems: "center", gap: 8, padding: "0.45rem 0.9rem", borderRadius: 10, fontSize: 12.5, backgroundColor: "rgba(99,102,241,0.18)", color: "#c7d2fe", border: "1px solid rgba(99,102,241,0.4)" }}
              >
                {triggeringPipeline ? <Loader2 size={13} className="sso-spin" /> : <PlayCircle size={13} />}
                Trigger Manual Pre-Flight Test
              </button>
            )}
          </div>

          <AnimatePresence mode="wait">
            {!isPipelineActive ? (
              <motion.div
                key="idle"
                initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ display: "flex", alignItems: "center", gap: 10 }}
              >
                <StatusDot status="healthy" />
                <p style={{ margin: 0, fontSize: 13.5, color: "#94a3b8" }}>Pipeline Ready &amp; Listening — waiting for a push or manual trigger.</p>
              </motion.div>
            ) : (
              <motion.div
                key="active"
                initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -8 }}
                style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 12 }}
              >
                <div>
                  <p style={{ margin: 0, fontSize: 15, fontWeight: 600, color: "#f8fafc" }}>Commit: {deployment.commit_hash}</p>
                  <p style={{ margin: "4px 0 0 0", fontSize: 13, color: "#94a3b8" }}>Pushed by {deployment.author} — "{deployment.message}"</p>
                </div>
                <div style={{ textAlign: "right", display: "flex", flexDirection: "column", alignItems: "flex-end" }}>
                  <p style={{ margin: 0, fontSize: 14, fontWeight: 500, color: deployment.status === "in_progress" ? "#fbbf24" : deployment.status === "failed" || deployment.status === "rolled_back" ? "#ef4444" : "#22c55e" }}>
                    {deployment.stage}
                  </p>
                  {deployment.status === "in_progress" && <Loader2 size={16} className="sso-spin" style={{ color: "#fbbf24", marginTop: 8 }} />}
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </GlassPanel>

        {/* KPI ROW */}
        <div className="sso-kpi-grid">
          <KpiCard icon={Server} label="Monitored Nodes" value={`${visibleServices.length} Active`} accent="#6366f1" />
          <KpiCard icon={Activity} label="Cluster Health Rate" value={`${healthRate}%`} sub="Uptime, rolling 24h" accent="#22c55e" />
          <KpiCard icon={AlertTriangle} label="Active Incidents" value={activeIncidentCount} sub="Awaiting or in remediation" accent="#ef4444" />
          <KpiCard icon={Clock} label="MTTR" value="1.2s" sub="Avg AI auto-heal time" accent="#8b5cf6" />
        </div>

        <div className="sso-main-grid">
          {/* LEFT: SERVICES */}
          <GlassPanel className="sso-col-span-3" style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: 16 }}>
            <h2 style={{ fontSize: 14, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
              <Server size={15} color="#a5b4fc" /> Monitored Services <span style={{ color: "#64748b", fontWeight: 400 }}>· {activeWorkspace?.label}</span>
            </h2>
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              {visibleServices.length === 0 && (
                <p style={{ fontSize: 13, color: "#64748b", margin: 0 }}>No services registered under this workspace yet.</p>
              )}
              {visibleServices.map((svc) => {
                const Icon = SERVICE_ICONS[svc.id] || Server;
                const meta = STATUS_META[svc.status] || STATUS_META.failed;
                return (
                  <motion.div layout key={svc.id} className="sso-service-row" style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: 16, borderRadius: 12, border: "1px solid rgba(255,255,255,0.05)", background: "rgba(255,255,255,0.02)" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                      <div style={{ padding: 8, borderRadius: 8, backgroundColor: `${meta.color}1a`, color: meta.color, display: "flex" }}><Icon size={16} /></div>
                      <div>
                        <p style={{ fontSize: 14, color: "#e2e8f0", margin: 0 }}>{svc.name}</p>
                        <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 4 }}>
                          <StatusDot status={svc.status} />
                          <span style={{ fontSize: 11, color: meta.color }}>{meta.label}</span>
                          {svc.status !== "healing" && <span style={{ fontSize: 11, color: "#64748b" }}>· {svc.latency > 0 ? `${svc.latency}ms` : "unreachable"}</span>}
                        </div>
                      </div>
                    </div>
                    {(svc.status === "degraded" || svc.status === "failed") && (
                      <button onClick={() => healService(svc.id)} className="sso-btn sso-heal-btn" style={{ display: "flex", alignItems: "center", gap: 6, padding: "0.4rem 0.75rem", borderRadius: 8, fontSize: 12, backgroundColor: "rgba(139,92,246,0.16)", color: "#c4b5fd", border: "1px solid rgba(139,92,246,0.4)" }}>
                        <Zap size={13} /> Auto-Heal
                      </button>
                    )}
                    {svc.status === "healing" && <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#c4b5fd" }}><Loader2 size={13} className="sso-spin" /> Healing…</div>}
                    {svc.status === "healthy" && <CheckCircle2 size={16} color="#34d399" />}
                  </motion.div>
                );
              })}
            </div>
          </GlassPanel>

          {/* RIGHT: TELEMETRY */}
          <GlassPanel className="sso-col-span-2" style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: 20 }}>
            <h2 style={{ fontSize: 14, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}><Cpu size={15} color="#a5b4fc" /> Infrastructure Telemetry</h2>
            <Gauge icon={Cpu} label="CPU Utilization" value={telemetry.cpu} unit="%" accent={telemetry.cpu > 85 ? "#ef4444" : "#6366f1"} />
            <Gauge icon={MemoryStick} label="Memory Footprint" value={telemetry.mem} unit="%" accent={telemetry.mem > 85 ? "#ef4444" : "#8b5cf6"} />
            <Gauge icon={HardDrive} label="Disk I/O" value={telemetry.disk} unit="%" accent={telemetry.disk > 80 ? "#f59e0b" : "#22c55e"} />
            <Gauge icon={Wifi} label="Network Throughput" value={telemetry.net} unit=" MB/s" accent="#38bdf8" />
          </GlassPanel>
        </div>

        {/* TERMINAL */}
        <GlassPanel style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: 12 }}>
          <h2 style={{ fontSize: 14, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}><Terminal size={15} color="#a5b4fc" /> AIOps Execution Log</h2>
          <div ref={terminalRef} className="sso-scroll" style={{ height: 256, overflowY: "auto", borderRadius: 12, background: "rgba(0,0,0,0.4)", border: "1px solid rgba(255,255,255,0.05)", padding: 16, fontFamily: "monospace", fontSize: "12.5px", lineHeight: 1.6 }}>
            <AnimatePresence initial={false}>
              {logs.map((log) => (
                <motion.div key={log.id} initial={{ opacity: 0, x: -6 }} animate={{ opacity: 1, x: 0 }} style={{ display: "flex", gap: 8 }}>
                  <span style={{ color: "#475569" }}>[{log.time}]</span>
                  <span style={{ fontWeight: 600, color: LOG_LEVEL_META[log.level]?.color || "#ffffff" }}>[{log.level}]</span>
                  <span style={{ color: "#cbd5e1" }}>{log.msg}</span>
                </motion.div>
              ))}
            </AnimatePresence>
          </div>
        </GlassPanel>

        {/* PERSISTENT INCIDENT FEED — active + recently resolved, always visible */}
        <GlassPanel style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: 12 }}>
          <h2 style={{ fontSize: 14, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}><AlertTriangle size={15} color="#a5b4fc" /> Incident Feed</h2>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {incidents.length === 0 && <p style={{ fontSize: 14, color: "#64748b", margin: 0 }}>No incidents recorded yet.</p>}
            {incidents.slice().reverse().map((inc) => {
              const resolved = inc.status === "Resolved";
              const sevMeta = SEVERITY_META[inc.severity] || SEVERITY_META.WARNING;
              return (
                <button
                  key={inc.id}
                  onClick={() => setActiveIncident(inc)}
                  className="sso-btn sso-incident-row"
                  style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0.9rem 1rem", textAlign: "left", borderRadius: 8, border: "none", background: "rgba(255,255,255,0.02)", color: "#f1f5f9", opacity: resolved ? 0.55 : 1 }}
                >
                  <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                    <span style={{ fontSize: 10, fontWeight: 600, padding: "4px 8px", borderRadius: 9999, color: sevMeta.color, backgroundColor: sevMeta.bg }}>{inc.severity}</span>
                    {resolved && (
                      <span style={{ fontSize: 10, fontWeight: 600, padding: "4px 8px", borderRadius: 9999, color: "#86efac", backgroundColor: "rgba(34,197,94,0.15)", display: "flex", alignItems: "center", gap: 4 }}>
                        <CheckCircle2 size={11} /> RESOLVED
                      </span>
                    )}
                    <div>
                      <p style={{ fontSize: 14, color: "#e2e8f0", margin: 0 }}>{inc.title || inc.service}</p>
                      <p style={{ fontSize: 11, color: "#64748b", margin: 0 }}>{inc.service}</p>
                    </div>
                  </div>
                  <span style={{ fontSize: 11, color: "#64748b" }}>{inc.time}</span>
                </button>
              );
            })}
          </div>
        </GlassPanel>
      </main>

      {/* RCA MODAL */}
      <AnimatePresence>
        {activeIncident && (
          <motion.div style={{ position: "fixed", inset: 0, zIndex: 50, display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onClick={() => setActiveIncident(null)}>
            <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.7)", backdropFilter: "blur(4px)" }} />
            <motion.div initial={{ opacity: 0, scale: 0.94, y: 12 }} animate={{ opacity: 1, scale: 1, y: 0 }} exit={{ opacity: 0, scale: 0.96, y: 8 }} onClick={(e) => e.stopPropagation()} style={{ position: "relative", width: "100%", maxWidth: 512, borderRadius: 16, border: "1px solid rgba(139,92,246,0.2)", padding: 24, display: "flex", flexDirection: "column", gap: 20, background: "linear-gradient(160deg, #14113a, #0c0a1f)" }}>
              <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 16 }}>
                <div>
                  <h3 style={{ fontSize: 18, fontWeight: 600, color: "#ffffff", margin: "0" }}>{activeIncident.title || activeIncident.service}</h3>
                  <p style={{ fontSize: 12, color: "#64748b", margin: "2px 0 0 0" }}>{activeIncident.service} · {activeIncident.time}</p>
                </div>
                <button onClick={() => setActiveIncident(null)} className="sso-btn sso-close-btn" style={{ padding: 6, borderRadius: 8, border: "none", background: "transparent", color: "#94a3b8", display: "flex" }}><X size={16} /></button>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 12, padding: 12, borderRadius: 12, border: "1px solid rgba(139,92,246,0.2)", background: "rgba(139,92,246,0.06)" }}>
                <Sparkles size={18} color="#c4b5fd" />
                <div><p style={{ fontSize: 12, color: "#94a3b8", margin: 0 }}>AI Confidence Score</p><p style={{ fontSize: 20, fontWeight: 600, color: "#ddd6fe", margin: 0 }}>{activeIncident.confidence}%</p></div>
              </div>
              <div><p style={{ fontSize: 12, textTransform: "uppercase", letterSpacing: "0.05em", color: "#64748b", margin: "0 0 6px 0" }}>Identified Root Cause</p><p style={{ fontSize: 14, color: "#cbd5e1", lineHeight: 1.6, margin: 0 }}>{activeIncident.rootCause}</p></div>
              <div><p style={{ fontSize: 12, textTransform: "uppercase", letterSpacing: "0.05em", color: "#64748b", margin: "0 0 6px 0" }}>Recommended Remediation</p><p style={{ fontSize: 14, color: "#cbd5e1", lineHeight: 1.6, margin: 0 }}>{activeIncident.remediation}</p></div>
              <div style={{ display: "flex", gap: 12 }}>
                {activeIncident.status === "Resolved" ? (
                  <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 8, padding: "0.65rem 1rem", borderRadius: 12, fontSize: 14, color: "#86efac", backgroundColor: "rgba(34,197,94,0.12)", border: "1px solid rgba(34,197,94,0.3)" }}>
                    <CheckCircle2 size={14} /> Already Resolved
                  </div>
                ) : (
                  <button
                    onClick={() => { healService(activeIncident.service_id); setActiveIncident(null); }}
                    className="sso-btn sso-execute-btn"
                    style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 8, padding: "0.65rem 1rem", borderRadius: 12, fontSize: 14, fontWeight: 500, color: "#ede9fe", backgroundColor: "rgba(139,92,246,0.25)", border: "1px solid rgba(139,92,246,0.5)" }}
                  >
                    <Zap size={14} /> Execute Fix
                  </button>
                )}
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}