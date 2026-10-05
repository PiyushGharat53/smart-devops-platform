import React, { useState, useEffect, useRef, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import axios from "axios";
import {
  Shield, ChevronDown, Activity, AlertTriangle, Clock, Server,
  Cpu, MemoryStick, HardDrive, Wifi, Zap, Loader2, CheckCircle2,
  X, Terminal, Sparkles, Lock, Unlock, Database, Globe, MessagesSquare,
  CreditCard, KeyRound, PlayCircle, WifiOff, CheckCheck, Send, FileCode,
  Flame, BellRing, Settings, RefreshCw, Check, ShieldAlert, ShieldCheck, Mail, UserX, ExternalLink, HelpCircle
} from "lucide-react";
import LiveTrafficChart from "./components/LiveTrafficChart";

/* -------------------------------------------------------------------------- */
/*  BACKEND CONFIG (Universal Vite & Vercel Compatible)                       */
/* -------------------------------------------------------------------------- */
const RAW_HTTP_URL = (
  import.meta.env?.VITE_BACKEND_HTTP_URL ||
  import.meta.env?.VITE_BACKEND_URL ||
  import.meta.env?.VITE_API_URL ||
  "https://sentinel-aiops-engine.onrender.com"
).replace(/\/+$/, "");

export const BACKEND_HTTP_URL = RAW_HTTP_URL;

const computeWsUrl = () => {
  const customWs = import.meta.env?.VITE_BACKEND_WS_URL || import.meta.env?.VITE_WS_URL;
  if (customWs) {
    return customWs.endsWith("/ws/telemetry")
      ? customWs
      : `${customWs.replace(/\/+$/, "")}/ws/telemetry`;
  }
  const proto = RAW_HTTP_URL.startsWith("https://") ? "wss://" : "ws://";
  const host = RAW_HTTP_URL.replace(/^https?:\/\//, "");
  return `${proto}${host}/ws/telemetry`;
};

export const BACKEND_WS_URL = computeWsUrl();
const WS_RECONNECT_DELAY_MS = 3000;

/* -------------------------------------------------------------------------- */
/*  GLOBAL CSS                                                                */
/* -------------------------------------------------------------------------- */
const GLOBAL_CSS = `
.sso-root, .sso-root * { box-sizing: border-box; }
.sso-root {
  --bg-deep-start: #14113a; --bg-deep-mid: #0f0d22; --bg-deep-end: #090714;
  --border-glow: rgba(99,102,241,0.22); --glass-fill: rgba(255,255,255,0.03);
}
@keyframes sso-spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
@keyframes sso-ping { 0% { transform: scale(1); opacity: 0.6; } 75%, 100% { transform: scale(2.2); opacity: 0; } }
@keyframes sso-pulse-glow {
  0% { box-shadow: 0 0 0px rgba(139,92,246,0.4); }
  50% { box-shadow: 0 0 20px rgba(139,92,246,0.7); }
  100% { box-shadow: 0 0 0px rgba(139,92,246,0.4); }
}
.sso-spin { animation: sso-spin 0.9s linear infinite; }
.sso-ping { animation: sso-ping 1.6s cubic-bezier(0,0,0.2,1) infinite; }
.sso-pulse-glow { animation: sso-pulse-glow 2.4s ease-in-out infinite; }
.sso-scroll::-webkit-scrollbar { width: 8px; height: 8px; }
.sso-scroll::-webkit-scrollbar-track { background: transparent; }
.sso-scroll::-webkit-scrollbar-thumb { background: rgba(139,92,246,0.35); border-radius: 8px; }
.sso-scroll { scrollbar-width: thin; scrollbar-color: rgba(139,92,246,0.35) transparent; }
.sso-btn { cursor: pointer; transition: all 0.18s ease; }
.sso-btn:disabled { cursor: not-allowed; opacity: 0.5; }
.sso-workspace-btn:hover, .sso-dropdown-item:hover, .sso-service-row:hover, .sso-incident-row:hover, .sso-dismiss-btn:hover { background: rgba(255,255,255,0.06) !important; }
.sso-heal-btn:hover { background: rgba(139,92,246,0.3) !important; }
.sso-execute-btn:hover { background: rgba(139,92,246,0.38) !important; }
.sso-queue-btn:hover { background: rgba(245,158,11,0.26) !important; }
.sso-trigger-btn:hover { background: rgba(99,102,241,0.35) !important; }
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
  INFO: { color: "#38bdf8" }, ANOMALY: { color: "#f59e0b" }, "AUTO-HEAL": { color: "#a78bfa" }, REMEDIATED: { color: "#22c55e" }, CRITICAL: { color: "#ef4444" }, WARN: { color: "#f59e0b" }
};
const SEVERITY_META = {
  CRITICAL: { color: "#fca5a5", bg: "rgba(239,68,68,0.18)" },
  HIGH: { color: "#fdba74", bg: "rgba(249,115,22,0.18)" },
  WARNING: { color: "#fcd34d", bg: "rgba(245,158,11,0.18)" },
  INFO: { color: "#7dd3fc", bg: "rgba(56,189,248,0.18)" },
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
      <span style={{ fontSize: 28, fontWeight: 700, color: "#ffffff", letterSpacing: "-0.02em" }}>{value}</span>
      {sub && <span style={{ fontSize: 11, color: "#64748b" }}>{sub}</span>}
    </GlassPanel>
  );
}

function Gauge({ icon: Icon, label, value, unit, accent }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13.5 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, color: "#cbd5e1" }}><Icon size={14} style={{ color: accent }} /><span>{label}</span></div>
        <span style={{ fontFamily: "monospace", color: "#e2e8f0" }}>{Number(value).toFixed(1)}{unit}</span>
      </div>
      <div style={{ height: 8, borderRadius: 9999, backgroundColor: "rgba(255,255,255,0.06)", overflow: "hidden" }}>
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
            style={{ position: "absolute", top: "calc(100% + 8px)", right: 0, width: 260, borderRadius: 12, border: "1px solid rgba(139,92,246,0.25)", background: "linear-gradient(160deg, #14113a, #0c0a1f)", boxShadow: "0 12px 32px rgba(0,0,0,0.5)", overflow: "hidden", zIndex: 40 }}
          >
            {workspaces.map((ws) => (
              <button
                key={ws.id}
                onClick={() => { onSelect(ws.id); setOpen(false); }}
                className="sso-btn sso-dropdown-item"
                style={{ width: "100%", display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0.7rem 0.9rem", border: "none", background: ws.id === activeWorkspace?.id ? "rgba(139,92,246,0.15)" : "transparent", color: "#e2e8f0", textAlign: "left" }}
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
    live: { color: "#22c55e", label: "Live Telemetry", icon: CheckCheck, spin: false },
    reconnecting: { color: "#f59e0b", label: "Reconnecting…", icon: Loader2, spin: true },
    offline: { color: "#ef4444", label: "Offline", icon: WifiOff, spin: false },
  }[state] || { color: "#64748b", label: "Unknown", icon: WifiOff, spin: false };
  const Icon = meta.icon;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6, padding: "0.35rem 0.75rem", borderRadius: 9999, border: `1px solid ${meta.color}40`, background: `${meta.color}15`, fontSize: 11.5, color: meta.color, fontWeight: 500 }}>
      <Icon size={12} className={meta.spin ? "sso-spin" : ""} />
      {meta.label}
    </div>
  );
}

// Formats timestamps into Indian Standard Time (Mumbai, India / UTC+5:30)
function formatISTTime(timeStr, createdAt) {
  if (createdAt) {
    try {
      const d = new Date(createdAt);
      if (!isNaN(d.getTime())) {
        return d.toLocaleTimeString("en-IN", {
          timeZone: "Asia/Kolkata",
          hour12: false,
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit"
        });
      }
    } catch (_) {}
  }
  return timeStr || "--:--:--";
}

const FALLBACK_SERVICES = [
  { id: "gateway", name: "FinSight API Gateway", status: "healthy", latency: 42 },
  { id: "mongo", name: "Primary MongoDB Cluster", status: "healthy", latency: 25 },
];

function getSeedTrafficHistory() {
  const points = [];
  const now = Date.now();
  for (let i = 12; i > 0; i--) {
    const d = new Date(now - i * 2000);
    const t = d.toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata", hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
    points.push({
      time: t,
      rps: +(2.1 + Math.random() * 0.8).toFixed(2),
      latency: 42,
      defense_active: false,
      heap_used: 20.1,
      heap_total: 23.5,
      rss: 74.2,
      db_status: "CONNECTED",
      uptime: 120
    });
  }
  return points;
}

export default function App() {
  const [workspaces, setWorkspaces] = useState(FALLBACK_WORKSPACES);
  const [workspaceId, setWorkspaceId] = useState(FALLBACK_WORKSPACES[0].id);
  const [autonomous, setAutonomous] = useState(true);

  // Real-Time Mumbai, India (IST) Clock
  const [currentMumbaiTime, setCurrentMumbaiTime] = useState(() =>
    new Date().toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata", hour12: false })
  );

  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentMumbaiTime(
        new Date().toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata", hour12: false })
      );
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  // LIVE TELEMETRY STATE
  const [services, setServices] = useState(FALLBACK_SERVICES);
  const [incidents, setIncidents] = useState([]);
  const [logs, setLogs] = useState([]);
  const [telemetry, setTelemetry] = useState({ cpu: 42, mem: 58, disk: 48, net: 52 });
  const [deployment, setDeployment] = useState({ status: "idle", stage: "Pipeline Ready & Listening" });
  
  // Real-Time Traffic Watchdog State
  const [trafficHistory, setTrafficHistory] = useState(getSeedTrafficHistory);
  const [defenseModeActive, setDefenseModeActive] = useState(false);
  const [isSimulating, setIsSimulating] = useState(false);

  // Active Defense IP Quarantine Jail & Threat Mitigation State
  const [jailedIps, setJailedIps] = useState([
    {
      ip: "198.51.100.84",
      threat_level: "CRITICAL",
      incident_id: "INC-2085",
      reason: "Volumetric traffic burst (14.8 req/s) exceeding threshold (8.0 req/s)",
      jailed_at: "21:50:40",
      status: "RELEASED (Self-Healed)",
      requests_blocked: 28,
      action_taken: "Direct HTTP 429 Security Challenge Dispatched",
      "auto_release_in": "Remediated"
    }
  ]);
  const [showThreatMatrixModal, setShowThreatMatrixModal] = useState(false);
  const [inspectingChallenge, setInspectingChallenge] = useState(null);
  const [dispatchingEmail, setDispatchingEmail] = useState(false);
  const [emailSuccessMsg, setEmailSuccessMsg] = useState("");

  // MongoDB Atlas Persistence & 30-Day TTL State
  const [mongoConnected, setMongoConnected] = useState(false);
  const [mongoDetails, setMongoDetails] = useState(null);
  const [showMongoModal, setShowMongoModal] = useState(false);
  const [mongoUriInput, setMongoUriInput] = useState("");
  const [mongoStatusMsg, setMongoStatusMsg] = useState("");
  const [savingMongo, setSavingMongo] = useState(false);

  // Modals & Panels
  const [connectionState, setConnectionState] = useState("connecting");
  const [triggeringPipeline, setTriggeringPipeline] = useState(false);
  const [activeIncident, setActiveIncident] = useState(null);
  const [showConfigModal, setShowConfigModal] = useState(false);
  const [sentinelConfigContent, setSentinelConfigContent] = useState("");
  const [showDiscordModal, setShowDiscordModal] = useState(false);
  const [discordWebhookInput, setDiscordWebhookInput] = useState("");
  const [discordConfigured, setDiscordConfigured] = useState(false);
  const [discordStatusMsg, setDiscordStatusMsg] = useState("");

  const terminalRef = useRef(null);
  const wsRef = useRef(null);
  const reconnectTimerRef = useRef(null);

  const activeWorkspace = workspaces.find((w) => w.id === workspaceId) || workspaces[0];

  const visibleServices = activeWorkspace?.service_ids
    ? services.filter((s) => activeWorkspace.service_ids.includes(s.id))
    : services;

  // Initial config, mongo status & workspaces fetch
  const fetchMongoStatus = useCallback(() => {
    axios.get(`${BACKEND_HTTP_URL}/api/config/mongo-status`)
      .then((res) => {
        setMongoConnected(Boolean(res.data?.connected));
        setMongoDetails(res.data);
      })
      .catch(() => {});
  }, []);

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
      .catch(() => {});

    axios.get(`${BACKEND_HTTP_URL}/api/config`)
      .then((res) => {
        if (cancelled) return;
        if (res.data?.discord_configured) setDiscordConfigured(true);
        if (res.data?.mongo_connected !== undefined) setMongoConnected(Boolean(res.data.mongo_connected));
      })
      .catch(() => {});

    fetchMongoStatus();

    return () => { cancelled = true; };
  }, [fetchMongoStatus]);

  // Auto-scroll terminal log
  useEffect(() => {
    if (terminalRef.current) terminalRef.current.scrollTop = terminalRef.current.scrollHeight;
  }, [logs]);

  // WEBSOCKET MANAGEMENT
  useEffect(() => {
    let isUnmounted = false;

    function connect() {
      if (isUnmounted) return;
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
          if (data.metrics) {
            setTelemetry({
              cpu: data.metrics.cpu_usage || 0,
              mem: data.metrics.memory_usage || 0,
              disk: data.metrics.disk_usage || 0,
              net: data.metrics.network_throughput || 0,
            });
          }
          if (Array.isArray(data.services)) setServices(data.services);
          if (Array.isArray(data.logs)) setLogs(data.logs);
          if (Array.isArray(data.incidents)) setIncidents(data.incidents);
          if (data.deployment) setDeployment(data.deployment);
          
          if (Array.isArray(data.traffic_history)) setTrafficHistory(data.traffic_history);
          setDefenseModeActive(Boolean(data.defense_mode_active));
          if (Array.isArray(data.jailed_ips)) setJailedIps(data.jailed_ips);
          if (data.mongo_status) setMongoConnected(Boolean(data.mongo_status.connected));
        } catch (err) {
          console.error("Failed to parse telemetry payload", err);
        }
      };

      ws.onerror = (err) => {
        console.warn("WebSocket stream notice:", err);
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
      if (wsRef.current) {
        wsRef.current.onclose = null;
        wsRef.current.close();
      }
    };
  }, []);

  // ACTION: Auto-Heal Service
  const healService = useCallback(async (id) => {
    setServices((prev) => prev.map((s) => (s.id === id ? { ...s, status: "healing" } : s)));
    try {
      await axios.post(`${BACKEND_HTTP_URL}/api/heal/${id}`);
    } catch (error) {
      console.error("Heal request failed", error);
      setServices((prev) => prev.map((s) => (s.id === id ? { ...s, status: "failed" } : s)));
    }
  }, []);

  // ACTION: Trigger CI/CD Pre-Flight Quality Gates
  const triggerManualPreflight = useCallback(async () => {
    setTriggeringPipeline(true);
    try {
      await axios.post(`${BACKEND_HTTP_URL}/api/pipeline/trigger`, {
        project: activeWorkspace?.id || "finsight",
      });
    } catch (error) {
      console.error("Manual pre-flight trigger failed", error);
    } finally {
      setTriggeringPipeline(false);
    }
  }, [activeWorkspace]);

  // ACTION: Simulate Spike (14.5 RPS)
  const handleSimulateSpike = async () => {
    setIsSimulating(true);
    try {
      await axios.post(`${BACKEND_HTTP_URL}/api/traffic/simulate-surge`, {
        rps: 14.8,
        duration: 3
      });
    } catch (err) {
      console.error("Simulate spike error", err);
    } finally {
      setTimeout(() => setIsSimulating(false), 2000);
    }
  };

  // ACTION: Real HTTP Burst Test (25 requests to FinSight)
  const handleRealBurst = async () => {
    setIsSimulating(true);
    try {
      await axios.post(`${BACKEND_HTTP_URL}/api/traffic/real-burst`);
    } catch (err) {
      console.error("Real burst error", err);
    } finally {
      setTimeout(() => setIsSimulating(false), 1500);
    }
  };

  // ACTION: Fetch .sentinel-config.yml
  const handleOpenConfigModal = async () => {
    setShowConfigModal(true);
    try {
      const res = await axios.get(`${BACKEND_HTTP_URL}/api/sentinel-config`);
      setSentinelConfigContent(res.data?.content || "No configuration content found.");
    } catch (err) {
      setSentinelConfigContent("# Error loading .sentinel-config.yml from backend.");
    }
  };

  // ACTION: Save Discord Webhook
  const handleSaveDiscordWebhook = async () => {
    if (!discordWebhookInput.trim()) return;
    setDiscordStatusMsg("Connecting & sending verification embed...");
    try {
      const res = await axios.post(`${BACKEND_HTTP_URL}/api/config/discord-webhook`, {
        webhook_url: discordWebhookInput.trim()
      });
      if (res.data?.success) {
        setDiscordConfigured(true);
        setDiscordStatusMsg("✅ Webhook connected! Check your Discord channel for verification alert.");
        setTimeout(() => setShowDiscordModal(false), 2500);
      } else {
        setDiscordStatusMsg(`❌ Failed: ${res.data?.message}`);
      }
    } catch (err) {
      setDiscordStatusMsg(`❌ Error: ${err.message}`);
    }
  };

  // ACTION: Save / Connect MongoDB Atlas
  const handleSaveMongoUri = async () => {
    if (!mongoUriInput.trim()) return;
    setSavingMongo(true);
    setMongoStatusMsg("Testing connection & registering 30-Day TTL Index...");
    try {
      const res = await axios.post(`${BACKEND_HTTP_URL}/api/config/mongo-uri`, {
        mongo_uri: mongoUriInput.trim()
      });
      if (res.data?.success) {
        setMongoConnected(true);
        setMongoStatusMsg("✅ Connected to MongoDB Atlas! 30-Day TTL auto-purge index active.");
        fetchMongoStatus();
        setTimeout(() => setShowMongoModal(false), 2200);
      } else {
        setMongoStatusMsg(`❌ ${res.data?.message}`);
      }
    } catch (err) {
      setMongoStatusMsg(`❌ Connection error: ${err.message}`);
    } finally {
      setSavingMongo(false);
    }
  };

  const healthyCount = visibleServices.filter((s) => s.status === "healthy").length;
  const healthRate = visibleServices.length > 0 ? ((healthyCount / visibleServices.length) * 100).toFixed(1) : "100.0";
  const activeIncidentCount = incidents.filter((i) => i.status !== "Resolved").length;
  const isPipelineActive = deployment.status !== "idle";

  return (
    <div className="sso-root" style={{ minHeight: "100vh", width: "100%", color: "#f1f5f9", fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif", background: "radial-gradient(circle at 20% 0%, var(--bg-deep-start) 0%, var(--bg-deep-mid) 45%, var(--bg-deep-end) 100%)" }}>
      <style>{GLOBAL_CSS}</style>

      {/* HEADER */}
      <header style={{ position: "sticky", top: 0, zIndex: 30, borderBottom: "1px solid rgba(255,255,255,0.06)", backdropFilter: "blur(16px)", background: "rgba(15, 13, 34, 0.75)" }}>
        <div style={{ maxWidth: 1600, margin: "0 auto", padding: "0.9rem 1.5rem", display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 12 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div className="sso-pulse-glow" style={{ display: "flex", alignItems: "center", justifyContent: "center", height: 42, width: 42, borderRadius: 12, background: "linear-gradient(135deg, rgba(99,102,241,0.3), rgba(139,92,246,0.2))", border: "1px solid rgba(139,92,246,0.4)" }}>
              <Shield size={22} color="#c4b5fd" />
            </div>
            <div>
              <h1 style={{ fontSize: 18, fontWeight: 700, color: "#ffffff", margin: 0, letterSpacing: "-0.01em" }}>Sentinel SmartOps</h1>
              <p style={{ fontSize: 11, color: "#94a3b8", margin: 0 }}>Autonomous DevOps &amp; Self-Healing Platform</p>
            </div>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: "0.75rem", flexWrap: "wrap" }}>
            <ConnectionBadge state={connectionState} />

            {/* Real-Time Mumbai (IST) Clock */}
            <div
              title="Real-Time Clock Synchronized to Indian Standard Time (Mumbai, India / UTC+5:30)"
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                padding: "0.42rem 0.85rem",
                borderRadius: 12,
                fontSize: 12,
                fontFamily: "ui-monospace, SFMono-Regular, monospace",
                fontWeight: 500,
                border: "1px solid rgba(56,189,248,0.35)",
                background: "rgba(56,189,248,0.08)",
                color: "#38bdf8"
              }}
            >
              <Clock size={13} color="#38bdf8" />
              <span>IST {currentMumbaiTime}</span>
            </div>

            {/* MongoDB Atlas Persistence Badge & Modal Trigger */}
            <button
              onClick={() => { setShowMongoModal(true); fetchMongoStatus(); }}
              className="sso-btn"
              title="MongoDB Atlas Lifecycle & 30-Day TTL Auto-Purge Policy"
              style={{
                display: "flex", alignItems: "center", gap: 6, padding: "0.45rem 0.85rem",
                borderRadius: 12, fontSize: 12,
                border: mongoConnected ? "1px solid rgba(34,197,94,0.4)" : "1px solid rgba(139,92,246,0.4)",
                background: mongoConnected ? "rgba(34,197,94,0.12)" : "rgba(139,92,246,0.12)",
                color: mongoConnected ? "#86efac" : "#c4b5fd"
              }}
            >
              <Database size={13} color={mongoConnected ? "#86efac" : "#c4b5fd"} />
              <span>{mongoConnected ? "MongoDB Atlas (30D TTL)" : "MongoDB Atlas"}</span>
            </button>

            {/* Discord Webhook Button */}
            <button
              onClick={() => setShowDiscordModal(true)}
              className="sso-btn"
              title="Configure Discord Operations Webhook"
              style={{
                display: "flex", alignItems: "center", gap: 6, padding: "0.45rem 0.85rem",
                borderRadius: 12, fontSize: 12,
                border: discordConfigured ? "1px solid rgba(34,197,94,0.4)" : "1px solid rgba(99,102,241,0.4)",
                background: discordConfigured ? "rgba(34,197,94,0.12)" : "rgba(99,102,241,0.12)",
                color: discordConfigured ? "#86efac" : "#c7d2fe"
              }}
            >
              <BellRing size={13} color={discordConfigured ? "#86efac" : "#a5b4fc"} />
              <span>{discordConfigured ? "Discord Live" : "Connect Discord"}</span>
            </button>

            {/* View .sentinel-config.yml Button */}
            <button
              onClick={handleOpenConfigModal}
              className="sso-btn"
              title="Inspect .sentinel-config.yml CI/CD Policy Gates"
              style={{
                display: "flex", alignItems: "center", gap: 6, padding: "0.45rem 0.85rem",
                borderRadius: 12, fontSize: 12,
                border: "1px solid rgba(139,92,246,0.35)",
                background: "rgba(139,92,246,0.1)",
                color: "#ddd6fe"
              }}
            >
              <FileCode size={13} color="#c4b5fd" />
              <span>.sentinel-config.yml</span>
            </button>

            <WorkspaceSwitcher workspaces={workspaces} activeWorkspace={activeWorkspace} onSelect={setWorkspaceId} />

            <button
              onClick={() => setAutonomous((a) => !a)}
              className="sso-btn"
              style={{
                display: "flex", alignItems: "center", gap: 8, padding: "0.45rem 0.95rem",
                borderRadius: 12, fontSize: 12.5,
                border: autonomous ? "1px solid rgba(139,92,246,0.45)" : "1px solid rgba(245,158,11,0.4)",
                background: autonomous ? "rgba(139,92,246,0.15)" : "rgba(245,158,11,0.1)",
                color: autonomous ? "#ddd6fe" : "#fde68a"
              }}
            >
              {autonomous ? <Unlock size={13} color="#c4b5fd" /> : <Lock size={13} color="#fcd34d" />}
              <span>{autonomous ? "Autonomous SRE" : "Manual Mode"}</span>
            </button>
          </div>
        </div>
      </header>

      <main style={{ maxWidth: 1600, margin: "0 auto", padding: "1.5rem", display: "flex", flexDirection: "column", gap: "1.5rem" }}>

        {/* 6. CI/CD PIPELINE DECK (.sentinel-config.yml Gates) */}
        <GlassPanel style={{
          padding: "1.25rem", display: "flex", flexDirection: "column", gap: 12,
          border: deployment.status === "failed" || deployment.status === "rolled_back" ? "1px solid rgba(239,68,68,0.5)"
            : deployment.status === "success" ? "1px solid rgba(34,197,94,0.5)"
            : isPipelineActive ? "1px solid rgba(99,102,241,0.5)"
            : "1px solid var(--border-glow)",
          background: deployment.status === "failed" || deployment.status === "rolled_back" ? "rgba(239,68,68,0.06)"
            : deployment.status === "success" ? "rgba(34,197,94,0.06)"
            : isPipelineActive ? "rgba(99,102,241,0.06)"
            : "var(--glass-fill)",
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 8 }}>
            <h2 style={{ fontSize: 14.5, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
              <Zap size={16} color={deployment.status === "failed" || deployment.status === "rolled_back" ? "#ef4444" : deployment.status === "success" ? "#22c55e" : "#a5b4fc"} />
              CI/CD Pre-Flight Quality Gates <span style={{ color: "#64748b", fontWeight: 400, fontSize: 12 }}>(Governed by .sentinel-config.yml)</span>
            </h2>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              {!isPipelineActive && (
                <button
                  onClick={triggerManualPreflight}
                  disabled={triggeringPipeline}
                  className="sso-btn sso-trigger-btn"
                  style={{ display: "flex", alignItems: "center", gap: 8, padding: "0.45rem 0.9rem", borderRadius: 10, fontSize: 12, backgroundColor: "rgba(99,102,241,0.18)", color: "#c7d2fe", border: "1px solid rgba(99,102,241,0.4)" }}
                >
                  {triggeringPipeline ? <Loader2 size={13} className="sso-spin" /> : <PlayCircle size={13} />}
                  Trigger Pre-Flight Quality Gates
                </button>
              )}
            </div>
          </div>

          <AnimatePresence mode="wait">
            {!isPipelineActive ? (
              <motion.div
                key="idle"
                initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ display: "flex", alignItems: "center", gap: 10 }}
              >
                <StatusDot status="healthy" />
                <p style={{ margin: 0, fontSize: 13, color: "#94a3b8" }}>
                  Pipeline Ready &amp; Listening — Enforcing Gates: <strong>npm install</strong> · <strong>npm audit</strong> · <strong>node --check</strong> · <strong>eslint</strong> · <strong>Secret Shield</strong>.
                </p>
              </motion.div>
            ) : (
              <motion.div
                key="active"
                initial={{ opacity: 0, y: -6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -6 }}
                style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 12 }}
              >
                <div>
                  <p style={{ margin: 0, fontSize: 14.5, fontWeight: 600, color: "#f8fafc" }}>Commit: {deployment.commit_hash}</p>
                  <p style={{ margin: "4px 0 0 0", fontSize: 12.5, color: "#94a3b8" }}>Pushed by {deployment.author} — "{deployment.message}"</p>
                </div>
                <div style={{ textAlign: "right", display: "flex", flexDirection: "column", alignItems: "flex-end" }}>
                  <p style={{ margin: 0, fontSize: 13.5, fontWeight: 500, color: deployment.status === "in_progress" ? "#fbbf24" : deployment.status === "failed" || deployment.status === "rolled_back" ? "#ef4444" : "#22c55e" }}>
                    {deployment.stage}
                  </p>
                  {deployment.status === "in_progress" && <Loader2 size={16} className="sso-spin" style={{ color: "#fbbf24", marginTop: 6 }} />}
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </GlassPanel>

        {/* KPI METRIC CARDS */}
        <div className="sso-kpi-grid">
          <KpiCard icon={Server} label="Monitored Services" value={`${visibleServices.length} Active`} accent="#6366f1" />
          <KpiCard icon={Activity} label="Cluster Health Rate" value={`${healthRate}%`} sub="Uptime, rolling 24h" accent="#22c55e" />
          <KpiCard icon={AlertTriangle} label="Active Incidents" value={activeIncidentCount} sub={defenseModeActive ? "Active Defense Shield ON" : "Normal operations"} accent={defenseModeActive ? "#ef4444" : "#8b5cf6"} />
          <KpiCard icon={Clock} label="Target MTTR" value="1.2s" sub="Autonomous AIOps Closed-Loop" accent="#38bdf8" />
        </div>

        <div className="sso-main-grid">
          {/* LEFT COLUMN: MONITORED SERVICES */}
          <GlassPanel className="sso-col-span-3" style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: 16 }}>
            <h2 style={{ fontSize: 14.5, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
              <Server size={16} color="#a5b4fc" /> Monitored Services <span style={{ color: "#64748b", fontWeight: 400 }}>· {activeWorkspace?.label}</span>
            </h2>
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              {visibleServices.length === 0 && (
                <p style={{ fontSize: 13, color: "#64748b", margin: 0 }}>No services registered under this workspace yet.</p>
              )}
              {visibleServices.map((svc) => {
                const Icon = SERVICE_ICONS[svc.id] || Server;
                const meta = STATUS_META[svc.status] || STATUS_META.failed;
                return (
                  <motion.div layout key={svc.id} className="sso-service-row" style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: 16, borderRadius: 12, border: "1px solid rgba(255,255,255,0.06)", background: "rgba(255,255,255,0.02)" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                      <div style={{ padding: 8, borderRadius: 8, backgroundColor: `${meta.color}1a`, color: meta.color, display: "flex" }}><Icon size={16} /></div>
                      <div>
                        <p style={{ fontSize: 14, color: "#e2e8f0", margin: 0, fontWeight: 500 }}>{svc.name}</p>
                        <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 4 }}>
                          <StatusDot status={svc.status} />
                          <span style={{ fontSize: 11.5, color: meta.color }}>{meta.label}</span>
                          {svc.status !== "healing" && <span style={{ fontSize: 11.5, color: "#64748b" }}>· {svc.latency > 0 ? `${svc.latency}ms latency` : "probing"}</span>}
                        </div>
                      </div>
                    </div>
                    {(svc.status === "degraded" || svc.status === "failed") && (
                      <button onClick={() => healService(svc.id)} className="sso-btn sso-heal-btn" style={{ display: "flex", alignItems: "center", gap: 6, padding: "0.4rem 0.75rem", borderRadius: 8, fontSize: 12, backgroundColor: "rgba(139,92,246,0.18)", color: "#c4b5fd", border: "1px solid rgba(139,92,246,0.4)" }}>
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

          {/* RIGHT COLUMN: INFRASTRUCTURE TELEMETRY */}
          <GlassPanel className="sso-col-span-2" style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: 18 }}>
            <h2 style={{ fontSize: 14.5, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
              <Cpu size={16} color="#a5b4fc" /> Infrastructure Telemetry
            </h2>
            <Gauge icon={Cpu} label="CPU Utilization" value={telemetry.cpu} unit="%" accent={telemetry.cpu > 80 ? "#ef4444" : "#6366f1"} />
            <Gauge icon={MemoryStick} label="Memory Footprint" value={telemetry.mem} unit="%" accent={telemetry.mem > 80 ? "#ef4444" : "#8b5cf6"} />
            <Gauge icon={HardDrive} label="Disk I/O" value={telemetry.disk} unit="%" accent={telemetry.disk > 80 ? "#f59e0b" : "#22c55e"} />
            <Gauge icon={Wifi} label="Network Throughput" value={telemetry.net} unit=" MB/s" accent="#38bdf8" />
          </GlassPanel>
        </div>

        {/* 1, 2, 5. LIVE TRAFFIC WATCHDOG & RECHARTS DYNAMIC GRAPH */}
        <LiveTrafficChart 
            trafficHistory={trafficHistory} 
            defenseModeActive={defenseModeActive} 
            onSimulateSpike={handleSimulateSpike}
            onRealBurst={handleRealBurst}
            isSimulating={isSimulating}
        />

        {/* 2 & 5. ACTIVE DEFENSE IP QUARANTINE JAIL & ROGUE TRAFFIC RESPONSE */}
        <GlassPanel style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: 14 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 10 }}>
            <div>
              <h2 style={{ fontSize: 14.5, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
                <ShieldAlert size={16} color="#ef4444" /> Active Defense IP Quarantine Jail &amp; Rogue Client Response
              </h2>
              <p style={{ fontSize: 11.5, color: "#94a3b8", margin: "3px 0 0 0" }}>
                Autonomous Heuristic Quarantine &middot; Direct HTTP 429 Challenge Delivery &middot; Upstream Abuse Telemetry
              </p>
            </div>

            <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
              {emailSuccessMsg && (
                <span style={{ fontSize: 12, color: "#86efac", background: "rgba(34,197,94,0.15)", border: "1px solid rgba(34,197,94,0.3)", padding: "3px 10px", borderRadius: 8 }}>
                  {emailSuccessMsg}
                </span>
              )}
              <button
                onClick={() => setShowThreatMatrixModal(true)}
                className="sso-btn"
                title="View 3-Tier Heuristic Policy & Decision Engine"
                style={{
                  display: "flex", alignItems: "center", gap: 6, padding: "0.4rem 0.85rem",
                  borderRadius: 10, fontSize: 12, border: "1px solid rgba(56,189,248,0.35)",
                  background: "rgba(56,189,248,0.1)", color: "#7dd3fc"
                }}
              >
                <ShieldCheck size={13} color="#38bdf8" />
                <span>Threat Decision Matrix</span>
              </button>
            </div>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {jailedIps.length === 0 ? (
              <p style={{ fontSize: 13, color: "#64748b", margin: 0 }}>No rogue IPs quarantined. Active defense shield standing by.</p>
            ) : (
              jailedIps.slice(0, 5).map((entry) => {
                const isQuarantined = entry.status === "QUARANTINED";
                return (
                  <div
                    key={`${entry.ip}-${entry.jailed_at}`}
                    style={{
                      display: "flex", justifyContent: "space-between", alignItems: "center",
                      padding: "0.85rem 1rem", borderRadius: 10, flexWrap: "wrap", gap: 10,
                      background: isQuarantined ? "rgba(239, 68, 68, 0.08)" : "rgba(255,255,255,0.02)",
                      border: isQuarantined ? "1px solid rgba(239, 68, 68, 0.3)" : "1px solid rgba(255,255,255,0.05)"
                    }}
                  >
                    <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                      <span style={{
                        fontSize: 10.5, fontWeight: 700, padding: "3px 8px", borderRadius: 9999,
                        color: isQuarantined ? "#fca5a5" : "#86efac",
                        backgroundColor: isQuarantined ? "rgba(239,68,68,0.2)" : "rgba(34,197,94,0.15)"
                      }}>
                        {isQuarantined ? "QUARANTINED" : "RELEASED"}
                      </span>
                      <div>
                        <p style={{ margin: 0, fontSize: 13.5, fontWeight: 600, color: "#f8fafc", fontFamily: "monospace" }}>
                          {entry.ip} <span style={{ color: "#94a3b8", fontWeight: 400, fontSize: 11.5 }}>· {entry.incident_id}</span>
                        </p>
                        <p style={{ margin: "2px 0 0 0", fontSize: 11.5, color: "#94a3b8" }}>
                          {entry.reason} &middot; <strong style={{ color: "#fca5a5" }}>{entry.requests_blocked || 20} blocked (HTTP 429)</strong>
                        </p>
                      </div>
                    </div>

                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <button
                        onClick={async () => {
                          try {
                            const res = await axios.get(`${BACKEND_HTTP_URL}/api/security/inspect-challenge/${encodeURIComponent(entry.ip)}`);
                            setInspectingChallenge(res.data);
                          } catch (_) {
                            setInspectingChallenge({
                              http_status: 429,
                              error: "Active Defense: Rate Limit & Volumetric Threshold Exceeded",
                              client_ip: entry.ip,
                              threat_level: entry.threat_level || "CRITICAL",
                              action: "IP Quarantined in Active Defense Jail",
                              reason: entry.reason,
                              incident_id: entry.incident_id,
                              quarantine_expires: "12 seconds (Self-Healing Stabilization)",
                              remediation: "Traffic must stabilize below 4.0 req/s before automated unjailing.",
                              support_contact: "security@finsight.com"
                            });
                          }
                        }}
                        className="sso-btn"
                        title="View the direct HTTP 429 Challenge Payload delivered to this IP"
                        style={{
                          display: "flex", alignItems: "center", gap: 5, padding: "0.35rem 0.75rem",
                          borderRadius: 8, fontSize: 11.5, border: "1px solid rgba(139,92,246,0.35)",
                          background: "rgba(139,92,246,0.12)", color: "#c4b5fd"
                        }}
                      >
                        <FileCode size={12} color="#c4b5fd" />
                        <span>Inspect 429 Challenge</span>
                      </button>

                      <button
                        onClick={async () => {
                          setDispatchingEmail(true);
                          try {
                            const res = await axios.post(`${BACKEND_HTTP_URL}/api/security/dispatch-abuse-email`, {
                              ip: entry.ip,
                              incident_id: entry.incident_id,
                              recipient: "secops-incident-team@finsight.io"
                            });
                            setEmailSuccessMsg(`✅ SecOps abuse notice transmitted to ${res.data?.dispatched_to || 'SecOps team'}`);
                            setTimeout(() => setEmailSuccessMsg(""), 4000);
                          } catch (_) {
                            setEmailSuccessMsg("⚠️ SecOps alert logged to audit trail.");
                            setTimeout(() => setEmailSuccessMsg(""), 3500);
                          } finally {
                            setDispatchingEmail(false);
                          }
                        }}
                        disabled={dispatchingEmail}
                        className="sso-btn"
                        title="Dispatch automated SecOps Incident Notice to security team & ISP abuse desk"
                        style={{
                          display: "flex", alignItems: "center", gap: 5, padding: "0.35rem 0.75rem",
                          borderRadius: 8, fontSize: 11.5, border: "1px solid rgba(245,158,11,0.35)",
                          background: "rgba(245,158,11,0.12)", color: "#fde68a"
                        }}
                      >
                        {dispatchingEmail ? <Loader2 size={12} className="sso-spin" /> : <Mail size={12} color="#f59e0b" />}
                        <span>Dispatch SecOps Alert</span>
                      </button>

                      {isQuarantined && (
                        <button
                          onClick={async () => {
                            try {
                              await axios.post(`${BACKEND_HTTP_URL}/api/security/release-ip/${encodeURIComponent(entry.ip)}`);
                              setJailedIps((prev) => prev.map((j) => (j.ip === entry.ip ? { ...j, status: "RELEASED (Manual Override)", auto_release_in: "Released by SRE Engineer" } : j)));
                            } catch (e) {
                              console.error("Failed to release IP", e);
                            }
                          }}
                          className="sso-btn"
                          title="Manually release IP from Active Defense quarantine"
                          style={{
                            display: "flex", alignItems: "center", gap: 5, padding: "0.35rem 0.75rem",
                            borderRadius: 8, fontSize: 11.5, border: "1px solid rgba(34,197,94,0.35)",
                            background: "rgba(34,197,94,0.12)", color: "#86efac"
                          }}
                        >
                          <Unlock size={12} color="#86efac" />
                          <span>Release IP</span>
                        </button>
                      )}
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </GlassPanel>

        {/* 3. AIOPS AUDIT & SRE EXECUTION LOG (WITH MONGODB 30-DAY TTL PERSISTENCE) */}
        <GlassPanel style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: 12 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 8 }}>
            <h2 style={{ fontSize: 14.5, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
              <Terminal size={16} color="#a5b4fc" /> AIOps Execution Log &amp; Audit Trail
            </h2>
            <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
              <div
                title="Synchronized to Indian Standard Time (Mumbai, India)"
                style={{
                  display: "flex", alignItems: "center", gap: 5, fontSize: 11,
                  color: "#38bdf8", background: "rgba(56,189,248,0.1)",
                  border: "1px solid rgba(56,189,248,0.3)",
                  padding: "3px 10px", borderRadius: 9999
                }}
              >
                <Clock size={11} color="#38bdf8" />
                <span>IST (Mumbai)</span>
              </div>
              <div 
                onClick={() => { setShowMongoModal(true); fetchMongoStatus(); }}
                className="sso-btn"
                title="Click to view MongoDB Atlas 30-Day TTL Index configuration"
                style={{ 
                  display: "flex", alignItems: "center", gap: 5, fontSize: 11, 
                  color: mongoConnected ? "#86efac" : "#c4b5fd", 
                  background: mongoConnected ? "rgba(34,197,94,0.12)" : "rgba(139,92,246,0.12)", 
                  border: mongoConnected ? "1px solid rgba(34,197,94,0.3)" : "1px solid rgba(139,92,246,0.3)", 
                  padding: "3px 10px", borderRadius: 9999 
                }}
              >
                <Database size={12} color={mongoConnected ? "#86efac" : "#c4b5fd"} />
                <span>{mongoConnected ? "MongoDB Atlas · 30-Day TTL Auto-Purge" : "Audit Store: Ready for MongoDB"}</span>
              </div>
              <span style={{ fontSize: 11, color: "#64748b" }}>Live Polling: 2.0s</span>
            </div>
          </div>
          <div ref={terminalRef} className="sso-scroll" style={{ height: 260, overflowY: "auto", borderRadius: 12, background: "rgba(0,0,0,0.5)", border: "1px solid rgba(255,255,255,0.06)", padding: 16, fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace", fontSize: "12.5px", lineHeight: 1.65 }}>
            <AnimatePresence initial={false}>
              {logs.map((log) => (
                <motion.div key={log.id || `${log.time}-${log.msg}`} initial={{ opacity: 0, x: -6 }} animate={{ opacity: 1, x: 0 }} style={{ display: "flex", gap: 8 }}>
                  <span style={{ color: "#475569" }}>[{formatISTTime(log.time, log.createdAt)}]</span>
                  <span style={{ fontWeight: 600, color: LOG_LEVEL_META[log.level]?.color || "#ffffff" }}>[{log.level}]</span>
                  <span style={{ color: "#cbd5e1" }}>{log.msg}</span>
                </motion.div>
              ))}
            </AnimatePresence>
          </div>
        </GlassPanel>

        {/* 3. PERSISTENT INCIDENT INTELLIGENCE FEED */}
        <GlassPanel style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: 12 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 8 }}>
            <h2 style={{ fontSize: 14.5, fontWeight: 600, color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
              <AlertTriangle size={16} color="#a5b4fc" /> Incident Intelligence Feed
            </h2>
            <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: "#86efac", background: "rgba(34,197,94,0.1)", border: "1px solid rgba(34,197,94,0.25)", padding: "3px 10px", borderRadius: 9999 }}>
              <Clock size={11} color="#86efac" />
              <span>Auto-Purge: 30-Day TTL Lifecycle</span>
            </div>
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {incidents.length === 0 && <p style={{ fontSize: 13, color: "#64748b", margin: 0 }}>No incidents recorded yet. System operational.</p>}
            {incidents.slice().reverse().map((inc) => {
              const resolved = inc.status === "Resolved";
              const sevMeta = SEVERITY_META[inc.severity] || SEVERITY_META.WARNING;
              return (
                <button
                  key={inc.id}
                  onClick={() => setActiveIncident(inc)}
                  className="sso-btn sso-incident-row"
                  style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0.9rem 1rem", textAlign: "left", borderRadius: 10, border: "none", background: "rgba(255,255,255,0.025)", color: "#f1f5f9", opacity: resolved ? 0.6 : 1 }}
                >
                  <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                    <span style={{ fontSize: 10.5, fontWeight: 600, padding: "4px 8px", borderRadius: 9999, color: sevMeta.color, backgroundColor: sevMeta.bg }}>{inc.severity}</span>
                    {resolved && (
                      <span style={{ fontSize: 10.5, fontWeight: 600, padding: "4px 8px", borderRadius: 9999, color: "#86efac", backgroundColor: "rgba(34,197,94,0.15)", display: "flex", alignItems: "center", gap: 4 }}>
                        <CheckCircle2 size={12} /> REMEDIATED
                      </span>
                    )}
                    <div>
                      <p style={{ fontSize: 13.5, color: "#e2e8f0", margin: 0, fontWeight: 500 }}>{inc.title || inc.service}</p>
                      <p style={{ fontSize: 11, color: "#64748b", margin: 0 }}>{inc.service} · ID: {inc.id}</p>
                    </div>
                  </div>
                  <span style={{ fontSize: 11.5, color: "#64748b", fontFamily: "monospace" }}>{formatISTTime(inc.time, inc.createdAt)}</span>
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
            <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.75)", backdropFilter: "blur(6px)" }} />
            <motion.div initial={{ opacity: 0, scale: 0.94, y: 12 }} animate={{ opacity: 1, scale: 1, y: 0 }} exit={{ opacity: 0, scale: 0.96, y: 8 }} onClick={(e) => e.stopPropagation()} style={{ position: "relative", width: "100%", maxWidth: 540, borderRadius: 16, border: "1px solid rgba(139,92,246,0.3)", padding: 24, display: "flex", flexDirection: "column", gap: 20, background: "linear-gradient(160deg, #14113a, #0c0a1f)", boxShadow: "0 20px 50px rgba(0,0,0,0.6)" }}>
              <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 16 }}>
                <div>
                  <h3 style={{ fontSize: 18, fontWeight: 600, color: "#ffffff", margin: "0" }}>{activeIncident.title || activeIncident.service}</h3>
                  <p style={{ fontSize: 12, color: "#94a3b8", margin: "2px 0 0 0" }}>{activeIncident.service} · {formatISTTime(activeIncident.time, activeIncident.createdAt)} · {activeIncident.id}</p>
                </div>
                <button onClick={() => setActiveIncident(null)} className="sso-btn" style={{ padding: 6, borderRadius: 8, border: "none", background: "transparent", color: "#94a3b8", display: "flex" }}><X size={18} /></button>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 12, padding: 12, borderRadius: 12, border: "1px solid rgba(139,92,246,0.25)", background: "rgba(139,92,246,0.08)" }}>
                <Sparkles size={20} color="#c4b5fd" />
                <div>
                  <p style={{ fontSize: 11.5, color: "#94a3b8", margin: 0 }}>AIOps Prediction &amp; Confidence</p>
                  <p style={{ fontSize: 20, fontWeight: 700, color: "#ddd6fe", margin: 0 }}>{activeIncident.confidence || 99}% Autonomous Reliability</p>
                </div>
              </div>
              <div>
                <p style={{ fontSize: 11.5, textTransform: "uppercase", letterSpacing: "0.08em", color: "#94a3b8", margin: "0 0 6px 0", fontWeight: 600 }}>Identified Root Cause</p>
                <p style={{ fontSize: 13.5, color: "#cbd5e1", lineHeight: 1.6, margin: 0 }}>{activeIncident.rootCause}</p>
              </div>
              <div>
                <p style={{ fontSize: 11.5, textTransform: "uppercase", letterSpacing: "0.08em", color: "#94a3b8", margin: "0 0 6px 0", fontWeight: 600 }}>Recommended Remediation</p>
                <p style={{ fontSize: 13.5, color: "#cbd5e1", lineHeight: 1.6, margin: 0 }}>{activeIncident.remediation}</p>
              </div>
              <div style={{ display: "flex", gap: 12 }}>
                {activeIncident.status === "Resolved" ? (
                  <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 8, padding: "0.65rem 1rem", borderRadius: 12, fontSize: 14, color: "#86efac", backgroundColor: "rgba(34,197,94,0.15)", border: "1px solid rgba(34,197,94,0.3)" }}>
                    <CheckCircle2 size={16} /> Closed &amp; Remediated
                  </div>
                ) : (
                  <button
                    onClick={() => { healService(activeIncident.service_id); setActiveIncident(null); }}
                    className="sso-btn sso-execute-btn"
                    style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 8, padding: "0.65rem 1rem", borderRadius: 12, fontSize: 14, fontWeight: 600, color: "#ede9fe", backgroundColor: "rgba(139,92,246,0.25)", border: "1px solid rgba(139,92,246,0.5)" }}
                  >
                    <Zap size={16} /> Execute Auto-Heal
                  </button>
                )}
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* MONGODB ATLAS RETENTION & 30-DAY TTL MODAL */}
      <AnimatePresence>
        {showMongoModal && (
          <motion.div style={{ position: "fixed", inset: 0, zIndex: 50, display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onClick={() => setShowMongoModal(false)}>
            <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.75)", backdropFilter: "blur(6px)" }} />
            <motion.div initial={{ opacity: 0, scale: 0.94, y: 12 }} animate={{ opacity: 1, scale: 1, y: 0 }} exit={{ opacity: 0, scale: 0.96, y: 8 }} onClick={(e) => e.stopPropagation()} style={{ position: "relative", width: "100%", maxWidth: 560, borderRadius: 16, border: "1px solid rgba(34,197,94,0.35)", padding: 24, display: "flex", flexDirection: "column", gap: 18, background: "linear-gradient(160deg, #14113a, #0c0a1f)", boxShadow: "0 20px 50px rgba(0,0,0,0.6)" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                <h3 style={{ fontSize: 17, fontWeight: 600, color: "#ffffff", margin: 0, display: "flex", alignItems: "center", gap: 8 }}>
                  <Database size={18} color="#86efac" /> MongoDB Atlas Lifecycle &amp; 30-Day TTL Policy
                </h3>
                <button onClick={() => setShowMongoModal(false)} className="sso-btn" style={{ padding: 6, borderRadius: 8, border: "none", background: "transparent", color: "#94a3b8", display: "flex" }}><X size={18} /></button>
              </div>
              
              <p style={{ fontSize: 13, color: "#94a3b8", margin: 0, lineHeight: 1.5 }}>
                In compliance with DevOps and enterprise log retention standards, all execution logs and incident tickets are persisted to MongoDB Atlas and automatically purged after <strong>30 days (1 month)</strong> using native MongoDB TTL Indexes.
              </p>

              {/* Status Details Cards */}
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 10 }}>
                <div style={{ padding: "10px 14px", borderRadius: 10, background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.06)" }}>
                  <p style={{ fontSize: 10.5, color: "#94a3b8", margin: "0 0 4px 0", textTransform: "uppercase" }}>Cluster &amp; Database</p>
                  <p style={{ fontSize: 13.5, fontWeight: 600, color: "#f8fafc", margin: 0 }}>Cluster0 · sentinel_ops</p>
                </div>
                <div style={{ padding: "10px 14px", borderRadius: 10, background: "rgba(34,197,94,0.08)", border: "1px solid rgba(34,197,94,0.25)" }}>
                  <p style={{ fontSize: 10.5, color: "#86efac", margin: "0 0 4px 0", textTransform: "uppercase" }}>Auto-Purge Policy</p>
                  <p style={{ fontSize: 13.5, fontWeight: 600, color: "#86efac", margin: 0 }}>30 Days (2,592,000s TTL)</p>
                </div>
                <div style={{ padding: "10px 14px", borderRadius: 10, background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.06)" }}>
                  <p style={{ fontSize: 10.5, color: "#94a3b8", margin: "0 0 4px 0", textTransform: "uppercase" }}>Persisted Collections</p>
                  <p style={{ fontSize: 13, color: "#cbd5e1", margin: 0 }}>system_logs, incidents</p>
                </div>
                <div style={{ padding: "10px 14px", borderRadius: 10, background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.06)" }}>
                  <p style={{ fontSize: 10.5, color: "#94a3b8", margin: "0 0 4px 0", textTransform: "uppercase" }}>Index Mechanism</p>
                  <p style={{ fontSize: 13, color: "#cbd5e1", margin: 0 }}>BSON createdAt + TTL Worker</p>
                </div>
              </div>

              {/* Connection string input */}
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                <label style={{ fontSize: 12, color: "#cbd5e1", fontWeight: 500 }}>MongoDB Atlas Connection URI</label>
                <input
                  type="password"
                  placeholder="mongodb+srv://<username>:<password>@cluster0.../sentinel_ops"
                  value={mongoUriInput}
                  onChange={(e) => setMongoUriInput(e.target.value)}
                  style={{
                    padding: "0.65rem 0.9rem",
                    borderRadius: 10,
                    border: "1px solid rgba(255,255,255,0.12)",
                    background: "rgba(0,0,0,0.4)",
                    color: "#f8fafc",
                    fontSize: 13,
                    fontFamily: "monospace",
                    outline: "none"
                  }}
                />
              </div>

              {mongoStatusMsg && (
                <p style={{ fontSize: 12.5, color: mongoStatusMsg.startsWith("✅") ? "#86efac" : "#fbbf24", margin: 0 }}>
                  {mongoStatusMsg}
                </p>
              )}

              <div style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}>
                <button
                  onClick={() => setShowMongoModal(false)}
                  className="sso-btn"
                  style={{ padding: "0.55rem 1rem", borderRadius: 10, border: "1px solid rgba(255,255,255,0.1)", background: "transparent", color: "#94a3b8", fontSize: 13 }}
                >
                  Close
                </button>
                <button
                  onClick={handleSaveMongoUri}
                  disabled={savingMongo}
                  className="sso-btn"
                  style={{ padding: "0.55rem 1.2rem", borderRadius: 10, border: "1px solid rgba(34,197,94,0.5)", background: "rgba(34,197,94,0.22)", color: "#86efac", fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center", gap: 6 }}
                >
                  {savingMongo ? <Loader2 size={13} className="sso-spin" /> : <Database size={13} />}
                  Connect &amp; Verify TTL
                </button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* 4. DISCORD WEBHOOK MODAL */}
      <AnimatePresence>
        {showDiscordModal && (
          <motion.div style={{ position: "fixed", inset: 0, zIndex: 50, display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onClick={() => setShowDiscordModal(false)}>
            <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.75)", backdropFilter: "blur(6px)" }} />
            <motion.div initial={{ opacity: 0, scale: 0.94, y: 12 }} animate={{ opacity: 1, scale: 1, y: 0 }} exit={{ opacity: 0, scale: 0.96, y: 8 }} onClick={(e) => e.stopPropagation()} style={{ position: "relative", width: "100%", maxWidth: 540, borderRadius: 16, border: "1px solid rgba(99,102,241,0.35)", padding: 24, display: "flex", flexDirection: "column", gap: 18, background: "linear-gradient(160deg, #14113a, #0c0a1f)", boxShadow: "0 20px 50px rgba(0,0,0,0.6)" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                <h3 style={{ fontSize: 17, fontWeight: 600, color: "#ffffff", margin: 0, display: "flex", alignItems: "center", gap: 8 }}>
                  <BellRing size={18} color="#a5b4fc" /> Discord Operations Webhook Integration
                </h3>
                <button onClick={() => setShowDiscordModal(false)} className="sso-btn" style={{ padding: 6, borderRadius: 8, border: "none", background: "transparent", color: "#94a3b8", display: "flex" }}><X size={18} /></button>
              </div>
              <p style={{ fontSize: 13, color: "#94a3b8", margin: 0, lineHeight: 1.5 }}>
                When a traffic surge or health anomaly occurs, Sentinel formats rich embedded alerts with incident ID, RPS, and operational status, and POSTs directly to your Discord channel.
              </p>
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                <label style={{ fontSize: 12, color: "#cbd5e1", fontWeight: 500 }}>Discord Webhook URL</label>
                <input
                  type="text"
                  placeholder="https://discord.com/api/webhooks/..."
                  value={discordWebhookInput}
                  onChange={(e) => setDiscordWebhookInput(e.target.value)}
                  style={{
                    padding: "0.65rem 0.9rem",
                    borderRadius: 10,
                    border: "1px solid rgba(255,255,255,0.12)",
                    background: "rgba(0,0,0,0.4)",
                    color: "#f8fafc",
                    fontSize: 13,
                    fontFamily: "monospace",
                    outline: "none"
                  }}
                />
              </div>
              {discordStatusMsg && (
                <p style={{ fontSize: 12.5, color: discordStatusMsg.startsWith("✅") ? "#86efac" : "#fbbf24", margin: 0 }}>
                  {discordStatusMsg}
                </p>
              )}
              <div style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}>
                <button
                  onClick={() => setShowDiscordModal(false)}
                  className="sso-btn"
                  style={{ padding: "0.55rem 1rem", borderRadius: 10, border: "1px solid rgba(255,255,255,0.1)", background: "transparent", color: "#94a3b8", fontSize: 13 }}
                >
                  Cancel
                </button>
                <button
                  onClick={handleSaveDiscordWebhook}
                  className="sso-btn"
                  style={{ padding: "0.55rem 1.2rem", borderRadius: 10, border: "1px solid rgba(99,102,241,0.5)", background: "rgba(99,102,241,0.25)", color: "#e0e7ff", fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center", gap: 6 }}
                >
                  <Send size={13} /> Save &amp; Test Webhook
                </button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* 6. SENTINEL CONFIG BLUEPRINT MODAL */}
      <AnimatePresence>
        {showConfigModal && (
          <motion.div style={{ position: "fixed", inset: 0, zIndex: 50, display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onClick={() => setShowConfigModal(false)}>
            <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.75)", backdropFilter: "blur(6px)" }} />
            <motion.div initial={{ opacity: 0, scale: 0.94, y: 12 }} animate={{ opacity: 1, scale: 1, y: 0 }} exit={{ opacity: 0, scale: 0.96, y: 8 }} onClick={(e) => e.stopPropagation()} style={{ position: "relative", width: "100%", maxWidth: 640, borderRadius: 16, border: "1px solid rgba(139,92,246,0.35)", padding: 24, display: "flex", flexDirection: "column", gap: 16, background: "linear-gradient(160deg, #14113a, #0c0a1f)", boxShadow: "0 20px 50px rgba(0,0,0,0.6)" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                <h3 style={{ fontSize: 17, fontWeight: 600, color: "#ffffff", margin: 0, display: "flex", alignItems: "center", gap: 8 }}>
                  <FileCode size={18} color="#a5b4fc" /> .sentinel-config.yml Quality Gate Blueprint
                </h3>
                <button onClick={() => setShowConfigModal(false)} className="sso-btn" style={{ padding: 6, borderRadius: 8, border: "none", background: "transparent", color: "#94a3b8", display: "flex" }}><X size={18} /></button>
              </div>
              <p style={{ fontSize: 12.5, color: "#94a3b8", margin: 0 }}>
                This policy blueprint enforces pre-flight quality gates before any commit is authorized to deploy into production on Render.
              </p>
              <div className="sso-scroll" style={{ maxHeight: 380, overflowY: "auto", background: "rgba(0,0,0,0.5)", borderRadius: 12, padding: 16, border: "1px solid rgba(255,255,255,0.08)", fontFamily: "monospace", fontSize: 12, color: "#cbd5e1", whiteSpace: "pre-wrap", lineHeight: 1.6 }}>
                {sentinelConfigContent}
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
      {/* 7. THREAT DECISION MATRIX & HEURISTIC ENGINE MODAL */}
      <AnimatePresence>
        {showThreatMatrixModal && (
          <motion.div style={{ position: "fixed", inset: 0, zIndex: 50, display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onClick={() => setShowThreatMatrixModal(false)}>
            <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.75)", backdropFilter: "blur(6px)" }} />
            <motion.div initial={{ opacity: 0, scale: 0.94, y: 12 }} animate={{ opacity: 1, scale: 1, y: 0 }} exit={{ opacity: 0, scale: 0.96, y: 8 }} onClick={(e) => e.stopPropagation()} style={{ position: "relative", width: "100%", maxWidth: 660, borderRadius: 16, border: "1px solid rgba(56,189,248,0.35)", padding: 24, display: "flex", flexDirection: "column", gap: 16, background: "linear-gradient(160deg, #0e1726, #090e17)", boxShadow: "0 20px 50px rgba(0,0,0,0.7)" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                <h3 style={{ fontSize: 17, fontWeight: 600, color: "#ffffff", margin: 0, display: "flex", alignItems: "center", gap: 8 }}>
                  <ShieldCheck size={18} color="#38bdf8" /> Active Defense Threat Matrix &amp; Decision Engine
                </h3>
                <button onClick={() => setShowThreatMatrixModal(false)} className="sso-btn" style={{ padding: 6, borderRadius: 8, border: "none", background: "transparent", color: "#94a3b8", display: "flex" }}><X size={18} /></button>
              </div>

              <div style={{ padding: 12, borderRadius: 10, background: "rgba(56,189,248,0.08)", border: "1px solid rgba(56,189,248,0.2)" }}>
                <p style={{ fontSize: 12, color: "#bae6fd", margin: 0, lineHeight: 1.5 }}>
                  <strong>DevOps &amp; Cybersecurity Architecture:</strong> Raw Layer-3 IP addresses do not have email inboxes attached to them. Instead of attempting to email a bot or spoofed IP, our platform delivers <strong>Direct HTTP 429 Security Challenges</strong> back to the attacker's client terminal, isolates the IP in our <strong>Dynamic IP Quarantine Jail</strong>, and broadcasts incident reports to the <strong>SecOps Response Team &amp; ISP Abuse Desks</strong>.
                </p>
              </div>

              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                <div style={{ display: "grid", gridTemplateColumns: "130px 110px 1fr", gap: 10, padding: "8px 12px", background: "rgba(255,255,255,0.04)", borderRadius: 8, fontSize: 11, fontWeight: 700, color: "#94a3b8", textTransform: "uppercase" }}>
                  <span>Tier / Threat Level</span>
                  <span>Traffic Limit</span>
                  <span>Autonomous Action &amp; Rationale</span>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "130px 110px 1fr", gap: 10, padding: "10px 12px", background: "rgba(34,197,94,0.05)", border: "1px solid rgba(34,197,94,0.2)", borderRadius: 8, fontSize: 12.5, color: "#e2e8f0", alignItems: "center" }}>
                  <span style={{ fontWeight: 600, color: "#86efac" }}>Tier 1: Normal</span>
                  <span style={{ fontFamily: "monospace", color: "#94a3b8" }}>0 - 8 req/s</span>
                  <span><strong>ALLOW (HTTP 200):</strong> Legitimate client browsing &amp; metrics telemetry probes.</span>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "130px 110px 1fr", gap: 10, padding: "10px 12px", background: "rgba(245,158,11,0.05)", border: "1px solid rgba(245,158,11,0.2)", borderRadius: 8, fontSize: 12.5, color: "#e2e8f0", alignItems: "center" }}>
                  <span style={{ fontWeight: 600, color: "#fcd34d" }}>Tier 2: Suspicious</span>
                  <span style={{ fontFamily: "monospace", color: "#94a3b8" }}>8 - 15 req/s</span>
                  <span><strong>SOFT-THROTTLE (HTTP 429):</strong> Suppresses transient spikes without permanently banning innocent users.</span>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "130px 110px 1fr", gap: 10, padding: "10px 12px", background: "rgba(239,68,68,0.08)", border: "1px solid rgba(239,68,68,0.25)", borderRadius: 8, fontSize: 12.5, color: "#e2e8f0", alignItems: "center" }}>
                  <span style={{ fontWeight: 600, color: "#fca5a5" }}>Tier 3: Rogue Attack</span>
                  <span style={{ fontFamily: "monospace", color: "#fca5a5" }}>&gt; 20 reqs / 10s</span>
                  <span><strong>DYNAMIC IP JAIL &amp; QUARANTINE:</strong> Rogue IP isolated, HTTP 429 Challenge delivered, SecOps alert dispatched.</span>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "130px 110px 1fr", gap: 10, padding: "10px 12px", background: "rgba(139,92,246,0.06)", border: "1px solid rgba(139,92,246,0.25)", borderRadius: 8, fontSize: 12.5, color: "#e2e8f0", alignItems: "center" }}>
                  <span style={{ fontWeight: 600, color: "#c4b5fd" }}>Tier 4: Recovery</span>
                  <span style={{ fontFamily: "monospace", color: "#94a3b8" }}>&lt; 4 req/s (12s)</span>
                  <span><strong>AUTONOMOUS SRE UNJAIL:</strong> Closed-loop self-healing disengages defense shield and resolves incident.</span>
                </div>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* 8. HTTP 429 CHALLENGE INSPECTOR MODAL */}
      <AnimatePresence>
        {inspectingChallenge && (
          <motion.div style={{ position: "fixed", inset: 0, zIndex: 50, display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onClick={() => setInspectingChallenge(null)}>
            <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.75)", backdropFilter: "blur(6px)" }} />
            <motion.div initial={{ opacity: 0, scale: 0.94, y: 12 }} animate={{ opacity: 1, scale: 1, y: 0 }} exit={{ opacity: 0, scale: 0.96, y: 8 }} onClick={(e) => e.stopPropagation()} style={{ position: "relative", width: "100%", maxWidth: 560, borderRadius: 16, border: "1px solid rgba(239,68,68,0.4)", padding: 24, display: "flex", flexDirection: "column", gap: 16, background: "linear-gradient(160deg, #180d19, #0c0812)", boxShadow: "0 20px 50px rgba(0,0,0,0.7)" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                <div>
                  <h3 style={{ fontSize: 17, fontWeight: 600, color: "#fca5a5", margin: 0, display: "flex", alignItems: "center", gap: 8 }}>
                    <ShieldAlert size={18} color="#ef4444" /> HTTP 429 Challenge Payload Delivered to Attacker
                  </h3>
                  <p style={{ fontSize: 11.5, color: "#94a3b8", margin: "2px 0 0 0" }}>Target Client IP: <strong style={{ color: "#ffffff", fontFamily: "monospace" }}>{inspectingChallenge.client_ip}</strong></p>
                </div>
                <button onClick={() => setInspectingChallenge(null)} className="sso-btn" style={{ padding: 6, borderRadius: 8, border: "none", background: "transparent", color: "#94a3b8", display: "flex" }}><X size={18} /></button>
              </div>

              <p style={{ fontSize: 12.5, color: "#cbd5e1", margin: 0 }}>
                This is the exact JSON response intercepted by Active Defense and returned directly to the attacking client terminal when they attempt a volumetric attack:
              </p>

              <div className="sso-scroll" style={{ maxHeight: 320, overflowY: "auto", background: "rgba(0,0,0,0.6)", borderRadius: 12, padding: 16, border: "1px solid rgba(239,68,68,0.25)", fontFamily: "monospace", fontSize: 12, color: "#86efac", whiteSpace: "pre-wrap", lineHeight: 1.6 }}>
                {JSON.stringify(inspectingChallenge, null, 2)}
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}