import React from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { 
    Shield, 
    ShieldAlert, 
    Activity, 
    Loader2, 
    Database, 
    Cpu, 
    Crosshair, 
    AlertTriangle, 
    CheckCircle2, 
    Radio, 
    Laptop, 
    Smartphone 
} from 'lucide-react';

// Custom Tooltip for Graph 1: Production Platform Traffic Load
function PlatformTooltip({ active, payload, label }) {
    if (active && payload && payload.length) {
        const data = payload[0].payload;
        const val = data.platform_rps !== undefined ? data.platform_rps : (data.rps || 0);
        return (
            <div style={{
                backgroundColor: '#0f0d22',
                border: '1px solid rgba(56, 189, 248, 0.4)',
                borderRadius: '10px',
                padding: '10px 14px',
                boxShadow: '0 8px 24px rgba(0,0,0,0.6)',
                fontSize: 12
            }}>
                <div style={{ color: '#94a3b8', marginBottom: 4, fontWeight: 500 }}>{label} (IST)</div>
                <div style={{ color: '#38bdf8', fontWeight: 700, fontSize: 14 }}>
                    Platform Load: {Number(val).toFixed(2)} req/s
                </div>
                <div style={{ color: '#64748b', fontSize: 11, marginTop: 4 }}>
                    Latency: {data.latency || 42}ms &middot; DB: {data.db_status || 'CONNECTED'} &middot; Heap: {data.heap_used || 20.1}MB
                </div>
            </div>
        );
    }
    return null;
}

// Custom Tooltip for Graph 2: Threat Velocity Matrix & Hacker Tracker
function ThreatTooltip({ active, payload, label }) {
    if (active && payload && payload.length) {
        const data = payload[0].payload;
        const val = data.threat_rps !== undefined ? data.threat_rps : 0;
        const isSpike = val >= 2.0;
        const targetIp = data.attacker_local_ip || (data.attacker_ip && data.attacker_ip !== 'None' ? data.attacker_ip : '');
        const devName = data.attacker_device_name || '';

        return (
            <div style={{
                backgroundColor: '#0f0d22',
                border: isSpike ? '1px solid rgba(239, 68, 68, 0.6)' : '1px solid rgba(34, 197, 94, 0.35)',
                borderRadius: '10px',
                padding: '10px 14px',
                boxShadow: '0 8px 24px rgba(0,0,0,0.6)',
                fontSize: 12
            }}>
                <div style={{ color: '#94a3b8', marginBottom: 4, fontWeight: 500 }}>{label} (IST)</div>
                <div style={{ color: isSpike ? '#ef4444' : '#22c55e', fontWeight: 700, fontSize: 14 }}>
                    Attacker Velocity: {Number(val).toFixed(2)} req/s
                </div>
                {targetIp && (
                    <div style={{ color: '#fca5a5', fontSize: 11, marginTop: 4, display: 'flex', alignItems: 'center', gap: 5 }}>
                        <Crosshair size={12} color="#ef4444" />
                        Target: {targetIp} {devName ? `(${devName})` : ''}
                    </div>
                )}
                <div style={{ color: isSpike ? '#fca5a5' : '#86efac', fontSize: 11, marginTop: 4 }}>
                    Status: {isSpike ? '🚨 Spiking Over 2.0 req/s Limit (Quarantining)' : '🟢 Flatline 0.0 req/s (Peaceful)'}
                </div>
            </div>
        );
    }
    return null;
}

export default function LiveTrafficChart({
    trafficHistory = [],
    defenseModeActive = false
}) {
    const SPIKE_LIMIT = 6.0;
    const latestData = trafficHistory.length > 0 ? trafficHistory[trafficHistory.length - 1] : {};

    const currentPlatformRps = latestData.platform_rps !== undefined ? latestData.platform_rps : (latestData.rps || 0);
    const currentThreatRps = latestData.threat_rps !== undefined ? latestData.threat_rps : 0;
    const currentLatency = latestData.latency || 42;
    const dbStatus = latestData.db_status || 'CONNECTED';
    const heapUsed = latestData.heap_used || 20.1;

    // Determine recent attack presence
    const recentMaxThreat = trafficHistory.slice(-5).reduce((max, pt) => Math.max(max, pt.threat_rps || 0), 0);
    const isUnderAttack = defenseModeActive || currentThreatRps >= SPIKE_LIMIT || recentMaxThreat >= SPIKE_LIMIT;

    // Detect attacker device details
    const attackerIp = latestData.attacker_local_ip || (latestData.attacker_ip && latestData.attacker_ip !== 'None' ? latestData.attacker_ip : '');
    const attackerDevice = latestData.attacker_device_name || '';

    return (
        <div style={{
            borderRadius: '1rem',
            border: isUnderAttack 
                ? '1px solid rgba(239, 68, 68, 0.55)' 
                : '1px solid var(--border-glow, rgba(99,102,241,0.25))',
            background: isUnderAttack 
                ? 'radial-gradient(ellipse at top, rgba(239, 68, 68, 0.08) 0%, rgba(15, 13, 34, 0.95) 70%)' 
                : 'linear-gradient(160deg, rgba(20, 17, 58, 0.7) 0%, rgba(12, 10, 31, 0.85) 100%)',
            backdropFilter: 'blur(16px)',
            boxShadow: isUnderAttack ? '0 0 32px rgba(239, 68, 68, 0.2)' : '0 8px 32px rgba(0, 0, 0, 0.3)',
            padding: '1.25rem',
            display: 'flex',
            flexDirection: 'column',
            gap: '1.25rem',
            transition: 'all 0.4s ease'
        }}>
            {/* Master Header & Live Controls */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 12 }}>
                <div>
                    <h2 style={{ fontSize: 16, fontWeight: 700, color: '#f8fafc', display: 'flex', alignItems: 'center', gap: 8, margin: 0 }}>
                        <Activity size={18} color="#38bdf8" />
                        Dual-Channel Telemetry &amp; Hacker Threat Watchdog
                    </h2>
                    <p style={{ fontSize: 11.5, color: '#94a3b8', margin: '3px 0 0 0' }}>
                        Decoupled Architecture &middot; Production Site Load vs Isolated Attacker Spike Matrix
                    </p>
                </div>
                
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                    {/* Defense Mode Badge */}
                    <div style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 6,
                        padding: '0.4rem 0.85rem',
                        borderRadius: 9999,
                        border: isUnderAttack ? '1px solid rgba(239, 68, 68, 0.6)' : '1px solid rgba(34, 197, 94, 0.35)',
                        background: isUnderAttack ? 'rgba(239, 68, 68, 0.18)' : 'rgba(34, 197, 94, 0.1)',
                        fontSize: 11.5,
                        fontWeight: 600,
                        color: isUnderAttack ? '#fca5a5' : '#86efac'
                    }}>
                        {isUnderAttack ? <ShieldAlert size={14} /> : <Shield size={14} />}
                        {isUnderAttack ? 'Active Defense Engaged' : 'Dual Watchdogs Active'}
                    </div>
                </div>
            </div>

            {/* DUAL GRAPHS GRID: Side-by-Side or Stacked on smaller displays */}
            <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(420px, 1fr))',
                gap: '1.25rem',
                width: '100%'
            }}>
                {/* ======================================================== */}
                {/* GRAPH 1: PRODUCTION PLATFORM TRAFFIC (WEBSITE LOAD)      */}
                {/* ======================================================== */}
                <div style={{
                    borderRadius: '0.85rem',
                    border: '1px solid rgba(56, 189, 248, 0.25)',
                    background: 'linear-gradient(170deg, rgba(14, 25, 48, 0.6) 0%, rgba(10, 15, 30, 0.85) 100%)',
                    padding: '1rem',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '0.75rem',
                    minWidth: 0 // Prevents recharts responsive overflow
                }}>
                    {/* Header: Platform Traffic */}
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 8 }}>
                        <div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                                <Radio size={14} color="#38bdf8" />
                                <span style={{ fontSize: 13.5, fontWeight: 600, color: '#f1f5f9' }}>
                                    Production Platform Traffic Load
                                </span>
                            </div>
                            <p style={{ fontSize: 11, color: '#94a3b8', margin: '2px 0 0 0' }}>
                                Total aggregate requests across all users &middot; FinSight Gateway
                            </p>
                        </div>

                        <span style={{
                            fontSize: 10.5,
                            fontWeight: 600,
                            padding: '2px 8px',
                            borderRadius: 6,
                            background: 'rgba(56, 189, 248, 0.12)',
                            color: '#38bdf8',
                            border: '1px solid rgba(56, 189, 248, 0.25)'
                        }}>
                            Site Traffic (RPS)
                        </span>
                    </div>

                    {/* Stats & Current Value */}
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', flexWrap: 'wrap', gap: 8, borderBottom: '1px solid rgba(255,255,255,0.05)', paddingBottom: 6 }}>
                        <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
                            <span style={{ fontSize: 26, fontWeight: 700, color: '#38bdf8', letterSpacing: '-0.02em', fontFamily: 'monospace' }}>
                                {Number(currentPlatformRps).toFixed(1)}
                            </span>
                            <span style={{ fontSize: 11.5, color: '#94a3b8' }}>req/s (Total Load)</span>
                        </div>

                        <div style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 11, color: '#94a3b8' }}>
                            <span>Latency: <strong style={{ color: '#e2e8f0', fontFamily: 'monospace' }}>{currentLatency}ms</strong></span>
                            <span>DB: <strong style={{ color: dbStatus === 'CONNECTED' ? '#86efac' : '#f87171', fontFamily: 'monospace' }}>{dbStatus}</strong></span>
                            <span>Heap: <strong style={{ color: '#e2e8f0', fontFamily: 'monospace' }}>{heapUsed}MB</strong></span>
                        </div>
                    </div>

                    {/* Recharts Area for Graph 1 */}
                    <div style={{ width: '100%', height: 190 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <AreaChart data={trafficHistory} margin={{ top: 10, right: 10, left: -25, bottom: 0 }}>
                                <defs>
                                    <linearGradient id="colorPlatform" x1="0" y1="0" x2="0" y2="1">
                                        <stop offset="5%" stopColor="#38bdf8" stopOpacity={0.45} />
                                        <stop offset="95%" stopColor="#38bdf8" stopOpacity={0} />
                                    </linearGradient>
                                </defs>
                                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" vertical={false} />
                                <XAxis 
                                    dataKey="time" 
                                    stroke="#64748b" 
                                    fontSize={10} 
                                    tickMargin={6} 
                                    minTickGap={20} 
                                />
                                <YAxis 
                                    stroke="#64748b" 
                                    fontSize={10} 
                                    domain={[0, (dataMax) => Math.max(6, Math.ceil(dataMax + 2))]} 
                                />
                                <Tooltip content={<PlatformTooltip />} />
                                <Area 
                                    type="monotone" 
                                    dataKey="platform_rps" 
                                    stroke="#38bdf8" 
                                    strokeWidth={2.2} 
                                    fillOpacity={1} 
                                    fill="url(#colorPlatform)" 
                                    isAnimationActive={false} 
                                />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                </div>

                {/* ======================================================== */}
                {/* GRAPH 2: HACKER TRACKER & THREAT VELOCITY MATRIX         */}
                {/* ======================================================== */}
                <div style={{
                    borderRadius: '0.85rem',
                    border: isUnderAttack 
                        ? '1px solid rgba(239, 68, 68, 0.6)' 
                        : '1px solid rgba(34, 197, 94, 0.25)',
                    background: isUnderAttack 
                        ? 'linear-gradient(170deg, rgba(38, 14, 20, 0.6) 0%, rgba(20, 10, 15, 0.85) 100%)' 
                        : 'linear-gradient(170deg, rgba(14, 34, 24, 0.6) 0%, rgba(10, 20, 15, 0.85) 100%)',
                    padding: '1rem',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '0.75rem',
                    minWidth: 0 // Prevents recharts responsive overflow
                }}>
                    {/* Header: Hacker Threat Tracker */}
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 8 }}>
                        <div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                                <Crosshair size={14} color={isUnderAttack ? '#ef4444' : '#22c55e'} />
                                <span style={{ fontSize: 13.5, fontWeight: 600, color: '#f1f5f9' }}>
                                    Threat Velocity Matrix (Hacker Tracker)
                                </span>
                            </div>
                            <p style={{ fontSize: 11, color: '#94a3b8', margin: '2px 0 0 0' }}>
                                Individual rogue terminal velocity &middot; Flatline at 0.0 req/s when peaceful
                            </p>
                        </div>

                        <span style={{
                            fontSize: 10.5,
                            fontWeight: 600,
                            padding: '2px 8px',
                            borderRadius: 6,
                            background: isUnderAttack ? 'rgba(239, 68, 68, 0.18)' : 'rgba(34, 197, 94, 0.12)',
                            color: isUnderAttack ? '#fca5a5' : '#86efac',
                            border: isUnderAttack ? '1px solid rgba(239, 68, 68, 0.35)' : '1px solid rgba(34, 197, 94, 0.25)'
                        }}>
                            {isUnderAttack ? '🚨 Rogue Surge' : '🟢 Flatline 0.0 RPS'}
                        </span>
                    </div>

                    {/* Stats & Current Attacker Details */}
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', flexWrap: 'wrap', gap: 8, borderBottom: '1px solid rgba(255,255,255,0.05)', paddingBottom: 6 }}>
                        <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
                            <span style={{ 
                                fontSize: 26, 
                                fontWeight: 700, 
                                color: currentThreatRps >= SPIKE_LIMIT ? '#ef4444' : (currentThreatRps > 0 ? '#f59e0b' : '#22c55e'), 
                                letterSpacing: '-0.02em', 
                                fontFamily: 'monospace' 
                            }}>
                                {Number(currentThreatRps).toFixed(1)}
                            </span>
                            <span style={{ fontSize: 11.5, color: '#94a3b8' }}>req/s (Attacker Velocity)</span>
                        </div>

                        {attackerIp ? (
                            <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: '#fca5a5' }}>
                                {attackerDevice.toLowerCase().includes('mobile') ? (
                                    <Smartphone size={12} color="#fca5a5" />
                                ) : (
                                    <Laptop size={12} color="#fca5a5" />
                                )}
                                <span>Target: <strong style={{ fontFamily: 'monospace' }}>{attackerIp}</strong> {attackerDevice ? `(${attackerDevice})` : ''}</span>
                            </div>
                        ) : (
                            <div style={{ fontSize: 11, color: '#86efac' }}>
                                <span>Baseline: <strong style={{ fontFamily: 'monospace' }}>0.0 req/s</strong> (Zero Active Threats)</span>
                            </div>
                        )}
                    </div>

                    {/* Recharts Area for Graph 2 */}
                    <div style={{ width: '100%', height: 190 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <AreaChart data={trafficHistory} margin={{ top: 10, right: 10, left: -25, bottom: 0 }}>
                                <defs>
                                    <linearGradient id="colorThreatAlert" x1="0" y1="0" x2="0" y2="1">
                                        <stop offset="5%" stopColor="#ef4444" stopOpacity={0.6} />
                                        <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
                                    </linearGradient>
                                    <linearGradient id="colorThreatNormal" x1="0" y1="0" x2="0" y2="1">
                                        <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
                                        <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
                                    </linearGradient>
                                </defs>
                                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" vertical={false} />
                                <XAxis 
                                    dataKey="time" 
                                    stroke="#64748b" 
                                    fontSize={10} 
                                    tickMargin={6} 
                                    minTickGap={20} 
                                />
                                <YAxis 
                                    stroke="#64748b" 
                                    fontSize={10} 
                                    domain={[0, (dataMax) => Math.max(8, Math.ceil(dataMax + 2))]} 
                                />
                                <Tooltip content={<ThreatTooltip />} />
                                <ReferenceLine 
                                    y={SPIKE_LIMIT} 
                                    stroke="#ef4444" 
                                    strokeDasharray="4 4" 
                                    strokeWidth={1.5}
                                    label={{ 
                                        position: 'insideTopLeft', 
                                        value: `Spike Threshold (${SPIKE_LIMIT} req/s)`, 
                                        fill: '#ef4444', 
                                        fontSize: 10,
                                        fontWeight: 600 
                                    }} 
                                />
                                <Area 
                                    type="monotone" 
                                    dataKey="threat_rps" 
                                    stroke={isUnderAttack ? '#ef4444' : '#22c55e'} 
                                    strokeWidth={2.2} 
                                    fillOpacity={1} 
                                    fill={isUnderAttack ? 'url(#colorThreatAlert)' : 'url(#colorThreatNormal)'} 
                                    isAnimationActive={false} 
                                />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>

            {/* Architecture Explanatory Ribbon */}
            <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                flexWrap: 'wrap',
                gap: 10,
                padding: '0.65rem 1rem',
                borderRadius: '0.65rem',
                background: 'rgba(255,255,255,0.03)',
                border: '1px solid rgba(255,255,255,0.06)',
                fontSize: 11.5,
                color: '#94a3b8'
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <Shield size={14} color="#38bdf8" />
                    <span>
                        <strong style={{ color: '#e2e8f0' }}>Dual-Telemetry Architecture:</strong> 50+ legitimate visitors only affect Graph 1 (no alarms). Rogue spamming only triggers Graph 2, isolating and quarantining <span style={{ color: '#fca5a5', fontWeight: 600 }}>that exact rogue device only</span> when exceeding {SPIKE_LIMIT} req/s.
                    </span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, color: '#38bdf8' }}>
                    <span>Active Defense IP Quarantine Jail &darr;</span>
                </div>
            </div>
        </div>
    );
}