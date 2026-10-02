import React from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { Shield, ShieldAlert, Activity, Zap, Flame, Loader2, Database, Cpu } from 'lucide-react';

export default function LiveTrafficChart({
    trafficHistory = [],
    defenseModeActive = false,
    onSimulateSpike = null,
    onRealBurst = null,
    isSimulating = false
}) {
    const latestData = trafficHistory.length > 0 ? trafficHistory[trafficHistory.length - 1] : {};
    const currentRps = latestData.rps !== undefined ? latestData.rps : 0;
    const currentLatency = latestData.latency || 42;
    const dbStatus = latestData.db_status || 'CONNECTED';
    const heapUsed = latestData.heap_used || 20.1;
    
    const themeColor = defenseModeActive ? '#ef4444' : '#38bdf8';
    const gradientId = defenseModeActive ? 'colorAlert' : 'colorNormal';
    const SPIKE_LIMIT = 8.0;

    return (
        <div style={{
            borderRadius: '1rem',
            border: defenseModeActive ? '1px solid rgba(239, 68, 68, 0.55)' : '1px solid var(--border-glow, rgba(99,102,241,0.25))',
            background: defenseModeActive 
                ? 'radial-gradient(ellipse at top left, rgba(239, 68, 68, 0.12) 0%, rgba(15, 13, 34, 0.95) 70%)' 
                : 'linear-gradient(160deg, rgba(20, 17, 58, 0.7) 0%, rgba(12, 10, 31, 0.85) 100%)',
            backdropFilter: 'blur(16px)',
            boxShadow: defenseModeActive ? '0 0 30px rgba(239, 68, 68, 0.2)' : '0 8px 32px rgba(0, 0, 0, 0.3)',
            padding: '1.25rem',
            display: 'flex',
            flexDirection: 'column',
            gap: '1rem',
            transition: 'all 0.4s ease'
        }}>
            {/* Header & Status Controls */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 12 }}>
                <div>
                    <h2 style={{ fontSize: 15, fontWeight: 600, color: '#f8fafc', display: 'flex', alignItems: 'center', gap: 8, margin: 0 }}>
                        <Activity size={18} color={defenseModeActive ? '#ef4444' : '#38bdf8'} />
                        Live Network Traffic Watchdog
                    </h2>
                    <p style={{ fontSize: 11.5, color: '#94a3b8', margin: '3px 0 0 0' }}>
                        Pull-based telemetry collector (<code style={{ color: '#38bdf8' }}>/metrics</code> polled every 2s) &amp; Real-time RPS delta
                    </p>
                </div>
                
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                    {/* Action buttons for live testing */}
                    {onSimulateSpike && (
                        <button
                            onClick={onSimulateSpike}
                            disabled={isSimulating || defenseModeActive}
                            title="Inject simulated traffic spike to test anomaly detection, active defense, and self-healing"
                            style={{
                                display: 'flex',
                                alignItems: 'center',
                                gap: 6,
                                padding: '0.4rem 0.8rem',
                                borderRadius: 8,
                                border: '1px solid rgba(245, 158, 11, 0.4)',
                                background: 'rgba(245, 158, 11, 0.12)',
                                color: '#fde68a',
                                fontSize: 11.5,
                                fontWeight: 500,
                                cursor: (isSimulating || defenseModeActive) ? 'not-allowed' : 'pointer',
                                opacity: (isSimulating || defenseModeActive) ? 0.6 : 1,
                                transition: 'all 0.2s ease'
                            }}
                        >
                            {isSimulating ? <Loader2 size={13} className="sso-spin" /> : <Zap size={13} color="#f59e0b" />}
                            Simulate Spike (14.5 RPS)
                        </button>
                    )}

                    {onRealBurst && (
                        <button
                            onClick={onRealBurst}
                            disabled={isSimulating}
                            title="Dispatches 25 real HTTP requests to FinSight backend to test rate limiting (HTTP 429)"
                            style={{
                                display: 'flex',
                                alignItems: 'center',
                                gap: 6,
                                padding: '0.4rem 0.8rem',
                                borderRadius: 8,
                                border: '1px solid rgba(239, 68, 68, 0.4)',
                                background: 'rgba(239, 68, 68, 0.12)',
                                color: '#fca5a5',
                                fontSize: 11.5,
                                fontWeight: 500,
                                cursor: isSimulating ? 'not-allowed' : 'pointer',
                                transition: 'all 0.2s ease'
                            }}
                        >
                            <Flame size={13} color="#ef4444" />
                            Real HTTP Burst (25 Reqs)
                        </button>
                    )}

                    {/* Defense Mode Badge */}
                    <div style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 6,
                        padding: '0.35rem 0.8rem',
                        borderRadius: 9999,
                        border: defenseModeActive ? '1px solid rgba(239, 68, 68, 0.6)' : '1px solid rgba(34, 197, 94, 0.35)',
                        background: defenseModeActive ? 'rgba(239, 68, 68, 0.18)' : 'rgba(34, 197, 94, 0.1)',
                        fontSize: 11.5,
                        fontWeight: 600,
                        color: defenseModeActive ? '#fca5a5' : '#86efac'
                    }}>
                        {defenseModeActive ? <ShieldAlert size={14} /> : <Shield size={14} />}
                        {defenseModeActive ? 'Active Defense Engaged' : 'Traffic Normal'}
                    </div>
                </div>
            </div>

            {/* Metrics Readout & Telemetry Stats Bar */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 12, borderBottom: '1px solid rgba(255,255,255,0.05)', paddingBottom: 10 }}>
                <div style={{ display: 'flex', alignItems: 'baseline', gap: 10 }}>
                    <span style={{ fontSize: 32, fontWeight: 700, color: defenseModeActive ? '#f87171' : '#ffffff', letterSpacing: '-0.02em', fontFamily: 'monospace' }}>
                        {Number(currentRps).toFixed(1)}
                    </span>
                    <span style={{ fontSize: 13, color: '#94a3b8' }}>req/s</span>
                    {defenseModeActive && (
                        <span style={{ fontSize: 11.5, color: '#fca5a5', background: 'rgba(239,68,68,0.18)', border: '1px solid rgba(239,68,68,0.3)', padding: '3px 10px', borderRadius: 6 }}>
                            Volumetric surge detected (&gt;{SPIKE_LIMIT} req/s). Closed-loop throttling &amp; stabilization window active.
                        </span>
                    )}
                </div>

                <div style={{ display: 'flex', alignItems: 'center', gap: 16, fontSize: 11.5, color: '#94a3b8' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                        <Activity size={13} color="#38bdf8" />
                        <span>Latency: <strong style={{ color: '#e2e8f0', fontFamily: 'monospace' }}>{currentLatency}ms</strong></span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                        <Database size={13} color="#34d399" />
                        <span>DB Cluster: <strong style={{ color: dbStatus === 'CONNECTED' ? '#86efac' : '#f87171', fontFamily: 'monospace' }}>{dbStatus}</strong></span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                        <Cpu size={13} color="#a78bfa" />
                        <span>Heap: <strong style={{ color: '#e2e8f0', fontFamily: 'monospace' }}>{heapUsed} MB</strong></span>
                    </div>
                </div>
            </div>

            {/* Responsive Recharts Container */}
            <div style={{ width: '100%', height: 230, marginTop: 4 }}>
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={trafficHistory} margin={{ top: 15, right: 10, left: -25, bottom: 0 }}>
                        <defs>
                            <linearGradient id="colorNormal" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#38bdf8" stopOpacity={0.45}/>
                                <stop offset="95%" stopColor="#38bdf8" stopOpacity={0}/>
                            </linearGradient>
                            <linearGradient id="colorAlert" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#ef4444" stopOpacity={0.6}/>
                                <stop offset="95%" stopColor="#ef4444" stopOpacity={0}/>
                            </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" vertical={false} />
                        <XAxis 
                            dataKey="time" 
                            stroke="#64748b" 
                            fontSize={11}
                            tickMargin={8}
                            minTickGap={25}
                        />
                        <YAxis 
                            stroke="#64748b" 
                            fontSize={11}
                            domain={[0, (dataMax) => Math.max(16, Math.ceil(dataMax + 4))]}
                        />
                        <Tooltip 
                            contentStyle={{ 
                                backgroundColor: '#0f0d22', 
                                borderColor: defenseModeActive ? 'rgba(239,68,68,0.4)' : 'rgba(139,92,246,0.3)', 
                                color: '#fff', 
                                borderRadius: '10px', 
                                fontSize: '12px',
                                boxShadow: '0 8px 24px rgba(0,0,0,0.5)'
                            }}
                            itemStyle={{ color: '#fff', fontWeight: 600 }}
                            labelStyle={{ color: '#94a3b8', marginBottom: '4px' }}
                            formatter={(value) => [`${Number(value).toFixed(2)} req/s`, 'Throughput (RPS)']}
                        />
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
                            dataKey="rps" 
                            stroke={themeColor} 
                            strokeWidth={2.5}
                            fillOpacity={1} 
                            fill={`url(#${gradientId})`} 
                            isAnimationActive={false}
                        />
                    </AreaChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
}