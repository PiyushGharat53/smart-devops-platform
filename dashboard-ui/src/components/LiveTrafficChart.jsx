import React from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { Shield, ShieldAlert, Activity } from 'lucide-react';

export default function LiveTrafficChart({ trafficHistory = [], defenseModeActive = false }) {
    const currentRps = trafficHistory.length > 0 ? trafficHistory[trafficHistory.length - 1].rps : 0;
    const themeColor = defenseModeActive ? '#ef4444' : '#38bdf8';
    const gradientId = defenseModeActive ? 'colorAlert' : 'colorNormal';

    return (
        <div style={{
            borderRadius: '1rem',
            border: defenseModeActive ? '1px solid rgba(239, 68, 68, 0.5)' : '1px solid var(--border-glow, rgba(99,102,241,0.2))',
            background: defenseModeActive ? 'rgba(239, 68, 68, 0.06)' : 'var(--glass-fill, rgba(255,255,255,0.03))',
            backdropFilter: 'blur(16px)',
            padding: '1.25rem',
            display: 'flex',
            flexDirection: 'column',
            gap: '1rem',
            transition: 'all 0.4s ease'
        }}>
            {/* Header & Status Badge */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
                <div>
                    <h2 style={{ fontSize: 14, fontWeight: 600, color: '#e2e8f0', display: 'flex', alignItems: 'center', gap: 8, margin: 0 }}>
                        <Activity size={16} color={defenseModeActive ? '#ef4444' : '#38bdf8'} />
                        Live Network Traffic Watchdog
                    </h2>
                    <p style={{ fontSize: 11, color: '#64748b', margin: '4px 0 0 0' }}>Real-time requests per second (RPS) &amp; Anomaly Detection</p>
                </div>
                
                <div style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 6,
                    padding: '0.35rem 0.75rem',
                    borderRadius: 9999,
                    border: defenseModeActive ? '1px solid rgba(239, 68, 68, 0.5)' : '1px solid rgba(34, 197, 94, 0.3)',
                    background: defenseModeActive ? 'rgba(239, 68, 68, 0.15)' : 'rgba(34, 197, 94, 0.1)',
                    fontSize: 11,
                    fontWeight: 600,
                    color: defenseModeActive ? '#fca5a5' : '#86efac'
                }}>
                    {defenseModeActive ? <ShieldAlert size={14} /> : <Shield size={14} />}
                    {defenseModeActive ? 'Active Defense Engaged' : 'Traffic Normal'}
                </div>
            </div>

            {/* Metrics Readout */}
            <div style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
                <span style={{ fontSize: 28, fontWeight: 700, color: '#ffffff', letterSpacing: '-0.02em', fontFamily: 'monospace' }}>
                    {Number(currentRps).toFixed(1)}
                </span>
                <span style={{ fontSize: 12, color: '#64748b' }}>req/s</span>
                {defenseModeActive && (
                    <span style={{ fontSize: 12, color: '#fca5a5', background: 'rgba(239,68,68,0.15)', padding: '2px 8px', borderRadius: 6 }}>
                        Threshold exceeded (45 req/s). Active defense throttling in progress.
                    </span>
                )}
            </div>

            {/* Responsive Recharts Container */}
            <div style={{ width: '100%', height: 220, marginTop: 4 }}>
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={trafficHistory} margin={{ top: 10, right: 10, left: -25, bottom: 0 }}>
                        <defs>
                            <linearGradient id="colorNormal" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#38bdf8" stopOpacity={0.4}/>
                                <stop offset="95%" stopColor="#38bdf8" stopOpacity={0}/>
                            </linearGradient>
                            <linearGradient id="colorAlert" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#ef4444" stopOpacity={0.5}/>
                                <stop offset="95%" stopColor="#ef4444" stopOpacity={0}/>
                            </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" vertical={false} />
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
                            domain={[0, 'auto']}
                        />
                        <Tooltip 
                            contentStyle={{ backgroundColor: '#0f0d22', borderColor: 'rgba(139,92,246,0.3)', color: '#fff', borderRadius: '8px', fontSize: '12px' }}
                            itemStyle={{ color: '#fff', fontWeight: 600 }}
                            labelStyle={{ color: '#94a3b8', marginBottom: '2px' }}
                        />
                        <ReferenceLine 
                            y={45} 
                            stroke="#ef4444" 
                            strokeDasharray="4 4" 
                            label={{ position: 'insideTopLeft', value: 'Spike Threshold (45 req/s)', fill: '#ef4444', fontSize: 10 }} 
                        />
                        <Area 
                            type="monotone" 
                            dataKey="rps" 
                            stroke={themeColor} 
                            strokeWidth={2}
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