import React from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { Shield, ShieldAlert, Activity } from 'lucide-react';

export default function LiveTrafficChart({ trafficHistory = [], defenseModeActive = false }) {
    const currentRps = trafficHistory.length > 0 ? trafficHistory[trafficHistory.length - 1].rps : 0;
    const themeColor = defenseModeActive ? '#ef4444' : '#3b82f6'; 
    const gradientId = defenseModeActive ? 'colorAlert' : 'colorNormal';

    return (
        <div className={`p-6 rounded-xl border transition-colors duration-500 ${
            defenseModeActive ? 'bg-red-900/10 border-red-500/50' : 'bg-gray-900/50 border-gray-800'
        }`}>
            {/* Header & Status Badge */}
            <div className="flex justify-between items-center mb-6">
                <div>
                    <h3 className="text-lg font-semibold text-white flex items-center gap-2">
                        <Activity size={20} className={defenseModeActive ? 'text-red-500 animate-pulse' : 'text-blue-500'} />
                        Live Network Traffic
                    </h3>
                    <p className="text-sm text-gray-400 mt-1">Real-time requests per second (RPS)</p>
                </div>
                
                <div className={`px-4 py-2 rounded-lg flex items-center gap-2 font-medium transition-colors duration-500 ${
                    defenseModeActive 
                        ? 'bg-red-500/20 text-red-400 border border-red-500/30 shadow-[0_0_15px_rgba(239,68,68,0.2)]' 
                        : 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                }`}>
                    {defenseModeActive ? <ShieldAlert size={18} className="animate-pulse" /> : <Shield size={18} />}
                    {defenseModeActive ? 'Active Defense Engaged' : 'Traffic Normal'}
                </div>
            </div>

            {/* Metrics Readout */}
            <div className="flex gap-8 mb-6 h-10 items-center">
                <div className="text-3xl font-bold text-white tabular-nums">
                    {Number(currentRps).toFixed(1)} <span className="text-sm font-normal text-gray-500">req/s</span>
                </div>
                {defenseModeActive && (
                    <div className="text-red-400 text-sm flex items-center bg-red-500/10 px-3 py-2 rounded border border-red-500/20">
                        Malicious traffic spike isolated. Throttling active to protect cluster resources.
                    </div>
                )}
            </div>

            {/* Recharts Area Chart */}
            <div className="h-64 w-full">
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={trafficHistory} margin={{ top: 10, right: 0, left: -20, bottom: 0 }}>
                        <defs>
                            <linearGradient id="colorNormal" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.4}/>
                                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                            </linearGradient>
                            <linearGradient id="colorAlert" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#ef4444" stopOpacity={0.5}/>
                                <stop offset="95%" stopColor="#ef4444" stopOpacity={0}/>
                            </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
                        <XAxis 
                            dataKey="time" 
                            stroke="#6b7280" 
                            fontSize={12}
                            tickMargin={10}
                            minTickGap={30}
                        />
                        <YAxis 
                            stroke="#6b7280" 
                            fontSize={12}
                            tickFormatter={(value) => `${value}`}
                        />
                        <Tooltip 
                            contentStyle={{ backgroundColor: '#1f2937', borderColor: '#374151', color: '#fff', borderRadius: '8px' }}
                            itemStyle={{ color: '#fff', fontWeight: 'bold' }}
                            labelStyle={{ color: '#9ca3af', marginBottom: '4px' }}
                        />
                        <ReferenceLine 
                            y={45} 
                            stroke="#ef4444" 
                            strokeDasharray="4 4" 
                            label={{ position: 'insideTopLeft', value: 'Spike Threshold (45 req/s)', fill: '#ef4444', fontSize: 12 }} 
                        />
                        <Area 
                            type="monotone" 
                            dataKey="rps" 
                            stroke={themeColor} 
                            strokeWidth={3}
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