"use client";

import React from 'react';
import { RiskMetricCard } from '@/components/RiskMetricCard';
import { mockMetrics, mockEvaluations } from '@/lib/mock-data';
import { Activity, Zap, IndianRupee, ShieldAlert } from 'lucide-react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export default function Dashboard() {
  const currentDate = new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });

  const distributionData = [
    { range: '0-20', count: 450 },
    { range: '21-40', count: 320 },
    { range: '41-60', count: 210 },
    { range: '61-80', count: 180 },
    { range: '81-100', count: 90 },
  ];

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Risk Operations Dashboard</h1>
        <p className="text-sm text-slate-500">{currentDate}</p>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <RiskMetricCard 
          title="Total Evaluations" 
          value={mockMetrics.totalEvaluations} 
          change="+12.5%" 
          changeType="positive"
          icon={<Activity className="w-5 h-5 text-slate-400" />}
        />
        <RiskMetricCard 
          title="Average Risk Score" 
          value={mockMetrics.avgRiskScore} 
          change="-2.1" 
          changeType="positive"
          icon={<ShieldAlert className="w-5 h-5 text-slate-400" />}
        />
        <RiskMetricCard 
          title="P99 Latency (ms)" 
          value={mockMetrics.p99Latency} 
          change="-15ms" 
          changeType="positive"
          icon={<Zap className="w-5 h-5 text-slate-400" />}
        />
        <RiskMetricCard 
          title="Capital Saved" 
          value={`₹${mockMetrics.capitalSaved}`} 
          change="+5.4%" 
          changeType="positive"
          icon={<IndianRupee className="w-5 h-5 text-slate-400" />}
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 border border-slate-200 rounded-md bg-white p-5">
          <h2 className="text-lg font-semibold mb-4">Recent Risk Evaluations</h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm text-left">
              <thead className="text-xs text-slate-500 uppercase bg-slate-50 border-b border-slate-200">
                <tr>
                  <th className="px-4 py-3">Order ID</th>
                  <th className="px-4 py-3">Risk Score</th>
                  <th className="px-4 py-3">Risk Tier</th>
                  <th className="px-4 py-3">Latency</th>
                  <th className="px-4 py-3">Timestamp</th>
                </tr>
              </thead>
              <tbody>
                {mockEvaluations.slice(0, 10).map((evaluation) => (
                  <tr key={evaluation.orderId} className="border-b border-slate-100 last:border-0 hover:bg-slate-50/50">
                    <td className="px-4 py-3 font-medium text-slate-900">{evaluation.orderId}</td>
                    <td className="px-4 py-3">
                      <span className={`px-2 py-1 rounded-sm text-xs font-medium ${
                        evaluation.score >= 80 ? 'bg-risk-critical/10 text-risk-critical' :
                        evaluation.score >= 50 ? 'bg-risk-warning/10 text-risk-warning' :
                        'bg-risk-safe/10 text-risk-safe'
                      }`}>
                        {evaluation.score}
                      </span>
                    </td>
                    <td className="px-4 py-3 capitalize">{evaluation.tier}</td>
                    <td className="px-4 py-3">{evaluation.latency}ms</td>
                    <td className="px-4 py-3 text-slate-500">{new Date(evaluation.timestamp).toLocaleTimeString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="border border-slate-200 rounded-md bg-white p-5">
          <h2 className="text-lg font-semibold mb-4">Risk Score Distribution</h2>
          <div className="h-64 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={distributionData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis dataKey="range" axisLine={false} tickLine={false} tick={{ fontSize: 12, fill: '#64748b' }} />
                <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 12, fill: '#64748b' }} />
                <Tooltip cursor={{ fill: '#f1f5f9' }} contentStyle={{ borderRadius: '4px', border: '1px solid #e2e8f0' }} />
                <Bar dataKey="count" fill="#0B5CFF" radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}
