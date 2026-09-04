"use client";

import React, { useState } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { mockCostCurveData } from '@/lib/mock-data';

export default function CostOptimizer() {
  const [threshold, setThreshold] = useState(60);

  const currentData = mockCostCurveData.find(d => d.threshold === threshold) || mockCostCurveData[0];

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">GMV/Loss Curve Optimizer</h1>
        <p className="text-sm text-slate-500">Adjust risk thresholds to balance logistics savings vs lost revenue.</p>
      </header>

      <div className="border border-slate-200 rounded-md bg-white p-6 space-y-8">
        <div>
          <div className="flex justify-between items-center mb-2">
            <label className="text-sm font-medium text-slate-700">Risk Threshold</label>
            <span className="text-lg font-semibold text-brand-primary">{threshold}</span>
          </div>
          <input
            type="range"
            min="0"
            max="100"
            step="1"
            value={threshold}
            onChange={(e) => setThreshold(Number(e.target.value))}
            className="w-full h-2 bg-slate-200 rounded-lg appearance-none cursor-pointer accent-brand-primary"
          />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="p-4 bg-slate-50 border border-slate-100 rounded">
            <h3 className="text-xs text-slate-500 uppercase tracking-wide">Orders Blocked</h3>
            <p className="text-2xl font-semibold text-slate-900">{currentData.ordersBlocked}</p>
          </div>
          <div className="p-4 bg-risk-safe/5 border border-risk-safe/20 rounded">
            <h3 className="text-xs text-risk-safe uppercase tracking-wide">Logistics Savings (₹)</h3>
            <p className="text-2xl font-semibold text-risk-safe">₹{(currentData.logisticsSavings / 1000).toFixed(1)}k</p>
          </div>
          <div className="p-4 bg-risk-critical/5 border border-risk-critical/20 rounded">
            <h3 className="text-xs text-risk-critical uppercase tracking-wide">Revenue Lost (₹)</h3>
            <p className="text-2xl font-semibold text-risk-critical">₹{(currentData.revenueLost / 1000).toFixed(1)}k</p>
          </div>
        </div>

        <div>
          <div className="flex justify-between items-end mb-4">
            <h3 className="text-sm font-semibold">Net Savings: <span className={currentData.netSavings >= 0 ? 'text-risk-safe' : 'text-risk-critical'}>₹{(currentData.netSavings / 1000).toFixed(1)}k</span></h3>
          </div>
          <div className="h-80 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={mockCostCurveData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis 
                  dataKey="threshold" 
                  axisLine={false} 
                  tickLine={false} 
                  tick={{ fontSize: 12, fill: '#64748b' }}
                  label={{ value: 'Risk Threshold', position: 'insideBottomRight', offset: -10, fill: '#64748b', fontSize: 12 }}
                />
                <YAxis 
                  axisLine={false} 
                  tickLine={false} 
                  tick={{ fontSize: 12, fill: '#64748b' }} 
                  tickFormatter={(val) => `₹${val/1000}k`}
                />
                <Tooltip 
                  contentStyle={{ borderRadius: '4px', border: '1px solid #e2e8f0' }}
                  formatter={(value: number) => `₹${value}`}
                />
                <Area type="monotone" dataKey="logisticsSavings" stackId="1" stroke="#10B981" fill="#10B981" fillOpacity={0.2} name="Logistics Savings" />
                <Area type="monotone" dataKey="revenueLost" stackId="2" stroke="#EF4444" fill="#EF4444" fillOpacity={0.2} name="Revenue Lost" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}
