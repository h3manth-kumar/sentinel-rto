import React from 'react';
import clsx from 'clsx';

interface RiskMetricCardProps {
  title: string;
  value: string | number;
  change: string;
  changeType: 'positive' | 'negative' | 'neutral';
  icon?: React.ReactNode;
}

export function RiskMetricCard({ title, value, change, changeType, icon }: RiskMetricCardProps) {
  return (
    <div className="border border-slate-200 rounded p-5 bg-white flex flex-col justify-between">
      <div className="flex justify-between items-start mb-4">
        <h3 className="text-sm text-slate-500 font-medium">{title}</h3>
        {icon && <div>{icon}</div>}
      </div>
      <div className="flex items-baseline gap-2">
        <p className="text-2xl font-semibold text-slate-900">{value}</p>
        <span className={clsx(
          "text-xs font-medium px-1.5 py-0.5 rounded-sm",
          changeType === 'positive' && "bg-risk-safe/10 text-risk-safe",
          changeType === 'negative' && "bg-risk-critical/10 text-risk-critical",
          changeType === 'neutral' && "bg-slate-100 text-slate-600"
        )}>
          {change}
        </span>
      </div>
    </div>
  );
}
