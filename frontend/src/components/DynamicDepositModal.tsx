import React from 'react';

interface DynamicDepositModalProps {
  isOpen: boolean;
  onClose: () => void;
  orderId: string;
  depositAmount?: number; // in paise
}

export function DynamicDepositModal({ isOpen, onClose, orderId, depositAmount = 4900 }: DynamicDepositModalProps) {
  if (!isOpen) return null;

  const amountInRupees = (depositAmount / 100).toFixed(0);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div className="w-full max-w-md bg-white border border-slate-200 rounded shadow-sm overflow-hidden">
        <div className="p-6">
          <h2 className="text-xl font-bold text-slate-900 mb-2">Verify your Cash on Delivery order</h2>
          <p className="text-slate-600 text-sm mb-6">
            Complete a refundable delivery deposit of ₹{amountInRupees}. This amount will be deducted from your final bill upon delivery.
          </p>

          <div className="bg-slate-50 border border-slate-100 p-4 rounded mb-6 flex justify-between items-center">
            <div>
              <span className="block text-xs text-slate-500 uppercase tracking-wide">Order</span>
              <span className="block font-medium text-slate-900">{orderId}</span>
            </div>
            <div className="text-right">
              <span className="block text-xs text-slate-500 uppercase tracking-wide">Deposit</span>
              <span className="block font-medium text-slate-900">₹{amountInRupees}</span>
            </div>
          </div>

          <div className="space-y-3">
            <button className="w-full bg-brand-primary hover:bg-brand-primary/90 text-white font-medium py-2.5 px-4 rounded transition-colors">
              Pay with UPI
            </button>
            <div className="text-center mt-4">
              <button onClick={onClose} className="text-sm text-slate-500 hover:text-slate-700 underline underline-offset-2">
                Cancel
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
