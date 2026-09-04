import type { Metadata } from 'next';
import { Inter, JetBrains_Mono } from 'next/font/google';
import './globals.css';
import Link from 'next/link';
import { LayoutDashboard, Network, TrendingUp, Shield } from 'lucide-react';

const inter = Inter({ subsets: ['latin'], variable: '--font-inter' });
const jetbrainsMono = JetBrains_Mono({ subsets: ['latin'], variable: '--font-jetbrains-mono' });

export const metadata: Metadata = {
  title: 'SENTINEL-RTO | Risk Console',
  description: 'Risk Operations Dashboard',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${inter.variable} ${jetbrainsMono.variable} font-sans bg-white text-slate-900 antialiased`}>
        <div className="flex min-h-screen">
          <aside className="fixed left-0 top-0 bottom-0 w-56 border-r border-slate-200 bg-white">
            <div className="flex items-center gap-2 p-4 border-b border-slate-200">
              <Shield className="w-6 h-6 text-brand-primary" />
              <span className="font-semibold text-lg">SENTINEL</span>
            </div>
            <nav className="p-4 space-y-2">
              <Link href="/" className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-slate-50 text-slate-700 hover:text-slate-900">
                <LayoutDashboard className="w-5 h-5" />
                <span className="text-sm font-medium">Dashboard</span>
              </Link>
              <Link href="/graph" className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-slate-50 text-slate-700 hover:text-slate-900">
                <Network className="w-5 h-5" />
                <span className="text-sm font-medium">Graph Explorer</span>
              </Link>
              <Link href="/optimizer" className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-slate-50 text-slate-700 hover:text-slate-900">
                <TrendingUp className="w-5 h-5" />
                <span className="text-sm font-medium">Cost Optimizer</span>
              </Link>
            </nav>
          </aside>
          <main className="ml-56 flex-1 p-6">
            {children}
          </main>
        </div>
      </body>
    </html>
  );
}
