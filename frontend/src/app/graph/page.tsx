"use client";

import React, { useState, useEffect, useRef } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { mockGraphNodes, mockGraphLinks } from '@/lib/mock-data';

export default function GraphExplorer() {
  const [mounted, setMounted] = useState(false);
  const [selectedNode, setSelectedNode] = useState<any>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setMounted(true);
    const updateDimensions = () => {
      if (containerRef.current) {
        setDimensions({
          width: containerRef.current.offsetWidth,
          height: containerRef.current.offsetHeight
        });
      }
    };

    window.addEventListener('resize', updateDimensions);
    // Add small delay to ensure container is fully rendered before measuring
    setTimeout(updateDimensions, 0);

    return () => window.removeEventListener('resize', updateDimensions);
  }, []);

  const getNodeColor = (type: string) => {
    switch (type) {
      case 'account': return '#0B5CFF'; // Blue
      case 'device': return '#10B981'; // Green
      case 'address': return '#F59E0B'; // Orange
      case 'scammer': return '#EF4444'; // Red
      default: return '#94A3B8';
    }
  };

  const handleNodeClick = (node: any) => {
    setSelectedNode(node);
  };

  return (
    <div className="h-[calc(100vh-3rem)] flex flex-col">
      <header className="mb-4">
        <h1 className="text-2xl font-bold text-slate-900">Syndicate Graph Explorer</h1>
        <p className="text-sm text-slate-500">Visualize connection anomalies and risk clusters</p>
      </header>

      <div className="flex-1 flex gap-4 min-h-0">
        <div 
          ref={containerRef}
          className="flex-1 border border-dashed border-slate-300 rounded-md bg-slate-50 relative overflow-hidden"
        >
          {mounted && (
            <ForceGraph2D
              width={dimensions.width}
              height={dimensions.height}
              graphData={{ nodes: mockGraphNodes, links: mockGraphLinks }}
              nodeColor={(node) => getNodeColor((node as any).type)}
              nodeRelSize={6}
              onNodeClick={handleNodeClick}
              linkColor={() => '#CBD5E1'}
            />
          )}
        </div>

        {selectedNode && (
          <div className="w-80 border border-slate-200 rounded-md bg-white p-5 flex flex-col overflow-y-auto">
            <div className="flex justify-between items-start mb-4">
              <h2 className="text-lg font-semibold">Node Details</h2>
              <button 
                onClick={() => setSelectedNode(null)}
                className="text-slate-400 hover:text-slate-600"
              >
                ×
              </button>
            </div>
            
            <div className="space-y-4">
              <div>
                <span className="text-xs text-slate-500 uppercase tracking-wider">ID</span>
                <p className="font-mono text-sm">{selectedNode.id}</p>
              </div>
              
              <div>
                <span className="text-xs text-slate-500 uppercase tracking-wider">Entity Type</span>
                <p className="capitalize font-medium">{selectedNode.type}</p>
              </div>

              <div>
                <span className="text-xs text-slate-500 uppercase tracking-wider">Risk Score</span>
                <p className="font-semibold text-lg">{selectedNode.riskScore || 'N/A'}</p>
              </div>
              
              <div>
                <span className="text-xs text-slate-500 uppercase tracking-wider">RTO Rate</span>
                <p className="font-medium">{(selectedNode.rtoRate * 100).toFixed(1)}%</p>
              </div>

              <div>
                <span className="text-xs text-slate-500 uppercase tracking-wider">Connections</span>
                <p className="font-medium">{
                  mockGraphLinks.filter(l => l.source === selectedNode.id || l.target === selectedNode.id || (l.source as any).id === selectedNode.id || (l.target as any).id === selectedNode.id).length
                }</p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
