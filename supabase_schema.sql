-- ==============================================================================
-- SENTINEL-RTO: Supabase PostgreSQL Schema & Real-Time Sync Tables
-- ==============================================================================
-- Run this script in your Supabase Dashboard -> SQL Editor
-- (https://supabase.com/dashboard/project/dqoaljyrmkvvhdawxqjk/sql/new)

CREATE TABLE IF NOT EXISTS public.orders (
    order_id VARCHAR(128) PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_phone VARCHAR(64),
    amount_paise BIGINT NOT NULL,
    payment_method VARCHAR(32) NOT NULL,
    payment_status VARCHAR(32) NOT NULL DEFAULT 'COD_PENDING',
    risk_score INT DEFAULT 0,
    risk_tier VARCHAR(32),
    action VARCHAR(64),
    raw_address TEXT,
    pincode VARCHAR(10),
    area_name VARCHAR(255),
    h3_index VARCHAR(20),
    what_action TEXT,
    why_reason TEXT,
    plain_english_reason TEXT,
    reasons_list JSONB DEFAULT '[]'::jsonb,
    items JSONB DEFAULT '[]'::jsonb,
    shipping_logistics JSONB DEFAULT '{}'::jsonb,
    invoice JSONB DEFAULT '{}'::jsonb,
    delivery_outcome VARCHAR(32),
    latency_ms NUMERIC(8, 2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for high-speed queries & analytics
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON public.orders(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_h3_index ON public.orders(h3_index);
CREATE INDEX IF NOT EXISTS idx_orders_payment_status ON public.orders(payment_status);
CREATE INDEX IF NOT EXISTS idx_orders_risk_score ON public.orders(risk_score);

-- Enable Row Level Security (RLS) and grant read/write access
ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Allow anon read and write on orders" ON public.orders;
CREATE POLICY "Allow anon read and write on orders" 
ON public.orders 
FOR ALL 
TO anon, authenticated, service_role 
USING (true) 
WITH CHECK (true);

-- Grant table permissions to PostgREST roles
GRANT ALL ON TABLE public.orders TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO anon, authenticated, service_role;

-- Enable Supabase Realtime for live updates (optional)
ALTER PUBLICATION supabase_realtime ADD TABLE public.orders;
