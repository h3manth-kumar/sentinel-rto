# Data Architecture & Database Schema (SCHEMA)
**Project:** SENTINEL-RTO  

---

## 1. Entity Relationship Overview (Conceptual)

[ Merchants ] 1 --------- N [ Transactions ] N --------- 1 [ Addresses_H3 ]
|
+--- N --- 1 [ Devices ]
|
+--- 1 --- 1 [ Risk_Evaluations ]
|
+--- N --- 1 [ Syndicate_Clusters ]


---

## 2. PostgreSQL DDL (Relational Storage & Auditing)

```sql
-- Core Merchant Table
CREATE TABLE merchants (
    merchant_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    api_key_hash VARCHAR(128) NOT NULL,
    deposit_step_up_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Spatial Entity Index
CREATE TABLE addresses_h3 (
    h3_index_res9 VARCHAR(15) PRIMARY KEY,
    h3_index_res8 VARCHAR(15) NOT NULL,
    pincode VARCHAR(10) NOT NULL,
    total_orders INT DEFAULT 0,
    rto_deliveries INT DEFAULT 0,
    last_updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_addresses_h3_res8 ON addresses_h3(h3_index_res8);

-- Device Registry
CREATE TABLE devices (
    device_hash VARCHAR(64) PRIMARY KEY,
    canvas_hash VARCHAR(64),
    first_seen TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    is_proxy BOOLEAN DEFAULT FALSE,
    associated_accounts_count INT DEFAULT 1
);

-- Syndicate Cluster Table (Graph Aggregation Sink)
CREATE TABLE syndicate_clusters (
    cluster_id VARCHAR(64) PRIMARY KEY,
    root_entity_type VARCHAR(32) NOT NULL, -- 'DEVICE', 'ADDRESS', 'VPA'
    cluster_size INT NOT NULL,
    composite_rto_rate NUMERIC(5, 4) NOT NULL,
    is_blacklisted BOOLEAN DEFAULT FALSE,
    discovered_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Transaction Stream
CREATE TABLE transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    merchant_id VARCHAR(64) REFERENCES merchants(merchant_id),
    order_id VARCHAR(128) NOT NULL,
    amount_in_paise BIGINT NOT NULL,
    payment_method VARCHAR(32) NOT NULL,
    customer_phone_hash VARCHAR(64) NOT NULL,
    device_hash VARCHAR(64) REFERENCES devices(device_hash),
    h3_index_res9 VARCHAR(15) REFERENCES addresses_h3(h3_index_res9),
    status VARCHAR(32) NOT NULL, -- 'PENDING', 'DELIVERED', 'RTO', 'CANCELLED'
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_transactions_phone ON transactions(customer_phone_hash);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);

-- Risk Evaluation Audit Ledger
CREATE TABLE risk_evaluations (
    evaluation_id VARCHAR(64) PRIMARY KEY,
    transaction_id VARCHAR(64) REFERENCES transactions(transaction_id),
    risk_score INT NOT NULL,
    risk_tier VARCHAR(32) NOT NULL,
    decision_action VARCHAR(64) NOT NULL,
    shap_attribution JSONB NOT NULL,
    total_latency_ms NUMERIC(6, 2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
---
##3. Redis In-Memory Data Models:

# 1. Precomputed Entity Risk Hash (O(1) lookup during checkout)
Key:   entity:device:{device_hash}
Type:  HASH
Field: rto_rate -> "0.85"
Field: order_count -> "14"
Field: cluster_id -> "clust_9901"
TTL:   86400 (24 hours)

# 2. H3 Hexagonal Spatial Metric
Key:   entity:h3:{h3_index_res9}
Type:  HASH
Field: cluster_rto_rate -> "0.42"
Field: density_weight -> "1.12"
TTL:   86400 (24 hours)

# 3. Sliding-Window Burst Counter (Edge Race Condition Guard)
Key:   burst:h3:{h3_index_res9}
Type:  ZSET
Score: Unix Timestamp (Milliseconds)
Value: "{order_id}"
Command: ZADD burst:h3:89618925133ffff 1724864368102 "ord_8829104"
Query:   ZREMRANGEBYSCORE burst:h3:89618925133ffff 0 (Current_Time - 10000)
         ZCARD burst:h3:89618925133ffff  -> Returns count in last 10s

---

### `IMPLEMENTATION_PLAN.md`

```markdown
# Phased Execution Roadmap (IMPLEMENTATION_PLAN)
**Project:** SENTINEL-RTO  

---

## 1. Phase Breakdown

| Phase | Objectives | Deliverables |
| :--- | :--- | :--- |
| **Phase 1: Data & Modeling**<br>*(Days 1-2)* | Synthetic Data Generation, Ground-Truth Labeling, Baseline LightGBM & ONNX Export. | • 100k Transaction Synthetic Dataset<br>• Pre-trained ONNX Model File<br>• Held-out Test Set Benchmark Report |
| **Phase 2: Graph Intelligence**<br>*(Days 3-4)* | Graph ML Engine, Louvain Clustering, Address Entity Normalization (Uber H3) & Feature Store Sync. | • NetworkX Syndicate Extractor<br>• H3 Spatial Transformation Pipeline<br>• Redis Feature Store Population Job |
| **Phase 3: Real-Time Engine**<br>*(Days 5-6)* | FastAPI Sub-50ms Online Scoring Engine, Edge Burst Atomic Rate Limiter, Dynamic Friction Logic. | • Online Scoring API (<50ms P99)<br>• Sliding-Window ZSET Burst Guard<br>• Fallback Circuit Breaker Layer |
| **Phase 4: UI & Demo Prep**<br>*(Days 7-8)* | Next.js Merchant Console, Threat Visualizer, Cost-Curve Optimization Demo App. | • Interactive Merchant Dashboard<br>• Live Fraud-Ring Injection Demo<br>• Final Benchmark & Slide Deck |

---

## 2. Dependencies & Risk Mitigations

* **Dependency:** Uber H3 Python C-extensions under high concurrency.
  * *Mitigation:* Pre-compile address lookup table into memory for high-frequency pin codes; fallback to pure math bounding boxes if native bindings fault.
* **Dependency:** Redis connection pool starvation under 2,500 RPS load testing.
  * *Mitigation:* Deploy `redis-py` async connection pool with keep-alive timeouts capped at 10ms and pipeline batch requests (`MGET`).
* **Dependency:** Cold-start latency on ONNX model invocation.
* *Mitigation:* Initialize and warm up inference sessions at application startup with synthetic dummy tensors.