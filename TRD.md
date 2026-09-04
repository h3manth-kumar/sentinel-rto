# Technical Requirements Document (TRD)
**Project:** SENTINEL-RTO  
**Status:** Approved Architecture  

---

## 1. System Architecture Overview

[ Merchant Checkout Modal ]
│
▼  (HTTPS / TLS 1.3)
[ Cloudflare / API Gateway ] (JWT Auth, Rate Limiting, DDoS Shield)
│
├─────────────────────────────────────────────────────┤
│ [ ONLINE FAST-PATH: < 50ms ]                        │
│                                                     │
│  ┌───────────────────────────────────────────────┐  │
│  │ FastAPI / Python 3.11 Risk Gateway            │  │
│  │  1. Atomic Redis Sliding-Window Burst Counter │  │
│  │  2. O(1) Redis Feature Store Lookup           │  │
│  │  3. ONNX Runtime LightGBM Inference Engine    │  │
│  │  4. Dynamic Policy Evaluator                  │  │
│  └───────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────┤
│
├───> Emits Async Order Event to Kafka
│
▼
[ Apache Kafka Event Bus ] (Topics: orders.raw, cancellations, rto.events)
│
▼
[ Apache Flink / Streaming Engine ]
│
▼
[ Graph ML Worker (NetworkX / PyTorch Geometric) ]

Entity Resolution (Address H3 Res 8/9, Device Hash, Phone Salt)
Louvain Community Detection / Subgraph Clustering
GraphSAGE Embedding Generation
│
▼ (Batch Sync / MSET)
[ Redis Cluster (Feature Store) ]
│
▼ (Cold Storage Persistence)
[ PostgreSQL + TimescaleDB ] (Auditing, Analytical Ledgers, Retraining)




---

## 2. Technology Stack

| Layer | Selected Technology | Architectural Rationale |
| :--- | :--- | :--- |
| **API Gateway / App** | FastAPI (Python 3.11 Async) | High-throughput async I/O; native Pydantic validation with sub-millisecond route handling. |
| **ML Inference** | ONNX Runtime (C++ bindings) + LightGBM | 4-8x lower latency than pure Python LightGBM engine; deterministic thread execution under 10ms. |
| **In-Memory Cache** | Redis Cluster v7.2 (Lua Scripts) | Sub-millisecond key-value lookups; native atomic ZSET operations for sliding-window burst protection. |
| **Event Streaming** | Apache Kafka (Confluent) | Scalable event streaming decouples the online path from heavy graph transformations. |
| **Graph Analytics** | NetworkX + PyG | Efficient community clustering (Louvain) and relational risk feature propagation. |
| **Spatial Index** | Uber H3 Engine (`h3-py`) | Discrete global grid system mapping raw lat/lng and addresses to hierarchical hexagonal cells. |
| **Primary DB / OLAP**| PostgreSQL 16 + TimescaleDB | ACID-compliant storage for audit logs, transactions, and time-series RTO reconciliation. |
| **Frontend Console** | Next.js 14, Tailwind CSS, Tremor | Server-side rendered merchant console with dynamic WebGL graph rendering for syndicate visualization. |

---

## 3. API Specifications

### `POST /v1/risk/evaluate`
Evaluates an incoming checkout transaction in real-time.

```json
// REQUEST PAYLOAD
{
  "merchant_id": "mer_rzp_99481a0e",
  "order_id": "ord_8829104",
  "timestamp": "2026-08-28T16:59:28.102Z",
  "amount_in_paise": 149900,
  "payment_method": "COD",
  "customer": {
    "phone_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "email_domain": "gmail.com",
    "account_age_days": 2
  },
  "device": {
    "fingerprint_hash": "dev_a19b882c0f",
    "ip_address": "49.207.180.12",
    "user_agent_raw": "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36",
    "client_signals": {
      "is_bot_keystrokes": false,
      "form_fill_duration_ms": 1820,
      "canvas_entropy_score": 0.88
    }
  },
  "shipping_address": {
    "raw_text": "Flat 402, Tower B, Green Glen Layout, Bellandur, Bengaluru",
    "pincode": "560103",
    "h3_index_res9": "89618925133ffff"
  }
}



// RESPONSE PAYLOAD (200 OK - Latency: 28ms)
{
  "evaluation_id": "eval_77182903",
  "order_id": "ord_8829104",
  "risk_score": 64,
  "risk_tier": "CHALLENGE_DEPOSIT",
  "action": {
    "type": "REQUIRE_PARTIAL_DEPOSIT",
    "deposit_amount_in_paise": 4900,
    "reason_code": "SPATIAL_CLUSTER_VELOCITY_SPIKE"
  },
  "explanation": {
    "shap_contributors": [
      {"feature": "h3_cluster_rto_rate", "weight": 0.38},
      {"feature": "device_burst_velocity_10s", "weight": 0.29},
      {"feature": "account_age_days", "weight": -0.05}
    ],
    "syndicate_detected": false
  },
  "execution_metrics": {
    "total_latency_ms": 28.4,
    "redis_lookup_ms": 4.1,
    "onnx_inference_ms": 11.2
  }
}



4. Security, Isolation, and DPDP Compliance
Zero Raw PII Persistence: Customer phone numbers, email prefixes, and device MACs are salted using a per-merchant cryptographic key and hashed via HMAC-SHA-256 before storage or graph node generation.

Deterministic Tokenization: Hashing algorithms preserve node uniqueness across identical entities without revealing cleartext PII.

Fail-Safe Circuit Breaker: If the online inference engine breaches a 45ms timeout threshold or experiences a critical fault, the gateway falls back to FAIL_SAFE_ALLOW and emits an alert to monitoring streams.