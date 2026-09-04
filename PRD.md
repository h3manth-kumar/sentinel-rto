<!-- ========================================== -->
<!-- File: PRD.md                               -->
<!-- ========================================== -->
# Product Requirements Document (PRD)
**Project Name:** SENTINEL-RTO  
**Target Track:** Razorpay Buildathon Track 02 (AI Risk Manager)  
**Document Version:** 1.0.0  

---

## 1. Executive Summary & Goals
SENTINEL-RTO is an enterprise-grade, defense-only merchant risk engine designed to curb two chronic revenue drains in Indian e-commerce:
1. **Return to Origin (RTO) Fraud:** Non-delivery, intentional refusal at doorstep, and phantom orders on Cash on Delivery (COD).
2. **Syndicate Abuse Rings:** Distributed fraud networks using rotating identities, burner phone numbers, and proxy devices to exhaust merchant inventory and exploit promotional discounts.

Traditional risk solutions rely on static pin-code blocklists (which indiscriminately kill legitimate revenue) or asynchronous fraud models (which suffer from streaming lag). SENTINEL-RTO deploys a **Two-Path Architecture**:
* **Online Fast-Path (≤ 50ms P99 SLA):** Evaluates incoming checkout requests using in-memory feature caches, atomic sliding-window burst velocity limiters, and an ONNX-optimized LightGBM model.
* **Offline/Streaming Graph Engine:** Ingests events asynchronously to compute Louvain community detection, H3 spatial clustering, and cross-account entity linkage, continuously sinking precomputed risk metrics to an in-memory feature store.

### Target Success Metrics (KPIs)
* **RTO Reduction:** Lower COD return rates by ≥ 35% across integrated merchant test volumes.
* **Conversion Preservation (False Positive Control):** Maintain ≥ 98% approval rate on legitimate, high-intent consumer checkouts without adding checkout friction.
* **Latency SLA:** ≤ 50 ms P99 response time for online risk scoring at checkout.
* **Financial Loss Optimization:** Minimize the Total Expected Loss function:
  $$\text{Loss} = (\text{FN} \times \text{Cost}_{\text{RTO}}) + (\text{FP} \times \text{Margin}_{\text{Lost}})$$
* **Model Benchmarks (Held-Out Test Set):** Precision ≥ 88%, Recall ≥ 82%, ROC-AUC ≥ 0.91.

---

## 2. User Personas & Pain Points

| Persona | Core Responsibilities | Critical Pain Points |
| :--- | :--- | :--- |
| **Rajiv (D2C Founder)**<br>Mid-Market Merchant | Revenue growth, margin control, logistics partner management. | Logistics costs eat 25% of operating margin due to fake COD orders; blanket COD blocks cause 40% cart abandonment. |
| **Ananya**<br>Platform Risk Lead | Chargeback defense, rule engine maintenance, false positive escalation review. | Overwhelmed by manual reviews; static rules fail against organized fraud rings using burner accounts and anti-detect tools. |
| **Rohan**<br>Honest End-Consumer | Shopping online, expecting frictionless checkout. | Gets blocked or forced to pay upfront when living in large societies where others abused COD. |

---

## 3. Functional Requirements

### Epic 1: Real-Time Checkout Risk Scoring & Dynamic Friction
* **Story 1.1 (Risk Inference):** As a checkout service, I need to send a payload containing user, device, cart, and address details to receive an instant risk score (0–100) and recommendation tier (`ALLOW`, `CHALLENGE_DEPOSIT`, `FORCE_PREPAID`, `BLOCK`).
  * *Given* a valid checkout payload, *When* the risk engine receives the request, *Then* it must respond within 50ms with a scored decision and top 3 SHAP attribution factors.
* **Story 1.2 (Dynamic Deposit Step-Up):** As a merchant, I want medium-risk buyers ($30 \le \text{Score} < 70$) to be offered a dynamic ₹49 refundable delivery deposit instead of a hard COD block.
  * *Given* an order scored as `CHALLENGE_DEPOSIT`, *When* rendered at checkout, *Then* present an instant UPI micro-intent for ₹49 which credits towards the total order upon delivery.
* **Story 1.3 (Atomic Edge Velocity Defense):** As a risk engine, I must intercept sub-second multi-tab burst orders from the same device/address before streaming pipelines finish updating.
  * *Given* a burst of ≥ 3 checkout attempts within 10 seconds sharing an H3 spatial index or device hash, *When* evaluated by the online gateway, *Then* atomically bump the local Redis sliding-window counter and force a step-up challenge regardless of current baseline score.

### Epic 2: Syndicate & Abuse-Ring Graph Sentinel
* **Story 2.1 (Entity Resolution & Linkage):** As an offline graph worker, I need to continuously link accounts sharing fuzzy addresses, device fingerprints, payment VPAs, or IP subnets.
  * *Given* new order and cancellation events from Kafka, *When* processed by the graph worker, *Then* create edges between entities and compute Louvain community clusters.
* **Story 2.2 (Ring Risk Feature Propagation):** As an online model, I need access to precomputed syndicate metrics in Redis.
  * *Given* a user belonging to a known high-RTO cluster, *When* querying Redis via `MGET`, *Then* return `cluster_rto_rate`, `cluster_size`, and `cluster_velocity_24h` in ≤ 5 ms.

### Epic 3: Spatial Intelligence & High-Density Entity Disambiguation
* **Story 3.1 (H3 Hexagonal Spatial Indexing):** As an address ingestion pipeline, I must convert unstructured Indian addresses into Uber H3 spatial resolutions (Res 8: ~460m, Res 9: ~100m).
  * *Given* a delivery address string, *When* parsed through normalization and spatial indexing, *Then* output an immutable `h3_index_res9` token.
* **Story 3.2 (High-Density Multi-Signal Verification):** As a risk scorer, I must never flag an entire high-rise building based solely on spatial proximity.
  * *Given* a high-density H3 index with historical RTOs, *When* scoring an incoming order, *Then* only escalate risk if the order *also* matches a secondary biometric or device hash, or fuzzy recipient name.

### Epic 4: Merchant Risk Console & Cost Optimization Curve
* **Story 4.1 (Dynamic Loss Simulator):** As a merchant finance manager, I want an interactive curve showing how moving the risk threshold changes logistics savings vs. lost revenue margins.
  * *Given* historical order data, *When* adjusting the risk slider in the dashboard, *Then* render real-time projections of net saved capital.

---

## 4. Non-Functional Requirements (NFRs)
* **Performance:** Online inference P99 latency ≤ 50 ms; P95 ≤ 30 ms at 2,500 requests per second (RPS).
* **Availability & Reliability:** 99.99% uptime for the online scoring API. Graceful degradation to heuristic fallback rules if Redis or the ML inference worker fails.
* **Data Privacy & Security:** Compliance with the India Digital Personal Data Protection (DPDP) Act 2023. PII (phone numbers, raw delivery addresses, names) must be salted and hashed (HMAC-SHA-256) before graph persistence.
* **Defense-Only Verification:** Zero capabilities for credential spraying, proxy generation, or checkout-bypass automation.

---

## 5. Out of Scope (Phase 1)
* Post-settlement card-not-present (CNP) chargeback arbitration filings with card networks (Visa/Mastercard representment).
* Physical delivery route optimization for logistics 3PL couriers.
* Deep biometric facial recognition verification at doorstep.