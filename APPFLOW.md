---

### `APPFLOW.md`

```markdown
# Application Flow & UX Journey (APPFLOW)
**Project:** SENTINEL-RTO  

---

## 1. User Journey Maps

### Journey A: End-Consumer Checkout Friction Resolution

[ Consumer Enters Checkout ]
|
v
[ Fills Address & Selects COD ]
|
v
[ JS SDK Captures Behavioral Telemetry (Duration, Canvas Hash) ]
|
v
[ API Gateway Evaluates Risk (< 50ms) ]
|
+---------------------------------------------------+
|                                                   |
[ Risk Score <= 30 ]                                [ Risk Score 31 - 70 ]
|                                                   |
v                                                   v
( Allow Instant 1-Click COD )                    ( Render ₹49 Deposit Modal )
|                                                   |
v                                                   +-----------------------+
[ Order Dispatched ]                                          |                       |
[ Pays ₹49 UPI ]        [ Rejects Modal ]
|                       |
v                       v
[ Order Placed (COD-49) ]   [ Return to Cart / Cancel ]


### Journey B: Platform Risk Lead / Merchant Admin Workflow

[ Login via MFA ] ---> [ Live Risk Operations Dashboard ]
|
+---> [ Inspect Live Threat Map (H3 Clusters) ]
|
+---> [ Explore Syndicate Graph Explorer ]
|        |
|        +--> Select Node (Device/Address/VPA)
|        +--> View Connected Burner Accounts
|        +--> Trigger Global Block / Blacklist
|
+---> [ Run What-If GMV/Loss Curve Optimizer ]
|
+--> Adjust Threshold Slider (e.g., 60 -> 72)
+--> Review Projected Net Savings & FP Cost


---

## 2. State Transition Logic

           +--------------------+
           |     INITIATED      |
           +--------------------+
                     |
                     v
           +--------------------+
           |     EVALUATING     |
           +--------------------+
                     |
     +---------------+---------------+
     |                               |
     v                               v

+------------------+           +-------------------+
|   APPROVED_COD   |           | DEPOSIT_REQUESTED |
+------------------+           +-------------------+
|                               |
|                   +-----------+-----------+
|                   |                       |
|                   v                       v
|          +-----------------+    +-------------------+
|          |  DEPOSIT_PAID   |    | DEPOSIT_ABANDONED |
|          +-----------------+    +-------------------+
|                   |                       |
+---------->+<------+                       v
|                      +-----------------+
v                      |    CANCELLED    |
+-----------------+             +-----------------+
|   DISPATCHED    |
+-----------------+
|
+-----------+-----------+
|                       |
v                       v
+-----------------+     +-----------------+
|    DELIVERED    |     |  RTO_RETURNED   |
+-----------------+     +-----------------+
|                       |
v                       v
[ Positive Reward Log ] [ Negative Edge Added to Graph ]


---

## 3. Edge Cases & Fallback Protocols

| Edge Case Event | System Reaction | Fallback Behavior |
| :--- | :--- | :--- |
| **Redis Cluster Timeout (> 15ms)** | Truncates feature fetch pipeline immediately. | Evaluates lightweight static rules (e.g., Cart Value > ₹3000 $\rightarrow$ Challenge). |
| **Ambiguous / Unparseable Address** | Sets `h3_index_res9` to Pin-Code Centroid. | Falls back to Pincode baseline RTO average without granular H3 penalty. |
| **Rapid Sub-Second Multi-Tab Burst** | Edge sliding-window counter in local RAM increments atomically. | Forces `FORCE_PREPAID` on 3rd order; emits high-priority Syndicate Alert. |
| **Anti-Detect Fingerprint Randomizer** | Canvas entropy anomaly detected (`canvas_entropy < 0.2`). | Flags device as high-risk proxy; enforces step-up challenge. |