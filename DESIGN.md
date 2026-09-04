# UI/UX & Design System Guidelines (DESIGN)
**Project:** SENTINEL-RTO  

---

## 1. Design Principles
1. **Financial Consequence Visibility:** Every risk decision displays the underlying financial trade-off (Logistics Cost vs. Lost Customer Lifetime Value).
2. **Cognitive Scannability:** High-density alerts leverage color-coded tiers (`Emerald-500`, `Amber-500`, `Rose-600`) with SHAP factor breakdowns.
3. **Friction Transparency:** Consumer-facing deposit prompts explain *why* a deposit is requested without using accusatory language.

---

## 2. Color Palette & Typography

| Color Token | Hex Code | Usage |
| :--- | :--- | :--- |
| `brand-primary` | `#0B5CFF` | Primary CTA, active tab highlights, Razorpay brand affinity |
| `risk-safe` | `#10B981` | Low Risk Tier (0 - 30), Allow COD, normal operation |
| `risk-warning` | `#F59E0B` | Medium Risk Tier (31 - 70), Partial Deposit Trigger |
| `risk-critical` | `#EF4444` | High Risk Tier (71 - 100), Prepaid Only, Syndicate Node |
| `surface-dark` | `#0F172A` | Background slate for Admin Risk Console (Dark Theme Default) |
| `surface-card` | `#1E293B` | Card container backgrounds, table panels |
| `text-primary` | `#F8FAFC` | Headers, critical metrics, high-contrast labels |
| `text-muted` | `#94A3B8` | Sub-labels, SHAP explanations, metadata timestamps |

* **Primary Font Family:** `Inter`, `-apple-system`, `sans-serif` (UI elements, metrics, body).
* **Monospace Font Family:** `JetBrains Mono`, `monospace` (H3 cell IDs, device hashes, latencies, raw logs).

---

## 3. Component Library Specs

### 1. Buyer-Facing Dynamic Deposit Modal (`DynamicDepositModal.tsx`)
* A compact overlay that opens if checkout returns `CHALLENGE_DEPOSIT`.
* Displays a clear, reassuring value proposition: *"Verify your Cash on Delivery order with an advance delivery deposit of ₹49. This will be deducted from your final bill."*
* Single-click UPI Intent button (GPay, PhonePe, Paytm).

### 2. Risk Evaluation Metric Card (`RiskMetricCard.tsx`)
* Top-level display on the Admin Console showing real-time decisions, average latency, and capital saved.
* Mini sparkline of P99 response times over the last 60 minutes.

### 3. Syndicate Graph Explorer (`GraphExplorer.tsx`)
* Canvas/WebGL interactive node-link visualization.
* Node color indicates entity type (Blue: Account, Green: Device, Orange: H3 Hexagon, Red: Known Scammer).
* Interactive drawer sliding out on node click with full historical order lineage.

---

## 4. Responsive Breakpoints

| Breakpoint | Viewport Width | Layout Adaptations |
| :--- | :--- | :--- |
| **Mobile (`sm`)** | `< 640px` | Single-column stack, full-width checkout modals, hidden graph visualizer. |
| **Tablet (`md`)** | `640px - 1024px` | 2-column dashboard layout, collapsed sidebar navigation. |
| **Desktop (`lg/xl`)** | `> 1024px` | Full 3-pane operations console with live WebGL graph inspector and split risk feed. |


## 5. Design Preference

Backgrounds

Avoid: Pure black, moody slate, radial orbs, dot grids, or liquid glass effects.

Include: Pure white (#FFFFFF) or controlled warm off-white (#FAFAFA) surfaces.

Color Palettes

Avoid: Harsh multi-color gradients, neon accents, rainbow coloring, purple-and-black themes, or basic pastel blocks.

Include: A single, restrained institutional accent color paired with high-contrast neutral slates and grays.

Layout & Geometry

Avoid: Symmetrical bento grids, standard 3-feature rows, soft bubbly corners, and heavy floating drop shadows.

Include: Flat or 1px bordered containers (border border-slate-200) with sharp, minimal corner radii (rounded or rounded-sm) and asymmetric, dense data layouts.

Icons & Decor

Avoid: Sparkle icons (✨), random emojis, and multicolored graphic containers.

Include: Monochrome, single-weight (1.5px stroke) functional icons.

Motion & Effects

Avoid: Floating cards, bouncy elements, and animated background rows.

Include: Micro-interactions restricted to subtle 1px border shifts or fast color state changes.

Content & Copywriting

Avoid: "It's not X, it's Y" copy tropes, fake founder testimonials, and abstract marketing placeholders.

Include: Direct, benefit-driven copy, real functional tables, code logs, and actual product data.

UI States & Pricing

Avoid: Blank loading states, generic spinners, and cookie-cutter 3-tier pricing tables.

Include: Monochromatic skeleton loaders and realistic pricing structures aligned strictly to core utility.