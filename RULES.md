# Development Standards & Governance (RULES)
**Project:** SENTINEL-RTO  

---

## 1. Coding Standards & Linting

### Python Backend
* Strict PEP 8 compliance enforced via `ruff` and formatted with `black` (line-length: 100).
* Strict static typing enforced with `mypy --strict`. Dynamic `Any` types are rejected in PR review.
* All I/O operations must be non-blocking (`async`/`await`) using native async drivers (`asyncpg`, `redis.asyncio`).

### TypeScript / Frontend
* Next.js 14 App Router standards with strict TypeScript mode (`strict: true`).
* Functional components with explicit prop interfaces; zero inline CSS (Tailwind classes only).

---

## 2. Git Workflow & Branching Strategy

* **Trunk-Based Development Workflow:**
  * Feature branches: `feat/feature-name`
  * Bugfixes: `fix/issue-description`
  * Performance optimizations: `perf/benchmark-target`
* **Conventional Commits Format:**
  * `feat(risk-engine): add onnx runtime inference session pool`
  * `perf(redis): pipeline entity feature lookups with MGET`
  * `fix(h3): handle null lat/lng fallback to pincode centroid`
* **PR Acceptance Criteria:**
  1. Minimum 1 peer code review approval.
  2. 100% CI pipeline pass (Lint + Types + Unit Tests).
  3. No latency regression on the synthetic inference benchmark suite ($\le 50\text{ ms}$).

---

## 3. Testing Standards & Quality Gates

* **Unit Testing:** Minimum 85% code coverage on core risk scoring, spatial transformation, and feature calculation modules (`pytest`).
* **Integration Testing:** End-to-end integration tests validating Kafka message consumption, graph clustering, and Redis feature synchronization.
* **Performance & Load SLA Testing:** Every release candidate must pass an automated `k6` or `Locust` load test simulating 2,500 RPS over 10 minutes with zero dropped requests and P99 latency $\le 50\text{ ms}$.