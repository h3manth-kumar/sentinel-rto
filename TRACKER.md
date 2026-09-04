# Task Breakdown & Progress Matrix (TRACKER)
**Project:** SENTINEL-RTO  

---

## 1. Backlog Table

| Task ID | Epic | Title | Priority | Status | Assignee Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TSK-001** | Epic 1 | Generate synthetic e-commerce data with embedded fraud rings & RTO patterns | P0 | Done | Data Scientist |
| **TSK-002** | Epic 1 | Train LightGBM model & export to ONNX runtime format with SHAP tree-explainer | P0 | Done | ML Engineer |
| **TSK-003** | Epic 3 | Implement address parser & Uber H3 spatial coordinate resolution service | P0 | In Progress | Backend Engineer |
| **TSK-004** | Epic 2 | Build NetworkX Louvain community graph clustering worker for entity linkage | P1 | In Progress | Data Engineer |
| **TSK-005** | Epic 1 | Develop FastAPI online scoring gateway with Redis MGET feature store pipe | P0 | In Progress | Systems Engineer |
| **TSK-006** | Epic 1 | Implement Redis ZSET sliding-window atomic burst-order rate limiter | P0 | Backlog | Systems Engineer |
| **TSK-007** | Epic 4 | Build Next.js Dashboard with live risk feed and financial cost-curve optimizer | P1 | Backlog | Frontend Lead |
| **TSK-008** | Epic 2 | Integrate Vis.js/React-Flow interactive syndicate ring graph visualizer | P2 | Backlog | Frontend Lead |
| **TSK-009** | Epic 1 | Conduct Locust load testing to validate P99 < 50ms latency at 2,500 RPS | P0 | Backlog | QA / DevOps |

---

## 2. Sprint / Milestone Allocation

Sprint 1 (Days 1 - 4): Foundations & Graph Data Pipeline
└── TSK-001 [Done] -> TSK-002 [Done] -> TSK-003 [In Progress] -> TSK-004 [In Progress]

Sprint 2 (Days 5 - 8): Real-Time Online Gateway, Console & Benchmarks
└── TSK-005 [In Progress] -> TSK-006 [Backlog] -> TSK-007 [Backlog] -> TSK-008 [Backlog] -> TSK-009 [Backlog]