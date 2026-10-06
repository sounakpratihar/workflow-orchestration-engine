# Enterprise Multi-Tenant Workflow Orchestration Engine

A high-performance, production-grade workflow orchestration platform featuring:
- **MongoDB Multi-Tenant Isolation Strategies**: Architectural blueprints, leak prevention plugins (Discriminator Keys), and Dynamic Multi-Database Connection Pooling with LRU eviction.
- **Visual DAG Workflow Canvas in React**: Interactive graph canvas with cubic bezier edges, node schema validation, conditional branching ports (`True`/`False`), real-time 3-color DFS cycle/loop detection, and step-by-step execution playback.
- **Express Multi-Tenant Middleware Suite**: Zero-leakage ambient context via Node.js `AsyncLocalStorage`, cryptographic API key management with constant-time verification (`crypto.timingSafeEqual`), and tenant-partitioned sliding-window rate limiting.

---

## 🏛️ 1. Multi-Tenant Database Isolation Strategies (MongoDB)

Detailed architecture and trade-offs are documented in [docs/ARCHITECTURE_MULTI_TENANCY.md](file:///c:/Users/Sounak%20Pratihar/Pictures/WEB%20DEV/workflow-orchestration-engine/docs/ARCHITECTURE_MULTI_TENANCY.md).

```
                              ┌────────────────────────────────────────┐
                              │           API Gateway / Ingress        │
                              └───────────────────┬────────────────────┘
                                                  │
                                                  ▼
                              ┌────────────────────────────────────────┐
                              │  Tenant Router & Tier Classification   │
                              └───────┬────────────────────────┬───────┘
                                      │                        │
                   Tier = "FREE" / "PRO"                       │ Tier = "ENTERPRISE"
                                      │                        │
                                      ▼                        ▼
                        ┌───────────────────────────┐    ┌───────────────────────────┐
                        │   Strategy A: Shared DB   │    │  Strategy B: Dedicated DB │
                        │  (Discriminator Scoped)   │    │  (Dynamic Multi-DB Pool)  │
                        └─────────────┬─────────────┘    └─────────────┬─────────────┘
                                      │                                │
                                      ▼                                ▼
                        ┌───────────────────────────┐    ┌───────────────────────────┐
                        │  MongoDB Cluster: Shared  │    │ MongoDB Cluster: Isolated │
                        │   db: "workflow_platform" │    │   db: "tenant_acme_corp"  │
                        │   collections: workflows, │    │   db: "tenant_global_bank"│
                        │   executions (tenantId)   │    │   (CMEK, Dedicated PITR)  │
                        └───────────────────────────┘    └───────────────────────────┘
```

### Strategy Comparison Matrix

| Evaluation Dimension | Strategy A: Discriminator Partitioning | Strategy B: Dynamic Multi-DB Connection Pools |
| :--- | :--- | :--- |
| **Data Isolation Level** | Logical only (shared collections) | Physical namespace isolation (`db.collection`) |
| **Blast Radius** | High (human coding bug could leak data) | Minimal (database boundary enforced by engine) |
| **Connection Pooling** | Single shared TCP socket pool | Multiplexed via `client.useDb(tenantDb, { useCache: true })` |
| **Cache Overhead** | Low (shared WiredTiger page cache) | High (metadata overhead per database at scale) |
| **Backup / PITR** | Complex (filtered export required) | Simple (native `mongodump --db` and point-in-time recovery) |
| **Compliance (GDPR)** | Bulk `deleteMany` (oplog bloat) | Atomic `db.dropDatabase()` |
| **Target Tenants** | Freemium, Pro, Self-serve SaaS | Enterprise, HIPAA, SOC-2, Fintech |

### Implementation Modules
- [src/database/tenantContext.js](file:///c:/Users/Sounak%20Pratihar/Pictures/WEB%20DEV/workflow-orchestration-engine/src/database/tenantContext.js): Ambient store powered by Node.js `AsyncLocalStorage`.
- [src/database/discriminatorStrategy.js](file:///c:/Users/Sounak%20Pratihar/Pictures/WEB%20DEV/workflow-orchestration-engine/src/database/discriminatorStrategy.js): Mongoose plugin automatically intercepting queries (`find`, `findOneAndUpdate`, `countDocuments`, `aggregate`, `save`) to inject `{ tenantId }`.
- [src/database/multiDbPoolStrategy.js](file:///c:/Users/Sounak%20Pratihar/Pictures/WEB%20DEV/workflow-orchestration-engine/src/database/multiDbPoolStrategy.js): Dynamic database switcher multiplexing collections over pooled sockets with LRU cache eviction and name sanitization.

---

## 🎨 2. Visual DAG Workflow Canvas (React & Standalone)

Located in [client/src/](file:///c:/Users/Sounak%20Pratihar/Pictures/WEB%20DEV/workflow-orchestration-engine/client/src/) and accessible directly in browser via [index.html](file:///c:/Users/Sounak%20Pratihar/Pictures/WEB%20DEV/workflow-orchestration-engine/index.html).

### Key Features
1. **Node Schema Validation**:
   - Webhook: Validates endpoint path.
   - Schedule: Checks 5-part cron syntax.
   - HTTP Request: Enforces `http://` or `https://` and method.
   - Condition: Evaluates boolean expressions.
   - Terminal: Prevents illegal outgoing connections.
2. **Conditional Branching (`If/Else`)**:
   - Conditional nodes render two distinct output ports: **True** (`T`, emerald green) and **False** (`F`, rose red).
   - Downstream branches reflect condition logic with color-coded cubic bezier curves.
3. **Loop & Cycle Detection**:
   - Uses the **DFS 3-Coloring Algorithm** (White, Gray, Black) to detect back-edges and extract the exact cyclic path.
   - Uses **Kahn's Algorithm** (in-degree BFS) for topological sort and schedule resolution.
   - Real-time visual feedback:
     - Glowing red pulsing edges and nodes.
     - Warning banner displaying the cycle path (e.g. `Step A → Step B → Step C → Step A`).
     - "Break Loop Edge" one-click resolver.
4. **Interactive Step-by-Step Execution Simulator**:
   - Real-time payload evaluation (e.g. `amount >= 500`).
   - Dynamic path execution highlighting active branches and dimming bypassed steps.

---

## 🛡️ 3. Express Multi-Tenant Middleware Suite

Located in [src/middleware/](file:///c:/Users/Sounak%20Pratihar/Pictures/WEB%20DEV/workflow-orchestration-engine/src/middleware/):

1. **Tenant Context Router** ([src/middleware/tenantRouter.js](file:///c:/Users/Sounak%20Pratihar/Pictures/WEB%20DEV/workflow-orchestration-engine/src/middleware/tenantRouter.js)):
   - Resolves tenant via Subdomain (`acme.api.domain.com`), Headers (`X-Tenant-ID`), or API key prefix.
   - Binds tenant context ambiently via `AsyncLocalStorage`.
2. **Cryptographic API Key Manager** ([src/middleware/cryptoApiKey.js](file:///c:/Users/Sounak%20Pratihar/Pictures/WEB%20DEV/workflow-orchestration-engine/src/middleware/cryptoApiKey.js)):
   - Key format: `wk_live_<tenantId>_<entropy>.<checksum>`.
   - Entropy: 32 bytes CSPRNG (`crypto.randomBytes`).
   - Storage: Salted HMAC SHA-256 hash (never stored in plaintext).
   - Verification: Constant-time comparison (`crypto.timingSafeEqual`) to prevent timing side-channel attacks.
   - Granular OAuth-style scopes (`workflows:read`, `workflows:write`, `workflows:execute`).
3. **Tenant Sliding-Window Rate Limiter** ([src/middleware/rateLimiter.js](file:///c:/Users/Sounak%20Pratihar/Pictures/WEB%20DEV/workflow-orchestration-engine/src/middleware/rateLimiter.js)):
   - Per-tenant sliding log counter.
   - Tier quotas: Free (60 req/min), Pro (600 req/min), Enterprise (3000 req/min).
   - Standard HTTP headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After`.
   - Strict tenant isolation: Quota exhaustion by Tenant A never impacts Tenant B.

---

## 🚀 Quick Start & Testing

### 1. Run Unit Tests (11/11 Passing)
```bash
npm test
```
Tests cover:
- Cycle detection (acyclic, feedback loop, diamond graphs).
- Topological sorting and parallel stage computation.
- Conditional branching simulation.
- Ambient tenant context and async propagation.
- Database name injection prevention and LRU eviction.
- Cryptographic key generation, constant-time verification, and forgery rejection.
- Tenant rate-limit isolation.

### 2. Start the Backend API Server
```bash
npm start
```
Starts Express server on `http://localhost:4000`:
- **Health Check**: `http://localhost:4000/health`
- **Tenant Metrics**: `http://localhost:4000/api/v1/tenant/metrics`
- **Visual DAG Canvas UI**: `http://localhost:4000/index.html`

### 3. Open the Visual DAG Canvas
Open `index.html` in your browser or navigate to `http://localhost:4000/` when the server is running.
- Try switching tenant tiers in the top navbar.
- Click **Cryptographic API Key** to generate a signed key.
- Click **Cycle Test Preset** to see real-time loop detection and glowing red back-edges.
- Click **Branching Preset** -> **Run Simulation** -> **Start Run** to step through execution.