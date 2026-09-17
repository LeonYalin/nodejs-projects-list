# nodejs-projects-list
This repo contains a list of nodejs projects that should be done in order to become nodejs expert.

# Phase 1: High-Performance Data Layers & Messaging

### 1. Enterprise Log ETL & Reporting Pipeline (Kafka + ClickHouse)
*   **Repo:** ✅ [nodejs-enterprise-etl-pipeline](https://github.com/LeonYalin/nodejs-enterprise-etl-pipeline)
*   **The Goal:** Master high-throughput data ingestion, batched transformations, and analytical write-optimization without blocking the event loop.
*   **Production Challenge:** Handling millions of real-time server records and performing instant aggregations without overwhelming relational databases or filling up Node memory.
*   **Tech Stack & Libraries:**
    *   *Broker/Database:* Apache Kafka (KRaft mode), ClickHouse (Columnar analytical database).
    *   *Node Libraries:* `kafkajs` (industry-standard Kafka client), `@clickhouse/client` (official ClickHouse driver), `zod` (runtime validation), `express` (reporting API).
*   **Local Setup & Simulation Plan:**
    *   Spin up Kafka and ClickHouse containers via Docker.
    *   Write a background script acting as a high-frequency Kafka Producer that blasts 10,000 mock log rows/sec into a topic.
    *   Your main Node application consumes messages in batches (`eachBatch`), validates them (invalid → Dead Letter Queue), buffers rows in memory, and executes optimized bulk column-inserts into ClickHouse — committing Kafka offsets only after a successful insert.
    *   A ClickHouse Materialized View pre-aggregates rows on insert, and an Express API serves reports from that summary table, with Prometheus + Grafana for observability.

### 2. Distributed Media Transcoding Pipeline (RabbitMQ + MinIO Object Storage)
*   **Repo:** ✅ [nodejs-media-transcoding-pipeline](https://github.com/LeonYalin/nodejs-media-transcoding-pipeline)
*   **The Goal:** Master asynchronous job queuing, heavy multi-core CPU background tasks, and S3-compatible local object storage.
*   **Production Challenge:** Offloading long-running file operations (resizing images, encoding video), securing uploads so tasks aren't lost on server crashes, and keeping heavy processing out of memory.
*   **Tech Stack & Libraries:**
    *   *Queue, Storage & State:* RabbitMQ (quorum queues), MinIO (Free, local S3-compatible cloud object storage container), Redis (job state + SSE progress).
    *   *Framework & Node Libraries:* Fastify, `amqplib`, `sharp`, `fluent-ffmpeg`, `@aws-sdk/client-s3` + `@aws-sdk/lib-storage` (Standard AWS SDK v3 to talk to MinIO), `ioredis`, `zod`.
    *   *System Tool:* A static `ffmpeg`/`ffprobe` build baked into the worker Docker image — deliberately never installed on the host.
*   **Local Setup & Simulation Plan:**
    *   Run RabbitMQ (with web management UI), MinIO, Redis and the workers via Docker, the workers scaled as compose replicas. Observability via Prometheus + Grafana and OpenTelemetry → Jaeger.
    *   The API streams each multipart upload straight into MinIO (never buffering it), publishes a persistent job on a **confirm channel**, and returns `202 Accepted` only once the broker has accepted it. A single durable quorum queue with manual `ack`/`nack` at `prefetch(1)` gives one job in flight per container, with a TTL delay queue for bounded retries via a Dead Letter Exchange and a terminal "parked" queue for corrupt files.
    *   Workers stream images through `sharp`, and fan video out into an **HLS ladder**: a fast `ffprobe` planning stage emits one job per rendition, rungs encode in parallel across replicas, and a redelivery-safe Redis barrier lets the last one finish the master playlist. A browser UI follows progress over SSE and plays the result with `hls.js`; a load script sustains ~1,000 uploads/sec at 100% `202` while queue depth grows and drains.

### 3. E-Commerce Search & Production Logging Platform (Full ELK Stack)
*   **The Goal:** Master Elasticsearch itself — mapping design, text analysis and tokenization, relevance scoring, aggregations, and the index lifecycle — plus structured JSON logging, the other canonical ELK use case.
*   **Production Challenge:** Serving high-concurrency text search that tolerates typos and partial terms, keeping relevance tuned as the catalog grows, and changing mappings with zero downtime — while shipping machine-queryable application logs instead of the plain strings most services emit.
*   **Tech Stack & Libraries:**
    *   *Search & Logging Stack:* Elasticsearch, Logstash, Kibana, Filebeat (The ELK Stack).
    *   *Node Libraries:* `@elastic/elasticsearch` (Official enterprise client), `pino` (structured JSON logging), `fastify`, `zod`.
*   **Local Setup & Simulation Plan:**
    *   Run Elasticsearch, Logstash, Kibana and Filebeat via Docker. No relational store — seed 50,000 mock catalog products from a local fixture file straight into Elasticsearch with the Bulk API.
    *   **Index design:** hand-write the mapping instead of relying on dynamic mapping. Build a custom analyzer chain (an `edge_ngram` analyzer for autocomplete, a separate one for full search) and learn where `keyword` beats `text` for filtering, sorting and aggregating.
    *   **Relevance:** expose a search endpoint building `multi_match` queries with explicit fuzziness, per-field boosting and `function_score`. Use the `_explain` API to understand *why* a document scored where it did, tuning "iphnoe" → "iPhone" deliberately rather than by accident. Layer faceted filtering on top with aggregations.
    *   **Lifecycle:** write through an alias rather than directly to an index, then perform a zero-downtime reindex behind that alias when the mapping changes.
    *   **Production logging:** emit structured JSON logs from the Node app — correlation ID per request, level-aware serializers, redaction of secrets — ship them Filebeat → Logstash → Elasticsearch, and build Kibana dashboards for search latency, error rates and slow queries.

### 4. High-Frequency Financial Ticker Engine (Streams & Worker Threads)
*   **The Goal:** Master Node's in-process concurrency and data primitives — `worker_threads`, `SharedArrayBuffer`, hand-written streams and backpressure, `EventEmitter` and `AsyncLocalStorage` — the parts of Node that containers and OS processes never teach you.
*   **Production Challenge:** Processing thousands of continuous data ticks per second and running math-heavy rolling aggregations without delaying the main event loop or paying serialization cost on every hop between threads.
*   **Tech Stack & Libraries:**
    *   *Core Engine:* Node.js native `worker_threads`, `stream`, `events`, `async_hooks` (`AsyncLocalStorage`) and `cluster` modules.
    *   *Node Libraries:* Nothing beyond the standard library — that is the point of this one.
*   **Local Setup & Simulation Plan:**
    *   Write a mock market data script that generates a massive local binary file or loops infinitely in memory to broadcast continuous, high-volume price ticks into a Node `Readable` stream.
    *   Build the pipeline out of **hand-written stream classes** — a `Transform` that decodes the binary tick format, another that batches — composed with `pipeline()`. Deliberately under-provision a consumer to watch backpressure engage, and tune `highWaterMark` to see it move.
    *   Distribute chunks into a pool of background Worker Threads, using `SharedArrayBuffer` and `Atomics` for the rolling-window buffer so threads read and write shared memory with zero serialization. Benchmark it against plain `postMessage` structured cloning to feel the difference.
    *   Use an internal `EventEmitter` bus to decouple tick ingestion from secondary reactions (alerts, counters, metric gathering), and `AsyncLocalStorage` to carry a correlation ID through the async pipeline without threading it through every function signature.
    *   *Out of scope on purpose:* profiling, metrics and dashboards — Projects 1 and 2 already covered observability.

---

# Phase 2: Resilience, Security, & API Gateways

### 5. Event-Driven Microservices & Transactional Outbox (Stripe + tRPC + PostgreSQL)
*   **The Goal:** Master service decomposition and the two ways services talk to each other — synchronous typed RPC and asynchronous events — with the Transactional Outbox pattern guaranteeing that a state change and the event announcing it can never diverge.
*   **Production Challenge:** The dual-write problem: a database commit succeeds but the broker publish fails, and the rest of the system never learns what happened. Compounded by external payment providers that retry callbacks aggressively, so every consumer must be idempotent or duplicates become real money.
*   **Tech Stack & Libraries:**
    *   *Services, DB & Broker:* PostgreSQL (a schema per service), RabbitMQ (or Redis Streams), Stripe (test mode).
    *   *Node Libraries:* `@trpc/server` + `@trpc/client` (typed synchronous service-to-service calls), `stripe`, `pg`, `zod`, `fastify`.
    *   *Supporting:* `ioredis` for a distributed lock — used only where the outbox work genuinely needs one, not as the headline.
*   **Local Setup & Simulation Plan:**
    *   Split the system into three small services — **orders**, **payments**, **notifications** — each owning its own schema, all run via Docker Compose.
    *   **The synchronous path (tRPC):** orders calls payments over tRPC for the operations that need an answer immediately, with end-to-end type inference and no hand-written client or schema file. Then learn where it breaks — kill the payments service and watch what a synchronous dependency does to your availability.
    *   **The asynchronous path (outbox):** ingest Stripe webhooks with signature verification. Inside a **single** PostgreSQL transaction, write the application state change *and* insert the event row into an `outbox` table. A separate relay worker polls that table and publishes to the broker, so a broker outage can never lose an event the database already committed.
    *   **Idempotency:** Stripe retries webhooks, so key every event by its Stripe event ID and make consumers dedupe on it. Replay the same webhook 50 times concurrently with `k6` and confirm exactly one state change and exactly one outbound event.
    *   **Failure handling:** exponential backoff with jitter in the relay, and a compensating action (saga) for the case where the payment captures but order fulfillment fails — the failure mode that makes distributed transactions interesting.

### 6. Whitelabel Dynamic API Gateway (Nginx Reverse Proxy + SSL/HTTPS Management)
*   **The Goal:** Master multi-tenant reverse proxy routing, SSL termination, and programmatic whitelabeling using Nginx.
*   **Production Challenge:** Routing B2B customers who map their custom domains (e.g., `://client.com`) to your server infrastructure, while enforcing dynamic routing, CORS policies, and local HTTPS certificate resolution.
*   **Tech Stack & Libraries:**
    *   *Core Engine:* Nginx (Reverse proxy), Node.js native `http`, `https`, and `tls` modules.
    *   *Tools:* `mkcert` (Local trusted SSL certificate tool via Homebrew) or OpenSSL.
*   **Local Setup & Simulation Plan:**
    *   Configure Nginx as a reverse proxy fronting a dynamic Node.js gateway application using local dummy domain configurations in your `/etc/hosts` file (e.g., `tenant-a.local`, `tenant-b.local`).
    *   Use `mkcert` to issue wildcard local SSL certificates to verify seamless HTTPS handshakes through Nginx.
    *   Your Node application inspects incoming `Host` headers to verify valid custom domains, applying whitelabel routing parameters dynamically and altering headers before completing responses.

### 7. Secure Identity Provider & RBAC (Apache Cassandra NoSQL + Token Rotation)
*   **The Goal:** Master stateless token authorization, asymmetric cryptography, deep session control, and hierarchical Role-Based Access Control (RBAC) schemas mapped to an ultra-high-scale wide-column database — query-first modelling, partition and clustering key design, denormalization instead of joins, and tunable consistency.
*   **Production Challenge:** Designing a distributed authentication server that processes millions of token validations, permission evaluations, and linear session footprints at immense scale without structural single points of data failure.
*   **Tech Stack & Libraries:**
    *   *Core Engine & DB:* Apache Cassandra (Wide-column NoSQL database).
    *   *Node Libraries:* `cassandra-driver`, `jsonwebtoken`, `bcrypt`, Node's native `crypto` module.
*   **Local Setup & Simulation Plan:**
    *   Run a single-node Apache Cassandra instance via Docker. Define a keyspace optimized to store user token histories alongside deeply nested corporate role and permission mappings — designing the tables around the queries you intend to run, since Cassandra will not join for you.
    *   Use Node's native `crypto` module to generate RSA public/private key pairs locally to handle cryptographic token signatures.
    *   Expose endpoints that evaluate dynamic middleware permissions (e.g., Admin vs Manager) against the wide-column Cassandra lookups. Write integration tests simulating token theft: if a user submits an older refresh token twice, your system must trigger an automatic security hook, invalidating all related tokens in that family branch within Cassandra immediately.

---

# Phase 3: Real-Time Systems, AI, & Infrastructure

### 8. AI Document Assistant — RAG Over Your Own Documents (pgvector + WebSockets)
*   **The Goal:** Master LLM application engineering end to end — chunking, embeddings, vector retrieval, grounded generation, tool calling, streaming transport, and measuring whether any of it actually works.
*   **Production Challenge:** AI features fail in ways ordinary backends don't: retrieval returns confident nonsense, token costs run away silently, providers rate-limit you mid-stream, and there is no obvious test that tells you whether a change made the system better or worse.
*   **Tech Stack & Libraries:**
    *   *Model & Vector Store:* Anthropic Claude API (or a local model via Ollama), PostgreSQL with the **pgvector** extension.
    *   *Node Libraries:* `@anthropic-ai/sdk`, `pg` with `pgvector`, `ws` (WebSocket token streaming), `fastify`, `zod`.
*   **Local Setup & Simulation Plan:**
    *   Run a `pgvector` container. Build the ingest pipeline: upload a document → split into overlapping chunks → embed each chunk → store the vector alongside its source metadata. Add an HNSW index and compare its recall and latency against an exact scan to understand what approximate search actually costs you.
    *   **Retrieval:** expose a query endpoint that embeds the question, pulls the top-K chunks, and builds a grounded prompt forcing the model to cite its sources — and to answer "I don't know" when retrieval comes back empty.
    *   **Streaming:** push tokens to the browser over **WebSockets** as they generate (deliberately not SSE — Project 2 already covered that), including mid-stream cancellation when the client disconnects.
    *   **Tool calling:** give the model a small set of real tools (a document search, a date lookup) and run the tool-use loop yourself rather than deferring it to a framework, so you understand the protocol.
    *   **Guardrails & evals:** enforce a per-conversation token budget, handle `429` rate limits with backoff, and write an **eval harness** — a fixture set of questions with known-correct source chunks, scored on retrieval hit rate — so you can prove a chunking or top-K change actually helped.

### 9. Cloud-Native Deployment Foundation & GitOps Pipeline (Kubernetes + Helm + Terraform + GitHub Actions)
*   **The Goal:** Master the core fundamentals of declarative Infrastructure as Code (IaC), container orchestration, template packaging, and automated Continuous Integration (CI) workflows.
*   **Production Challenge:** Eliminating manual infrastructure provisioning, cleanly injecting configuration secrets into distributed environments, ensuring isolated network communication between services, and building automated quality gates that safely test and package container images before deployment.
*   **Tech Stack & Tools:**
    *   *CI/CD Pipeline:* GitHub Actions, GitHub Container Registry (`ghcr.io`).
    *   *Orchestration & IaC:* Terraform (using local `kubernetes` and `helm` providers), Kubernetes (via local Minikube or Docker Desktop), Helm (Kubernetes package manager).
    *   *App Components:* Two minimal Node.js services (one gateway API, one background service) to act as the deployment assets.
*   **Local Setup & Simulation Plan:**
    *   **The GitHub Actions Gate:** Write a lean GitHub Actions workflow that automatically triggers on a git push. It executes basic linting/testing and, upon success, builds your application into a Docker image and pushes it securely to GitHub's built-in container registry.
    *   **The Terraform Infrastructure Setup:** Use **Terraform** locally *strictly* to manage your local cluster boundaries. Write configurations to declaratively spin up a dedicated Kubernetes namespace and configure cluster-wide environment variables without touching an external cloud account.
    *   **The Helm Packaging Phase:** Package your services into a custom **Helm Chart**. Define a fixed replication boundary (e.g., run exactly 2 pods for high availability), set explicit CPU/Memory resource constraints, and write native Kubernetes liveness and readiness health probes that point to your Node.js endpoints.
    *   **The Self-Healing Simulation:** Deploy your services onto your local Kubernetes cluster using your custom Helm chart. Open your terminal and manually "kill" a running pod using `kubectl delete pod`. Observe the cluster dashboard to watch Kubernetes instantly auto-heal by spinning up a fresh, configured replacement replica to maintain your system state.

### 10. Uber-Style Real-Time Delivery Matcher & Geo-Analytics Engine (PostgreSQL Deep Dive + PostGIS)
*   **The Goal:** Master PostgreSQL properly — index types and when each one applies, reading `EXPLAIN ANALYZE`, query planning, window functions and CTEs, partitioning, and transaction isolation levels — using specialized geospatial indexing (`GiST`) and high-performance spatial queries as the domain demanding enough to force the lesson.
*   **Production Challenge:** Safely storing massive histories of trips, calculating complex location-based matches (e.g., finding the 5 closest available drivers to a passenger instantly), and identifying high-demand areas (Surge Pricing zones) without destroying database CPU performance.
*   **Tech Stack & Libraries:**
    *   *Database:* PostgreSQL with the **PostGIS** extension enabled.
    *   *Node Libraries:* `pg` (Official Postgres client), `fastify`.
    *   *System Tool:* Docker (using the `postgis/postgis` image).
*   **Local Setup & Simulation Plan:**
    *   Spin up a PostGIS container via Docker. Seed the database with a clean relational schema: `drivers` (with current coordinates), `passengers`, and `rides` (storing start/end pickup geometry paths).
    *   **Geospatial Indexing:** Map latitude and longitude fields into official PostGIS geography points and bind a **GiST (Generalized Search Tree)** index to that column. Compare B-tree, GiST and GIN on the same data to learn which structure suits which query shape, verifying every choice with `EXPLAIN ANALYZE` rather than assumption.
    *   **The Uber-Style Match:** Expose an HTTP endpoint where a passenger requests a ride. Your Node app executes an optimized SQL query using `ST_DWithin` and the `<->` KNN distance operator to instantly find and lock the nearest 5 available drivers sorted by physical proximity within milliseconds — and you explore how `SELECT ... FOR UPDATE SKIP LOCKED` and the isolation level prevent two passengers claiming the same driver.
    *   **Dynamic Surge Analytics:** Store high-demand polygon shapes (e.g., concert venues or downtown grid coordinates) as `GEOMETRY` zones. Write a query using `ST_Contains` to detect if a passenger's pickup location falls inside a high-demand zone to dynamically apply a pricing multiplier, using window functions and CTEs to rank zones by live demand.
    *   Partition the `rides` history table by month and watch the planner prune partitions away. Then use `k6` to blast hundreds of concurrent driver location coordinate updates (`PING` updates) per second into the server, verifying that the database cleanly indexes the moving coordinates while serving passenger match requests without lag.
