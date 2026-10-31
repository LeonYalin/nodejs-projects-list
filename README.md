# nodejs-projects-list
This repo contains a list of nodejs projects that should be done in order to become nodejs expert.

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

### 3. Driver Dispatch & Geo Queries (PostgreSQL Deep Dive + PostGIS)
*   **The Goal:** Master PostgreSQL essentials — constraints, index selection proven with `EXPLAIN ANALYZE`, transactions, isolation and row locking, CTEs and window functions, connection pooling from Node, and zero-downtime schema migrations — with PostGIS location data as the domain.
*   **Production Challenge:** Ingesting a constant stream of driver location pings while answering "nearest available drivers" in milliseconds, never assigning one driver to two passengers, and evolving the schema of a populated table without downtime.
*   **Tech Stack & Libraries:**
    *   *Database:* PostgreSQL with the PostGIS extension (`postgis/postgis` image).
    *   *Node Libraries:* `pg` (raw SQL, no ORM), `node-pg-migrate`, `fastify`, `zod`.
*   **Local Setup & Simulation Plan:**
    *   Run PostGIS via Docker with a small `drivers` / `rides` / `zones` schema whose rules live in constraints — including a partial unique index allowing one active ride per driver. A script streams location pings, upserted with `ON CONFLICT`.
    *   Find nearest drivers with `ST_DWithin` and the `<->` KNN operator over a GiST index, and justify every index (B-tree, GiST, GIN on a small jsonb column) with `EXPLAIN ANALYZE`. Use keyset pagination for ride history, and CTEs, window functions and `ST_Contains` to rank zones by live demand.
    *   Reproduce two passengers grabbing the same driver, then fix it with a transaction and `FOR UPDATE SKIP LOCKED` on a single checked-out pool client, comparing isolation levels along the way.
    *   **Migrations:** manage the schema with `node-pg-migrate`, and look at how it tracks applied migrations in its own table and uses an advisory lock to stop two instances migrating at once. Move `lat`/`lng` floats to a PostGIS `geography` column on the live table with expand/contract: add column, dual-write, batched backfill, `CREATE INDEX CONCURRENTLY`, switch reads, drop the old columns.

### 4. Uptime Monitor & Status Page on Kubernetes with GitOps (GitHub Actions + Helm + Terraform + Argo CD)
*   **The Goal:** Master how a Node service gets from `git push` to a running cluster — container images, CI quality gates, core Kubernetes objects, Helm packaging, Infrastructure as Code, and GitOps, where git is the single source of truth for what runs — and the Node code that makes a service deployable: health checks, graceful shutdown, config from the environment.
*   **Production Challenge:** Shipping changes repeatably and without downtime: no manual `kubectl`, config and secrets per environment, schema migrations that don't break old pods still running mid-rollout, and rollbacks and self-healing that need no human — proven by the app itself, which monitors its own deployment.
*   **Tech Stack & Tools:**
    *   *App:* an uptime monitor — a Fastify **API** (register monitored URLs, uptime %, response times, incidents, a small public status page) and a **worker** that runs due HTTP checks with timeouts and records the results; PostgreSQL, `pg`, `node-pg-migrate`, `zod`.
    *   *CI & Registry:* GitHub Actions, GitHub Container Registry (`ghcr.io`).
    *   *Cluster & IaC:* k3d (Kubernetes in Docker), Terraform (`kubernetes` + `helm` providers), Helm, Argo CD.
*   **Local Setup & Simulation Plan:**
    *   **Make the app deployable:** config only from env vars validated with `zod`; `/livez` (liveness: the process is alive) separate from `/readyz` (readiness: the database is reachable); `SIGTERM` handling that drains in-flight requests in the API and lets the worker finish its in-flight checks; a `/version` endpoint to see which build serves. Two worker replicas claim due monitors with `FOR UPDATE SKIP LOCKED`, so no monitor is checked twice.
    *   **CI:** on every push, lint and test, build multi-stage images for both services, push them as private images to `ghcr.io` tagged with the commit SHA, and bump the tag in the deployment manifests in git. The cluster pulls them with an `imagePullSecret`.
    *   **Terraform** owns the cluster-level setup — `dev` and `staging` namespaces, secrets, an ingress controller, and Argo CD itself — teaching state, `plan` vs `apply`, and drift.
    *   **Helm chart:** API Deployment (2 replicas, Service, Ingress, probes), worker Deployment (2 replicas, no Service), ConfigMap and Secret, resource limits, and Postgres as a StatefulSet with a persistent volume; per-environment values files.
    *   **GitOps & safe rollouts:** Argo CD syncs the cluster from git — a manual `kubectl edit` gets reverted, `git revert` is the rollback. Migrations run as a Helm `pre-upgrade` hook Job: rename `checks.latency` to `response_time_ms` in one step and watch old pods fail mid-rollout, then redo it with expand/contract. Point a monitor at the app's own Ingress: rolling updates, rollbacks and `kubectl delete pod` must leave its status page at 100% uptime.

### 5. Serverless Newsletter Platform on AWS with Staging & Production (Lambda + API Gateway + DynamoDB + SQS + SES + Terraform)
*   **The Goal:** Get hands-on with the most popular AWS serverless services — functions, HTTP APIs, DynamoDB, queues, scheduling, email and static hosting — and ship them the way companies do: separate staging and production accounts and domains (`staging.<domain>.com` / `<domain>.com`), Infrastructure as Code, a pipeline where production is protected, and the ability to freeze the whole thing to exactly $0 and rebuild it later. Breadth over depth: each service is used for one clear job.
*   **Production Challenge:** Sending a newsletter reliably: double opt-in, a scheduled send that fires when planned, and fan-out delivery that survives failures without ever emailing anyone twice. It also deals with serverless realities (cold starts, at-least-once delivery, no background work after a function returns), keeps staging fully isolated from production, and makes sure nothing is billed by accident.
*   **Tech Stack & Libraries:**
    *   *AWS:* Lambda (Node.js), API Gateway (HTTP API), DynamoDB, SQS with a dead-letter queue, EventBridge Scheduler, SES, S3 + CloudFront, Route 53 (domain + DNS), ACM, SSM Parameter Store, CloudWatch, IAM, Organizations + IAM Identity Center, Budgets.
    *   *IaC & CI:* Terraform, GitHub Actions with OIDC (no stored AWS keys), in a public repo so environments can require approval.
    *   *Node Libraries:* AWS SDK v3, `zod`, `esbuild`.
*   **Local Setup & Simulation Plan:**
    *   **Account & cost guardrails:** reuse an existing AWS account as the Organization's management account (no workloads in it). Before building, audit it clean: the last bills show $0.00, no leftover resources in any region, MFA on root, old access keys deleted. AWS Budgets alerts at $1 and $5, low API Gateway throttling limits, and nothing in the design bills while idle (no VPC, NAT Gateway, load balancers, customer-managed KMS keys or Secrets Manager). Handlers are developed as plain functions against DynamoDB Local in Docker.
    *   **Subscribe flow:** a static sign-up page on S3 + CloudFront posts through API Gateway to a Lambda that stores a pending subscriber in DynamoDB. SES emails a signed confirmation link, and every newsletter carries a signed unsubscribe link.
    *   **Sending:** publishing an issue creates a one-time EventBridge schedule. At send time, a Lambda enqueues the active subscribers to SQS in batches, and a sender Lambda with limited concurrency delivers each email through SES. A conditional DynamoDB write per (issue, subscriber) makes redeliveries harmless, and repeated failures land in the dead-letter queue. Bundle with `esbuild`, reuse SDK clients across invocations, load config from SSM, and alarm on dead-letter queue depth.
    *   **Environments:** `staging` and `production` member accounts created from the management account. The domain is registered once in Route 53 Domains (auto-renew off). Production owns the `<domain>.com` hosted zone and delegates `staging.<domain>.com` to a zone in the staging account. Each environment gets its own site, `api.` subdomain, ACM certificates (the CloudFront one in `us-east-1`), SES sender and Terraform state. Staging is password-protected by a CloudFront Function, marked `noindex`, and keeps SES in sandbox so it can never email real people.
    *   **Pipeline:** one GitHub OIDC role per account. Staging deploys from `main`, and the production role only trusts the approval-gated GitHub `production` environment. GitHub Actions builds once, deploys to staging, runs a smoke test with SES's mailbox simulator, then promotes the same build to production after approval. Rollback means pointing a Lambda alias back to the previous version.
    *   **Freeze & unfreeze to exactly $0:** a `bootstrap` Terraform root (local state) creates the state buckets and OIDC roles; everything else lives in per-environment stacks. Freezing destroys production, staging and then `bootstrap`, leaving only free things: the empty accounts, the Organization, Identity Center and the Budgets alerts. Resources the app creates at runtime are covered too: Lambda log groups are declared in Terraform, the app's schedules live in a Terraform-owned schedule group, and buckets use `force_destroy`. Every resource is tagged, a `verify-frozen` script fails if anything is left, and the next bill must show $0.00. Unfreezing applies `bootstrap`, staging and production, then reseeds demo data; Terraform repoints the domain at the new hosted zone automatically.
    *   *Out of scope on purpose:* bounce and complaint handling, canary deployments, and performance tuning.

### 6. Personal Document Assistant — Chat With Your Own Files (Ollama + pgvector + WebSockets + grammY)
*   **The Goal:** Master the core of LLM application engineering — document extraction, chunking and embeddings, vector search, grounded answers with citations, a hand-run multi-step tool-use loop, long-document summarization, structured-output classification, streaming, conversation management, and measuring whether a change made answers better — exposed through a web chat and a Telegram bot.
*   **Production Challenge:** Letting a model work with private files on your disk: finding the right document, answering only from what it says, summarizing files bigger than the context window, never letting the model reach outside the folders you allowed, and keeping sensitive content off third-party channels unless you explicitly agree. All of this runs on a local model, so nothing leaves your machine by default.
*   **Tech Stack & Libraries:**
    *   *Model & Vector Store:* Ollama (a local open model with tool calling, plus an embedding model), PostgreSQL with the **pgvector** extension.
    *   *Node Libraries:* `ollama`, `unpdf`, `mammoth`, `pg`, `ws`, `grammy`, `fastify`, `zod`.
    *   *UI:* a small React (Vite) ChatGPT-style chat page.
*   **Local Setup & Simulation Plan:**
    *   **Files & indexing:** Docker mounts only allowlisted folders (e.g. `~/Downloads`, `~/Documents`) read-only. The indexer extracts text from PDF, DOCX and Markdown files up to a size limit, splits it into overlapping chunks, embeds them, and stores each vector with its file, page and position behind an HNSW index, skipping unchanged files by content hash. It also labels each document's sensitivity (medical, financial, identity, none) with a structured-output classification validated by `zod`.
    *   **Tool-use loop:** the model gets read-only tools — `list_files`, `read_document`, `search_documents` — and you run the multi-step loop yourself with no framework. Every path is resolved with `realpath` and rejected unless it's inside an allowed root. `read_document` summarizes files too big for the context window chunk by chunk, then combines the partial summaries. Answers cite file and page, and say "I don't know" when the documents don't cover the question.
    *   **Web chat:** a conversation sidebar, token streaming over **WebSockets**, clickable citations, and a Stop button that aborts the model request with an `AbortController`.
    *   **Conversations:** store conversations and messages in PostgreSQL, send multi-turn history so follow-ups work, keep it inside the context window (set with `num_ctx`) by folding old turns into a running summary, and restore everything after a reload.
    *   **Telegram bot:** the same assistant core through `grammy` with long polling, answering only your own Telegram user ID. It streams by editing the reply in place, throttled to Telegram's rate limits. Telegram chats aren't end-to-end encrypted, so an answer built from a sensitive document is held until you confirm with an inline button (showing only the category, never content or file names) or choose to open it in the web chat instead.
    *   **Evals:** a fixture folder of sample documents plus ~20 questions tagged with the file that answers them, scored on retrieval hit rate@K, so tuning is measured on test data instead of your real files.

### 7. E-Commerce Product Search & Centralized Logging (ELK Stack)
*   **The Goal:** Master Elasticsearch's two canonical uses — full-text search (mappings, analyzers, relevance tuning, aggregations, zero-downtime mapping changes) and centralized logging (structured JSON logs from Node shipped through Filebeat and Logstash into Kibana).
*   **Production Challenge:** Serving fast, typo-tolerant, partial-term search with facets whose relevance stays sensible as the catalog grows and whose mappings change without downtime — and turning application logs into queryable data (latency, errors, searches that found nothing) instead of plain strings.
*   **Tech Stack & Libraries:**
    *   *Search & Logging Stack:* Elasticsearch, Logstash, Kibana, Filebeat.
    *   *Node Libraries:* `@elastic/elasticsearch`, `pino`, `fastify`, `zod`.
*   **Local Setup & Simulation Plan:**
    *   Run single-node Elasticsearch, Kibana, Logstash and Filebeat via Docker, and seed 50,000 products from a fixture file with the Bulk API helper — no relational store.
    *   **Index design:** hand-written mapping (`text` vs `keyword`), an `edge_ngram` analyzer for autocomplete and a stemming + synonyms analyzer for full search — inspecting the tokens each produces with the `_analyze` API.
    *   **Search API:** `multi_match` with fuzziness, per-field boosts and `function_score` for popularity; exact filters in filter context; faceted aggregations; `search_after` for deep pagination; and `_explain` to tune "iphnoe" → "iPhone" deliberately rather than by accident.
    *   **Zero-downtime mapping change:** the app reads and writes through an alias; add synonyms, reindex into a new index, and swap the alias atomically.
    *   **Logging:** `pino` JSON logs with a request ID and secret redaction → Filebeat reads the container logs → Logstash parses, enriches and drops health-check noise → a data stream with an ILM retention policy. A Kibana dashboard shows search latency, error rate and zero-result searches.

### 8. High-Frequency Market Data Engine (Streams, Worker Threads & Node Internals)
*   **The Goal:** Master Node's in-process concurrency and data primitives — hand-written streams and backpressure, binary data with `Buffer`, `worker_threads` with `SharedArrayBuffer` and `Atomics`, `EventEmitter` and `AsyncLocalStorage` — and learn to *prove* the event loop stays free and to find hot spots and memory leaks with Node's built-in tools.
*   **Production Challenge:** A market feed delivers ~100,000 binary price ticks per second. The service must compute rolling per-symbol statistics (moving average, VWAP) and fire alerts on sharp moves without blocking the event loop, without paying serialization cost on every hop between threads, and without memory growing over hours of uptime.
*   **Tech Stack & Libraries:**
    *   *Core:* Node.js native `stream`, `buffer`, `worker_threads`, `events`, `async_hooks` (`AsyncLocalStorage`), `perf_hooks` and `http` modules.
    *   *Node Libraries:* Nothing beyond the standard library — that is the point of this one.
*   **Local Setup & Simulation Plan:**
    *   **Feed & streams:** a generator writes fixed-layout binary ticks (symbol id, price, volume, timestamp) into a `Readable`. Hand-written `Transform` classes decode them — including ticks split across chunk boundaries — and batch them, composed with `pipeline()` and cancelled via `AbortSignal`. Under-provision a consumer to watch backpressure engage, and tune `highWaterMark` to see it move.
    *   **Worker pool:** a hand-written pool of `worker_threads`, each owning a shard of symbols. Rolling windows live in ring buffers inside a `SharedArrayBuffer` coordinated with `Atomics`. Benchmark three ways of passing data: `postMessage` structured cloning, transferring an `ArrayBuffer`, and shared memory.
    *   **Decoupling:** an internal `EventEmitter` bus delivers alerts and counters to separate listeners (learning the `'error'` event and max-listener leak warnings), and `AsyncLocalStorage` carries a batch correlation ID through every async step into the logs without threading it through function signatures.
    *   **Prove it:** a native `http` status endpoint reports live stats and event-loop delay from `perf_hooks.monitorEventLoopDelay` — compare the math on the main thread vs in workers. Find the hot path with `--cpu-prof`, then plant a leak (a listener that is never removed) and track it down with a heap snapshot in Chrome DevTools.

### 9. Order & Payment Microservices with Transactional Outbox (tRPC + Stripe + Redis Streams)
*   **The Goal:** Master service decomposition and the two ways services talk — synchronous typed RPC and asynchronous events — with the Transactional Outbox guaranteeing that a state change and the event announcing it can never diverge, and idempotency making retries safe.
*   **Production Challenge:** The dual-write problem: a database commit succeeds but the event publish fails, and the rest of the system never learns what happened. Compounded by payment providers that retry webhooks aggressively, so every consumer must be idempotent or duplicates become real money.
*   **Tech Stack & Libraries:**
    *   *Services, DB & Broker:* two services — **orders** and **payments** — in a pnpm-workspace monorepo; PostgreSQL (a schema per service), Redis Streams as the event broker, Stripe (test mode) with the Stripe CLI forwarding webhooks.
    *   *Node Libraries:* `@trpc/server` + `@trpc/client`, `stripe`, `pg`, `ioredis`, `fastify`, `zod`.
*   **Local Setup & Simulation Plan:**
    *   **The synchronous path (tRPC):** orders reserves stock and calls payments over tRPC to create a Stripe PaymentIntent, fully typed by importing the payments router's type — no generated client. Kill payments and watch order creation fail; add timeouts and decide what the caller does when payments is down.
    *   **The asynchronous path (outbox):** payments verifies the Stripe webhook signature and, in **one** PostgreSQL transaction, updates the payment and inserts an `outbox` row. A relay claims rows with `FOR UPDATE SKIP LOCKED` (safe with several replicas) and publishes them to a Redis Stream with exponential backoff and jitter. Stop Redis, keep paying, restart it, and confirm no event was lost.
    *   **Idempotency:** orders consumes through a Redis consumer group (`XACK`, and the pending-entries list to recover messages from crashed consumers). Dedupe at every layer — Stripe event IDs on the webhook, an inbox table on the consumer, an `Idempotency-Key` header on order creation — then replay one signed webhook 50 times concurrently and confirm exactly one state change and one event.
    *   **Compensation (saga):** when a payment succeeds after the order's stock reservation has expired, orders publishes a compensation event and payments refunds through Stripe.

### 10. Identity Provider with Token Rotation & RBAC (Apache Cassandra + JWT)
*   **The Goal:** Master auth essentials — password hashing, asymmetric JWTs with a JWKS endpoint and key rotation, rotating refresh tokens with reuse detection, and role-based access control — on top of Cassandra: query-first modelling, partition and clustering keys, denormalization, TTLs, lightweight transactions and tunable consistency.
*   **Production Challenge:** Many services must verify tokens without calling the identity server on every request, a stolen refresh token must be detected and its whole session revoked at once, and keys, roles and sessions must stay correct across a distributed database where nodes can be down.
*   **Tech Stack & Libraries:**
    *   *Database:* Apache Cassandra (a 3-node cluster with small heaps).
    *   *Node Libraries:* `cassandra-driver`, `jsonwebtoken`, `bcrypt`, native `crypto`, `fastify`, `zod`.
*   **Local Setup & Simulation Plan:**
    *   **Data model:** run a 3-node Cassandra cluster via Docker with a keyspace at replication factor 3. Design one table per query (`users_by_email`, `refresh_tokens_by_id`, `sessions_by_user`, `roles_by_user_and_org`), denormalize instead of joining, and expire tokens with TTLs instead of deletes to avoid tombstones.
    *   **Access tokens:** generate RSA key pairs with `crypto`, sign 15-minute access tokens tagged with a `kid`, publish `/.well-known/jwks.json`, and rotate keys — the new key signs while the old one stays published until its tokens expire. A small separate resource API verifies tokens from the cached JWKS alone, never calling the identity server.
    *   **Refresh rotation & reuse detection:** refresh tokens are opaque and stored hashed. Each use issues a new token in the same family and marks the old one used with a lightweight transaction (`UPDATE ... IF used = false`); presenting a used token revokes the whole family. Integration tests simulate the theft, plus "log out everywhere".
    *   **RBAC:** roles per organization (admin in one, viewer in another) mapped to permissions, enforced by a `requirePermission('orders:write')` middleware. Compare permissions embedded in the token vs looked up per request — fast, but stale until the token expires.
    *   **Consistency:** compare `ONE` vs `QUORUM` for writes and reads, kill a node to see which operations still succeed, and reason about why revocation checks need `QUORUM`.
