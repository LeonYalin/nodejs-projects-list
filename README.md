# nodejs-projects-list
This repo contains a list of nodejs projects that should be done in order to become nodejs expert.

# Phase 1: High-Performance Data Layers & Messaging

### 1. Enterprise Log ETL & Reporting Pipeline (Kafka + ClickHouse)
*   **The Goal:** Master high-throughput data ingestion, streaming transformations, and analytical write-optimization without blocking the event loop.
*   **Production Challenge:** Handling millions of real-time server records and performing instant aggregations without overwhelming relational databases or filling up Node memory.
*   **Tech Stack & Libraries:**
    *   *Broker/Database:* Apache Kafka, ClickHouse (Columnar analytical database).
    *   *Node Libraries:* `kafkajs` (industry-standard Kafka client), `@clickhouse/client` (official ClickHouse driver).
*   **Local Setup & Simulation Plan:**
    *   Spin up Kafka and ClickHouse containers via Docker.
    *   Write a background script acting as a high-frequency Kafka Producer that blasts 10,000 mock log rows/sec into a topic.
    *   Your main Node application consumes messages in batches, transforms the shapes using streams, and executes optimized bulk column-inserts into ClickHouse.

### 2. Distributed Media Transcoding Pipeline (RabbitMQ + MinIO Object Storage)
*   **The Goal:** Master asynchronous job queuing, heavy multi-core CPU background tasks, and S3-compatible local object storage.
*   **Production Challenge:** Offloading long-running file operations (resizing images, encoding video), securing uploads so tasks aren't lost on server crashes, and keeping heavy processing out of memory.
*   **Tech Stack & Libraries:**
    *   *Queue & Storage:* RabbitMQ, MinIO (Free, local S3-compatible cloud object storage container).
    *   *Framework & Node Libraries:* Fastify or Express, `amqplib`, `sharp`, `fluent-ffmpeg`, `@aws-sdk/client-s3` (Standard AWS SDK v3 to talk to MinIO).
    *   *System Tool:* Native `ffmpeg` binary installed on macOS via Homebrew.
*   **Local Setup & Simulation Plan:**
    *   Run RabbitMQ (with web management UI) and MinIO via Docker.
    *   Configure a durable RabbitMQ queue with explicit manual acknowledgments (`ack`/`nack`) and a Dead Letter Exchange (DLX) for corrupt file drops.
    *   Use `autocannon` to fire concurrent file uploads at your HTTP API. The API instantly writes the raw file to MinIO, pushes a metadata job payload to RabbitMQ, and returns a `202 Accepted` status. Your background worker process pulls jobs one by one, downloads from MinIO, transcodes via `ffmpeg`, and uploads the final version back to MinIO.

### 3. E-Commerce Fuzzy Search & Autocomplete Engine (Elasticsearch)
*   **The Goal:** Master text tokenization, index structures, typo tolerance, and ultra-fast text matching.
*   **Production Challenge:** High-concurrency text searching across fields with typos and partial terms where standard databases fail at speed and relevance.
*   **Tech Stack & Libraries:**
    *   *Search Engine:* Elasticsearch or OpenSearch.
    *   *Node Libraries:* `@elastic/elasticsearch` (The official enterprise client), `dotenv`.
*   **Local Setup & Simulation Plan:**
    *   Run a single-node development container of Elasticsearch via Docker.
    *   Write an optimized batch seeding script using the Elasticsearch bulk API to push 50,000 mock catalog products into an index configured with an `edge_ngram` custom analyzer (essential for typing autocomplete).
    *   Expose a search HTTP endpoint that builds multi-field match queries with explicit fuzziness parameters, allowing search terms like "iphnoe" to cleanly match "iPhone".

### 4. High-Frequency Financial Ticker Engine (Streams & Worker Threads)
*   **The Goal:** Leverage underlying system hardware by breaking past the single-threaded nature of Node.js for CPU-heavy tasks.
*   **Production Challenge:** Processing thousands of continuous WebSocket price ticks per second and running math-heavy aggregations (moving averages) without delaying the main event loop.
*   **Tech Stack & Libraries:**
    *   *Core Engine:* Node.js native `worker_threads` and `stream` modules.
    *   *Node Libraries:* `ws` (The standard high-performance WebSocket engine).
*   **Local Setup & Simulation Plan:**
    *   Write a separate mock market script that opens a local WebSocket connection and loops infinitely to broadcast randomized price objects every few milliseconds.
    *   Your main application reads the continuous incoming stream, sends the arrays into a pool of background Node Worker Threads using `SharedArrayBuffer` to eliminate serialization lag, and handles calculation updates efficiently.

---

# Phase 2: Resilience, Security, & API Gateways

### 5. Resilient Webhook Ingestion Engine (Redis Distributed Locking + PostgreSQL)
*   **The Goal:** Implement strict protective measures against traffic spikes and ensure idempotency (preventing accidental double-processing) using a relational database layer.
*   **Production Challenge:** Securely handling external payment callbacks (like Stripe or PayPal) where network retries from the provider can accidentally trigger duplicate actions or state corruption.
*   **Tech Stack & Libraries:**
    *   *Cache & DB:* Redis, PostgreSQL.
    *   *Node Libraries:* `ioredis` (Robust driver supporting atomic Lua scripts), `pg` (Standard Postgres client), `uuid`.
*   **Local Setup & Simulation Plan:**
    *   Run Redis and PostgreSQL containers via Docker.
    *   Expose a webhook ingestion endpoint. Write defensive middleware that reads an event token and uses Redis's atomic `SET NX` command to acquire a short-lived distributed lock.
    *   Simulate load using `k6` to send identical payload batches simultaneously. Confirm that exactly one request triggers the PostgreSQL database state modification, while all duplicates are instantly dropped with a `429 Too Many Requests` error.

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

### 7. Secure OAuth2/OIDC Identity Provider (Apache Cassandra NoSQL + Token Rotation)
*   **The Goal:** Master stateless token authorization, asymmetric cryptography, and deep session control mapped to an ultra-high-scale wide-column database.
*   **Production Challenge:** Designing a distributed authentication server that processes token validations and linear session footprints at immense scale without single points of data failure.
*   **Tech Stack & Libraries:**
    *   *Core Engine & DB:* Apache Cassandra (Wide-column NoSQL database).
    *   *Node Libraries:* `cassandra-driver`, `jsonwebtoken`, `bcrypt`, Node's native `crypto` module.
*   **Local Setup & Simulation Plan:**
    *   Run a single-node Apache Cassandra instance via Docker. Define a keyspace tracking active token lineages using optimized primary/clustering keys.
    *   Use Node's native `crypto` module to generate RSA public/private key pairs locally to handle token signatures.
    *   Write integration tests simulating token theft: if a user submits an older refresh token twice, your system must trigger its automatic security hook, invalidating all related tokens in that family branch within Cassandra immediately.

---

# Phase 3: Real-Time Systems, Scheduling, & Fault Tolerance

### 8. Orchestrated Real-Time Activity Broker (Node EventEmitter + WebSockets + Redis Pub/Sub)
*   **The Goal:** Manage decoupled event architectures within a single process alongside cross-server horizontal socket state syncing.
*   **Production Challenge:** Keeping messaging logic memory-efficient, clean, and modular using event-driven architectures while syncing live notifications cleanly across disparate running nodes.
*   **Tech Stack & Libraries:**
    *   *Core Engine & Cache:* Node.js native `events` (EventEmitter) module, Redis (Pub/Sub system).
    *   *Node Libraries:* `socket.io` or pure `ws`.
*   **Local Setup & Simulation Plan:**
    *   Launch two instances of your Node web server on separate local ports (`4001` and `4002`) hooked to a local Redis container on port `6379`.
    *   Implement an internal central `EventEmitter` bus inside the Node application layer to safely decouple incoming socket payloads from secondary actions (e.g., analytics triggers, metric gathering).
    *   Connect to both ports using **wscat**. Prove that pushing a websocket message into port 4001 fires an internal EventEmitter event, passes out to Redis Pub/Sub, and makes the socket connected on port 4002 print the message instantly.

### 9. Automated Batch Scheduler (Distributed Crons + MongoDB)
*   **The Goal:** Implement automated periodic data aggregations that execute safely without doubling up tasks or breaking under deployment cycles.
*   **Production Challenge:** Running a cron job on a multi-instance production setup without having the job fire multiple times, and ensuring mid-process jobs exit cleanly during a container update.
*   **Tech Stack & Libraries:**
    *   *Database:* MongoDB.
    *   Node Libraries: `agenda` (highly popular MongoDB-backed cron manager) or `bullmq` (Redis-backed repeat scheduler), `pdfkit` (streaming PDF creator), `nodemailer`.
*   **Local Setup & Simulation Plan**:
    *   Run a database container alongside **Mailhog** (a free developer SMTP tool that intercepts outbound mail for local UI viewing).
    *   Seed your database with 5,000 mock user rows. Open three distinct terminal windows running the app simultaneously.
    *   Simulate high-availability coordination: trigger a heavy PDF billing report aggregation job and observe the logs to confirm only one instance claims the execution lock while others go idle. Force-quit (`SIGTERM`) a running job to confirm it recovers gracefully without dropping data.

### 10. Fault-Tolerant Distributed Asset Scraper (Kubernetes + Helm Deployment Lifecycle)
*   **The Goal**: Architect external API fault tolerance using circuit breakers and master cloud-native orchestration pipelines.
*   **Production Challenge**: Isolating third-party scraping crashes from your application layer, keeping web scrapers auto-healing, and declaring predictable deployments using infrastructure-as-code.
*   **Tech Stack & Libraries**:
    *   *Orchestration*: Kubernetes (Local cluster via Docker Desktop or Minikube), Helm (Kubernetes package manager).
    *   *Node Libraries*: `opossum` (circuit breaker engine), `axios`, `cheerio` (for fast asset parsing).
*   **Local Setup & Simulation Plan**:
    *   Write a target mock script on port `9000` that is programmed to deliberately throw `500 errors` or slow delays 80% of the time to act as your unstable target endpoint.
    *   Package your web-scraping Node.js application into a custom Docker image and write a **Helm Chart** to manage its values, resource constraints, replication bounds, and readiness/liveness probes.
    *   Deploy the chart to your local Kubernetes cluster. Run a stress-test query script and inspect your application logs to watch the opossum circuit breaker trip **Open** to preserve cluster resources, while Kubernetes auto-heals any replica nodes that hit unhandled limits.


