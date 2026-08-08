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

### 3. E-Commerce Fuzzy Search & Autocomplete Engine (Full ELK Stack)
*   **The Goal:** Master text tokenization, index structures, typo tolerance, Change Data Capture (CDC) streaming, and log/metric visualization using a unified enterprise search stack.
*   **Production Challenge:** High-concurrency text searching across fields with typos and partial terms while maintaining sync between primary relational stores and the search index without application layer coupling.
*   **Tech Stack & Libraries:**
    *   *Search & Logistics Stack:* Elasticsearch, Logstash, Kibana (The ELK Stack), PostgreSQL.
    *   *Node Libraries:* `@elastic/elasticsearch` (Official enterprise client), `pg` (PostgreSQL client), `dotenv`.
*   **Local Setup & Simulation Plan:**
    *   Run Elasticsearch, Logstash, Kibana, and PostgreSQL containers via Docker.
    *   Seed PostgreSQL with 50,000 mock catalog products. Configure Logstash with a JDBC input plugin to poll PostgreSQL for real-time updates and automatically stream them into an Elasticsearch index configured with an `edge_ngram` custom analyzer (essential for typing autocomplete).
    *   Expose a search HTTP endpoint that builds multi-field match queries with explicit fuzziness parameters, allowing search terms like "iphnoe" to cleanly match "iPhone". Use Kibana to build dashboards monitoring search latency and Logstash ingestion performance.

### 4. High-Frequency Financial Ticker Engine (Streams & Worker Threads)
*   **The Goal:** Leverage underlying system hardware by breaking past the single-threaded nature of Node.js for heavy CPU-bound data parsing and stream processing.
*   **Production Challenge:** Processing thousands of continuous data ticks per second and running math-heavy aggregations (moving averages) without delaying the main event loop or incurring serialization lag.
*   **Tech Stack & Libraries:**
    *   *Core Engine:* Node.js native `worker_threads` and `stream` modules.
    *   *Node Libraries:* Native file system streams or in-memory array generators.
*   **Local Setup & Simulation Plan:**
    *   Write a mock market data script that generates a massive local binary file or loops infinitely in memory to broadcast continuous, high-volume price objects into a Node Readable Stream.
    *   Your main application reads the continuous incoming stream, splits the pipeline using stream transforms, and distributes the data chunks into a pool of background Node Worker Threads.
    *   Use `SharedArrayBuffer` to eliminate serialization lag between threads, calculating complex rolling metrics efficiently on background threads before streaming the aggregated values to standard output.

---

# Phase 2: Resilience, Security, & API Gateways

### 5. Resilient Webhook & Outbox Engine (Redis Distributed Locking + PostgreSQL)
*   **The Goal:** Master strict protective traffic measures, concurrency controls, and the Transactional Outbox pattern to guarantee event idempotency and reliable event delivery.
*   **Production Challenge:** Securely handling external payment callbacks where network retries from providers can cause duplicate database side-effects, and avoiding partial failures where database updates succeed but message broker notifications fail to send.
*   **Tech Stack & Libraries:**
    *   *Cache, DB & Broker:* Redis, PostgreSQL.
    *   *Node Libraries:* `ioredis` (Robust driver supporting atomic Lua scripts), `pg` (Standard Postgres client), `uuid`.
*   **Local Setup & Simulation Plan:**
    *   Run Redis and PostgreSQL containers via Docker. Define an application state table alongside a dedicated `outbox` table in PostgreSQL.
    *   Expose a webhook ingestion endpoint. Write defensive middleware that reads an event token and uses Redis's atomic `SET NX` command to acquire a short-lived distributed lock to reject concurrent processing.
    *   Inside the logic, open a single atomic PostgreSQL transaction (`BEGIN` / `COMMIT`) that writes the verified application update *and* saves the event notification to the `outbox` table simultaneously.
    *   Simulate load using `k6` to send identical payload batches concurrently, confirming exactly one transaction modifies state while duplicates drop with a `429 Too Many Requests` error. A separate background worker script continuously polls the outbox table to mimic forwarding successfully saved events outward.

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
*   **The Goal:** Master stateless token authorization, asymmetric cryptography, deep session control, and hierarchical Role-Based Access Control (RBAC) schemas mapped to an ultra-high-scale wide-column database.
*   **Production Challenge:** Designing a distributed authentication server that processes millions of token validations, permission evaluations, and linear session footprints at immense scale without structural single points of data failure.
*   **Tech Stack & Libraries:**
    *   *Core Engine & DB:* Apache Cassandra (Wide-column NoSQL database).
    *   *Node Libraries:* `cassandra-driver`, `jsonwebtoken`, `bcrypt`, Node's native `crypto` module.
*   **Local Setup & Simulation Plan:**
    *   Run a single-node Apache Cassandra instance via Docker. Define a keyspace optimized to store user token histories alongside deeply nested corporate role and permission mappings.
    *   Use Node's native `crypto` module to generate RSA public/private key pairs locally to handle cryptographic token signatures.
    *   Expose endpoints that evaluate dynamic middleware permissions (e.g., Admin vs Manager) against the wide-column Cassandra lookups. Write integration tests simulating token theft: if a user submits an older refresh token twice, your system must trigger an automatic security hook, invalidating all related tokens in that family branch within Cassandra immediately.

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

### 9. Fault-Tolerant Cloud-Native Deployment (Kubernetes + Helm + Terraform)
*   **The Goal:** Master automated cloud infrastructure provisioning, enterprise container orchestration, and application layer circuit-breaking mechanisms.
*   **Production Challenge:** Isolating third-party dependency crashes from your critical runtime pathways, keeping distributed services auto-healing under load, and managing infrastructure predictably using declarative configuration files.
*   **Tech Stack & Libraries:**
    *   *Orchestration & IaC:* Terraform (Local file/Docker providers), Kubernetes (Local cluster via Docker Desktop or Minikube), Helm (Kubernetes package manager).
    *   *Node Libraries:* `opossum` (Circuit breaker engine), `axios`.
*   **Local Setup & Simulation Plan:**
    *   Write a trivial downstream target mock script on port `9000` that is programmed to deliberately throw `500 Internal Server Errors` or heavy timeouts 80% of the time to act as an unstable downstream dependency.
    *   Use **Terraform** locally to manage your local container registry contexts, networking boundaries, and persistent volume mount structures.
    *   Package a simple asset-fetching Node.js application into a custom Docker image and write a **Helm Chart** defining resource limits, replication bounds, liveness/readiness probes, and horizontal pod autoscalers.
    *   Deploy your Helm release onto the local Kubernetes cluster. Run a stress-test load script using `autocannon` against the application, verifying the `opossum` circuit breaker trips **Open** to shield internal resources, while monitoring your local cluster dashboard as Kubernetes spins up replacement pods when specific processing boundaries are exceeded.


