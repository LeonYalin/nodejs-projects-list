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

### 5. Resilient Webhook Ingestion & Event-Sourced Outbox Engine (Redis + PostgreSQL)
*   **The Goal:** Master strict protective traffic measures, concurrency controls, Event Sourcing mechanics, and the Transactional Outbox pattern to guarantee event idempotency, reliable microservices communication, and state replayability.
*   **Production Challenge:** Securely capturing external payment callbacks, avoiding duplicate side-effects via distributed locks, and solving the "dual-write" problem where updating the database succeeds but notifying downstream microservices fails.
*   **Tech Stack & Libraries:**
    *   *Infrastructure:* Redis, PostgreSQL.
    *   *Node Libraries:* `ioredis` (Atomic Lua scripting/locks), `pg` (Postgres client), `uuid`.
*   **Local Setup & Simulation Plan:**
    *   **Database Schema:** Run Redis and PostgreSQL via Docker. Define an `outbox_events` table (acting jointly as the Event Store log and the Transactional Outbox table) and a `payment_projections` table (representing the current read/write state).
    *   **Ingestion Guard:** Build a webhook endpoint that uses Redis `SET NX` to acquire a short-lived distributed lock. Reject concurrent duplicate retry attempts immediately with a `429 Too Many Requests` error.
    *   **Atomic Outbox Commit:** Inside a single atomic PostgreSQL transaction (`BEGIN` / `COMMIT`), append the new immutable event to the `outbox_events` table and update the current state in the `payment_projections` table simultaneously.
    *   **The Event Sourcing Twist (State Replay):** Write a CLI script that deletes a row from `payment_projections`, reads all historical events for that specific ID sequentially from the `outbox_events` table, and completely rebuilds the current state from scratch.
    *   **Microservices Outbox Worker:** Write a separate background worker script (the Outbox Relayer) that continuously polls or streams unsent entries from the `outbox_events` table, simulates forwarding them to downstream microservices, and marks them as processed to guarantee at-least-once delivery.
    *   **Load Testing:** Use `k6` to send identical payload batches concurrently. Verify that the distributed lock drops duplicates, sequence numbers in the outbox remain gap-free, and the outbox worker successfully broadcasts events forward without missing data.



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

### 9. Fault-Tolerant Cloud Deployment & GitOps Automation (Kubernetes + Helm + Terraform + GitHub Actions)
*   **The Goal:** Master automated cloud infrastructure provisioning, enterprise container orchestration, application-layer circuit-breaking mechanisms, and multi-stage production GitOps pipelines.
*   **Production Challenge:** Isolating third-party dependency crashes from critical runtime pathways, keeping distributed services auto-healing under load, managing infrastructure predictably using declarative configuration files, and building automated quality gates that safely test, secure, and containerize code before deployment.
*   **Tech Stack & Libraries:**
    *  *CI/CD & Delivery*: GitHub Actions, Docker Registry, Security scanners (`gitleaks`, `npm audit`, `trivy`).
    *   *Orchestration & IaC*: Terraform (Local file/Docker providers), Kubernetes (Local cluster via Docker Desktop, Minikube, or Kind), Helm (Kubernetes package manager).
    *   *Node Libraries:* `opossum` (Circuit breaker engine), `axios`.
*   **Local Setup & Simulation Plan:**
    *   **The Automated Quality & Containerization Pipeline**: Write a production-grade GitHub Actions workflow that executes automatically on code changes. Structure it into rigid validation phases:
        *   *Code Health*: Run automated code linting and strict TypeScript compilation checks.
        *   *Security Audits*: Scan for dependency CVE vulnerabilities using `npm audit` and prevent hardcoded credentials leaks using a specialized static scanner like `gitleaks`.
        *   *Multi-Arch Packaging*: Compile the Node.js application into a production-optimized, multi-stage Docker image, utilizing advanced layer caching strategies to speed up pipeline execution.
        *   *Image Security*: Run an automated vulnerability assessment on the final built image using a tool like `trivy` before signing off on the release.
    *  ** The Infrastructure & Configuration Phase**: Use **Terraform** locally to manage your local container registry contexts, networking boundaries, and persistent volume mount structures. Package the application into a custom **Helm Chart** that declaratively defines CPU/Memory resource constraints, replication boundaries, horizontal pod autoscalers (HPA), and native Kubernetes liveness and readiness health probes.
    *   **The Resiliency & Fault-Tolerance Simulation**: Write a trivial downstream target mock script on port `9000` that is programmed to deliberately throw `500 Internal Server Errors` or heavy timeouts 80% of the time to simulate an unstable third-party API.
    *   **The Live Load Test**: Deploy your verified Helm release onto your local Kubernetes cluster. Execute a heavy stress-test script using `autocannon` against the deployed gateway service. Verify that your application's `opossum` circuit breaker smoothly trips **Open** to shield local node event loops from cascading network lag, while simultaneously observing your local cluster dashboard as Kubernetes spins up fresh pod replicas when configured processing metric boundaries are exceeded.

    ### 10.  Uber-Style Real-Time Delivery Matcher & Geo-Analytics Engine (PostgreSQL + PostGIS)

*   ***The Goal:** Master advanced relational data design, specialized geospatial indexing (`GiST`), and high-performance spatial queries by building the core backend routing engine for a ride-hailing service.
*   ***Production Challenge:** Safely storing massive histories of trips, calculating complex location-based matches (e.g., finding the 5 closest available drivers to a passenger instantly), and identifying high-demand areas (Surge Pricing zones) without destroying database CPU performance.
*   ***Tech Stack & Libraries:**
    *   **Database:* PostgreSQL with the **PostGIS** extension enabled.
    *   **Node Libraries:* `pg` (Official Postgres client), `fastify` or `express`.
    *   **System Tool:* Docker (using the `postgis/postgis` image).
*   ***Local Setup & Simulation Plan:**
    *   *Spin up a PostGIS container via Docker. Seed the database with a clean relational schema: `drivers` (with current coordinates), `passengers`, and `rides` (storing start/end pickup geometry paths).
    *   ***Geospatial Indexing:** Map latitude and longitude fields into official PostGIS geography points and bind a **GiST (Generalized Search Tree)** index to that column.
    *   ***The Uber-Style Match:** Expose an HTTP endpoint where a passenger requests a ride. Your Node app executes an optimized SQL query using `ST_DWithin` and the `<->` KNN distance operator to instantly find and lock the nearest 5 available drivers sorted by physical proximity within milliseconds.
    *   ***Dynamic Surge Analytics:** Store high-demand polygon shapes (e.g., concert venues or downtown grid coordinates) as `GEOMETRY` zones. Write a query using `ST_Contains` to detect if a passenger's pickup location falls inside a high-demand zone to dynamically apply a pricing multiplier.
    *   *Use `k6` to blast hundreds of concurrent driver location coordinate updates (`PING` updates) per second into the server, verifying that the database cleanly indexes the moving coordinates while serving passenger match requests without lag.



