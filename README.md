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

### 2. Distributed Media Transcoding Pipeline (RabbitMQ)
*   **The Goal:** Master asynchronous job queuing, heavy multi-core CPU background tasks, and decoupling client APIs from heavy operations.
*   **Production Challenge:** Offloading non-blocking long-running work (resizing/encoding files), ensuring tasks are never lost if a server crashes, and isolating resource-heavy worker pools.
*   **Tech Stack & Libraries:**
    *   *Queue & Framework:* RabbitMQ, Fastify or Express.
    *   *Node Libraries:* `amqplib` (the standard AMQP client), `sharp` (blazing fast image processor), `fluent-ffmpeg` (the industry video/audio tool), `multer` (upload handler).
    *   *System Tool:* Native `ffmpeg` binary installed on macOS via Homebrew.
*   **Local Setup & Simulation Plan:**
    *   Run RabbitMQ with its web management UI plugin via Docker.
    *   Configure a durable queue with an explicit manual acknowledgment (`ack`/`nack`) setup, along with a Dead Letter Exchange (DLX) for corrupted file drops.
    *   Use an `autocannon` stress-test script to fire concurrent file uploads at the HTTP API. Ensure the API immediately sends back a `202 Accepted` status while your background consumer process handles the processing work one by one.

### 3. E-Commerce Fuzzy Search & Autocomplete Engine (Elasticsearch)
*   **The Goal:** Master text tokenization, index structures, typo tolerance, and fast text matching in production applications.
*   **Production Challenge:** Relational databases fail at speed and relevance when searching text fields with typos, partial terms, and high concurrency.
*   **Tech Stack & Libraries:**
    *   *Search Engine:* Elasticsearch or OpenSearch.
    *   *Node Libraries:* `@elastic/elasticsearch` (the official client), `dotenv`.
*   **Local Setup & Simulation Plan:**
    *   Run a single-node development container of Elasticsearch.
    *   Write a seeding script to push 50,000 mock catalog products into an index configured with an `edge_ngram` custom analyzer (essential for typing autocomplete).
    *   Expose a search HTTP endpoint that builds multi-field match queries with explicit fuzziness parameters, allowing search terms like "iphnoe" to cleanly match "iPhone".

### 4. High-Frequency Financial Ticker Engine (Streams & Worker Threads)
*   **The Goal:** Leverage the underlying system hardware by breaking past the single-threaded nature of Node.js for CPU-heavy tasks.
*   **Production Challenge:** Processing thousands of continuous WebSocket price ticks per second and running math-heavy aggregations (moving averages) without delaying the main event loop.
*   **Tech Stack & Libraries:**
    *   *Core Engine:* Node.js native `worker_threads` and `stream` modules.
    *   *Node Libraries:* `ws` (the standard fast WebSocket engine).
*   **Local Setup & Simulation Plan:**
    *   Write a separate mock market script that opens a local WebSocket connection and loops infinitely to broadcast randomized price objects every few milliseconds.
    *   Your main application reads the continuous incoming stream, sends the arrays into a pool of background Node Worker Threads using `SharedArrayBuffer` to eliminate serialization lag, and handles calculation updates efficiently.

---

# Phase 2: Resilience, Security, & API Gateways

### 5. Resilient Webhook Ingestion Engine (Redis Distributed Locking)
*   **The Goal:** Implement strict protective measures against traffic spikes and ensure idempotency (preventing accidental double-processing).
*   **Production Challenge:** Securely handling external payment callbacks (like Stripe or PayPal) where network retries from the provider can accidentally trigger duplicate actions or state corruption.
*   **Tech Stack & Libraries:**
    *   *Cache & DB:* Redis, PostgreSQL or MongoDB.
    *   *Node Libraries:* `ioredis` (robust redis driver supporting atomic scripts), `uuid`.
*   **Local Setup & Simulation Plan:**
    *   Run Redis and PostgreSQL containers via Docker.
    *   Expose a webhook ingestion endpoint. Write defensive middleware that reads an event token and uses Redis's atomic `SET NX` command to acquire a short-lived distributed lock.
    *   Simulate load using `k6` to send identical payload batches simultaneously. Confirm that exactly one request triggers the database state modification, while all duplicates are instantly dropped with a `429` error.

### 6. Dynamic Multi-Tenant API Gateway (Cluster Module & Dynamic Routing)
*   **The Goal:** Build an infrastructural reverse proxy layer capable of routing traffic dynamically while taking advantage of multi-core systems.
*   **Production Challenge:** Merging isolated underlying services behind a single gateway URL, managing cross-origin headers (CORS), and distributing network load smoothly across hardware threads.
*   **Tech Stack & Libraries:**
    *   *Core Engine:* Node.js native `cluster`, `http`, and `url` core modules.
    *   *Node Libraries:* `http-proxy` (the underlying engine for proxy routing networks).
*   **Local Setup & Simulation Plan:**
    *   Spin up two minimalist mock API servers on separate local ports (e.g., `3001` and `3002`).
    *   Your Gateway application will use the native `cluster` module to fork an execution thread for every available core on your Mac processor.
    *   Use `autocannon` to hit your gateway port, validating via the Activity Monitor app that incoming connection routing streams are split evenly across every core.

### 7. Secure OAuth2/OIDC Identity Provider (Crypto & Token Rotation)
*   **The Goal:** Master real-world stateless token authorization, asymmetric cryptography, and deep session control.
*   **Production Challenge:** Designing an independent authorization server that issues access and refresh tokens without using heavy third-party SaaS abstractions, with absolute protection against token theft.
*   **Tech Stack & Libraries:**
    *   *Core Engine:* Node.js native `crypto` module.
    *   *Database:* PostgreSQL or MySQL.
    *   *Node Libraries:* `jsonwebtoken` (industry standard JWT handler), `bcrypt` (standard secure password hashing).
*   **Local Setup & Simulation Plan:**
    *   Run a relational database via Docker to record active session footprints and token lineage tracking.
    *   Use Node's native `crypto` module to generate RSA public/private key pairs locally to handle token signatures.
    *   Write integration tests simulating token theft: if a user submits an older refresh token twice, your system must trigger its automatic security hook, invalidating all related tokens in that family branch immediately.

---

# Phase 3: Real-Time Systems, Scheduling, & Fault Tolerance

### 8. Real-Time Activity Broker (WebSockets & Pub/Sub Scalability)
*   **The Goal:** Manage persistent, bi-directional stateful communication layers that scale horizontally across independent servers.
*   **Production Challenge:** Syncing real-time notifications or chat states when users are connected across entirely different server instances. (No complex UI built here; backend focus only).
*   **Tech Stack & Libraries:**
    *   *Cache Layer:* Redis (specifically leveraging its high-speed Pub/Sub system).
    *   *Node Libraries:* `socket.io` or pure `ws`.
*   **Local Setup & Simulation Plan:**
    *   Run a local Redis container on port `6379`.
    *   Launch two instances of your Node web server on separate local ports (`4001` and `4002`) simulating an auto-scaling environment.
    *   Connect to both ports concurrently using the terminal CLI client **wscat**. Show that firing an event into port 4001 triggers an internal Redis Pub/Sub broadcast, which makes the socket connected on port 4002 print the message instantly.

### 9. Automated Batch Scheduler (Distributed Crons & Process Signals)
*   **The Goal:** Implement automated periodic data aggregations that execute safely without doubling up tasks or breaking under deployment cycles.
*   **Production Challenge:** Running a cron job on a multi-instance production setup without having the job fire multiple times, and ensuring mid-process jobs exit cleanly during a container update.
*   **Tech Stack & Libraries:**
    *   *Database:* MongoDB or PostgreSQL.
    *   *Node Libraries:* `agenda` (highly popular MongoDB-backed cron manager) or `bullmq` (Redis-backed repeat scheduler), `pdfkit` (streaming PDF creator), `nodemailer`.
*   **Local Setup & Simulation Plan:**
    *   Run a database container alongside **Mailhog** (a free developer SMTP tool that intercepts outbound mail for local UI viewing).
    *   Seed your database with 5,000 mock user rows. Open three distinct terminal windows running the app simultaneously.

### 10. Fault-Tolerant Distributed Asset Scraper (Circuit Breakers & Backoff)
*   **The Goal:** Architect deep resilience into applications that interact heavily with external third-party internet endpoints.
*   **Production Challenge:** Preventing a slow or broken external API from consuming your local server's network sockets and causing a cascading system-wide crash.
*   **Tech Stack & Libraries:**
    * *Node Libraries:* `opossum` (the absolute industry standard Node.js Circuit Breaker library), `axios`, `cheerio` (for fast asset parsing).
*   **Local Setup & Simulation Plan:**
    *   To test your code safely without hitting real web blocks, write a 20-line target script on port `9000` that is programmed to deliberately throw `500 errors` or slow delays 80% of the time.
    *   Configure your scraper to query this faulty local endpoint through an `opossum` circuit wrapper using exponential backoff retry logic.
    *   Run a heavy query script and look at your logs to witness the circuit breaker trip **Open**, completely bypassing network requests to preserve system health until the target stabilizes.

