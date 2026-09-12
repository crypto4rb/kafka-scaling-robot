# OrderFlow: Kafka E-Commerce Pipeline

**OrderFlow** is a robust, event-driven stream processing pipeline built with Python, Apache Kafka, and PostgreSQL. It simulates a modern e-commerce backend architecture that handles order ingestion, validation, routing, inventory management, and data persistence in real-time.

---

## Architecture Overview

The system is designed around a microservices-like architecture communicating asynchronously via Kafka topics.

### Data Flow

1. **Producer (`order_producer.py`)**: Acts as the customer frontend. It generates simulated order data (both valid and intentionally invalid) using the `Faker` library and publishes them to the `orders` topic.
2. **Stream Processor (`order_processor.py`)**: The brain of the operation. It consumes raw orders, applies business validation rules (e.g., positive quantities/prices, checking against valid product categories), and then handles routing:
   - **Valid Orders** are enriched (given a processing timestamp and a priority flag if the total > $500) and pushed to the `processed-orders` topic.
   - **Invalid Orders** are tagged with an error reason and routed to a Dead Letter Queue (DLQ) topic named `failed-orders`.
3. **Consumers**: Several downstream services react to the processed streams independently:
   - **Notification Service (`notification_consumer.py`)**: Listens to valid orders and simulates triggering customer notifications (e.g., priority alerts for high-value orders).
   - **Inventory Service (`inventory_consumer.py`)**: Listens to valid orders, deducts items from a simulated in-memory store, and triggers alerts when stock runs low.
   - **DLQ Handler (`dlq_handler.py`)**: Dedicated handler for monitoring and logging invalid orders from the `failed-orders` topic.
   - **PostgreSQL Sinks (`pg_sink_consumer.py` & `pg_dlq_sink_consumer.py`)**: Stateful consumers that batch-read from both `processed-orders` and `failed-orders` topics, subsequently persisting the records securely into an Aiven PostgreSQL database for analytics and long-term storage.

### Topics

- `orders`: Raw data ingestion queue.
- `processed-orders`: Validated and enriched orders stream.
- `failed-orders`: Dead Letter Queue for malformed data.

---

## Tech Stack

- **Python 3.12+**
- **Apache Kafka** (Confluent Kafka Client)
- **PostgreSQL** (Aiven Cloud, `psycopg2-binary`)
- **Faker** (For realistic synthetic data generation)
- **Threading & Subprocess** (Pipeline orchestration)

---

## Project Structure

```text
kafka-proj/
├── config/
│   ├── settings.py           # Central configuration (Broker, DB, Topics, Groups)
│   └── setup_topics.py       # Bootstrap script to ensure topics exist
├── consumers/
│   ├── dlq_handler.py        # Monitors failed orders
│   ├── inventory_consumer.py # Manages stock reduction
│   ├── notification_consumer.py # Simulates alerts
│   ├── pg_sink_consumer.py   # Persists valid orders to PG
│   └── pg_dlq_sink_consumer.py # Persists invalid orders to PG
├── producers/
│   └── order_producer.py     # Generates mock orders
├── streams/
│   └── order_processor.py    # Core validation & routing logic
├── utils/
│   ├── logger.py             # Shared color-coded logger
│   ├── models.py             # Data schemas and validation rules
│   └── pg_inspect.py         # DB Utility for inspection
├── main.py                   # Central launcher to orchestrate all services
├── pyproject.toml            # Dependencies and project metadata
└── uv.lock
```

---

## Installation & Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/divakar166/orderflow-kafka.git
   cd orderflow-kafka
   ```

2. **Set up a virtual environment:**
   We recommend using `uv` (as indicated by `uv.lock`) or standard `venv`:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   # Or using uv:
   uv sync
   ```

4. **Configuration:**
   Update `config/settings.py` with your Kafka Broker details and PostgreSQL database credentials.

---

## How to Run

The easiest way to stand up the entire architecture is using the built-in launcher.

1. Ensure your Kafka broker and PostgreSQL instances are running (and configured in `config/settings.py`).
2. Run the orchestrator:

```bash
python main.py
```

### Launcher Arguments (`main.py`)

- `--no-producer`: Start all consumers and processors, but hold off on producing new messages. Useful for draining existing queues or testing consumer logic.
- `--skip-setup`: Bypass the initial topic creation check if your topics are already strictly defined.
