from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    # Kafka
    KAFKA_BROKER: str = "localhost:9092"

    # Kafka Topics
    TOPIC_ORDERS:         str = "orders"
    TOPIC_PROCESSED:      str = "processed-orders"
    TOPIC_FAILED:         str = "failed-orders"
    TOPIC_NOTIFICATIONS:  str = "notifications"

    # Topic Config
    TOPIC_PARTITIONS:  int = 3
    TOPIC_REPLICATION: int = 1

    # Consumer Groups
    GROUP_NOTIFICATION: str = "notification-service"
    GROUP_INVENTORY:    str = "inventory-service"
    GROUP_DLQ:          str = "dlq-handler"
    GROUP_PG_SINK:      str = "pg-sink-service"
    GROUP_PG_DLQ_SINK:  str = "dlq-sink-handler"

    # Producer Settings
    PRODUCE_INTERVAL_SEC: float = 1.0
    INVALID_ORDER_RATE:   float = 0.2

    # PostgreSQL (Aiven)
    AIVEN_PG_HOST:     str = "localhost"
    AIVEN_PG_PORT:     int = 5432
    AIVEN_PG_DBNAME:   str = "defaultdb"
    AIVEN_PG_USER:     str = "postgres"
    AIVEN_PG_PASSWORD: str = ""
    AIVEN_PG_SSLMODE:  str = "require"

    # PG Table Names
    PG_TABLE_ORDERS:     str = "processed_orders"
    PG_TABLE_FAILED:     str = "failed_orders"
    PG_BATCH_SIZE:       int = 50
    PG_BATCH_TIMEOUT_SEC: float = 5.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )

settings = AppConfig()
