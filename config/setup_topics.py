import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
 
from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import settings

TOPICS = [
    settings.TOPIC_ORDERS,
    settings.TOPIC_PROCESSED,
    settings.TOPIC_FAILED,
    settings.TOPIC_NOTIFICATIONS,
]

def create_topics():
    admin = AdminClient({"bootstrap.servers": settings.KAFKA_BROKER})

    # Check which topics already exist
    existing = admin.list_topics(timeout=10).topics.keys()

    new_topics = [
        NewTopic(
            topic,
            num_partitions=settings.TOPIC_PARTITIONS,
            replication_factor=settings.TOPIC_REPLICATION,
            config={
                "retention.ms": str(7 * 24 * 60 * 60 * 1000),
                "cleanup.policy": "delete",
            }
        )
        for topic in TOPICS
        if topic not in existing
    ]

    if not new_topics:
        print("All topics already exist.")
        return

    futures = admin.create_topics(new_topics)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic: '{topic}'  "
                  f"[partitions={TOPIC_PARTITIONS}, replication={TOPIC_REPLICATION}]")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

    print("\nAll Kafka topics:")
    for t in admin.list_topics(timeout=10).topics:
        print(f"   - {t}")

if __name__ == "__main__":
    print(f"Connecting to Kafka broker at {settings.KAFKA_BROKER}...\n")
    create_topics()
