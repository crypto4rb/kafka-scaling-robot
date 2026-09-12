"""Order data model helpers: construction, validation, enrichment, and JSON
(de)serialization shared across producers and consumers."""

import json
import uuid
from datetime import datetime, timezone
from typing import Optional

VALID_STATUSES   = frozenset({"pending", "processing", "completed", "failed"})
VALID_CATEGORIES = frozenset({"electronics", "clothing", "food", "books", "furniture"})

def make_order(
    customer_id: str,
    product: str,
    category: str,
    quantity: int,
    price: float,
    status: str = "pending",
) -> dict:
    """Build a new order dict with a generated id, computed total, and creation timestamp."""
    return {
        "order_id":    str(uuid.uuid4()),
        "customer_id": customer_id,
        "product":     product,
        "category":    category,
        "quantity":    quantity,
        "price":       price,
        "total":       round(quantity * price, 2),
        "status":      status,
        "created_at":  datetime.now(timezone.utc).isoformat(),
    }

def validate_order(order: dict) -> tuple[bool, Optional[str]]:
    """Check required fields, quantity, price, and category; return (is_valid, error_reason)."""
    if not order.get("order_id"):
        return False, "missing order_id"
    if not order.get("customer_id"):
        return False, "missing customer_id"
    if order.get("quantity", 0) <= 0:
        return False, f"invalid quantity: {order.get('quantity')}"
    if order.get("price", 0) <= 0:
        return False, f"invalid price: {order.get('price')}"
    if order.get("category") not in VALID_CATEGORIES:
        return False, f"unknown category: {order.get('category')}"
    return True, None

def enrich_order(order: dict) -> dict:
    """Return a copy of `order` marked as processing, timestamped, and flagged priority if total > $500."""
    order = order.copy()
    order["status"]       = "processing"
    order["processed_at"] = datetime.now(timezone.utc).isoformat()
    # Simple business rule: orders > $500 get priority shipping
    order["priority"]     = order.get("total", 0) > 500
    return order

def to_json(order: dict) -> bytes:
    """Serialize an order dict to UTF-8 encoded JSON bytes for Kafka."""
    return json.dumps(order).encode("utf-8")

def from_json(data: bytes) -> dict:
    """Deserialize UTF-8 encoded JSON bytes from Kafka back into an order dict."""
    return json.loads(data.decode("utf-8"))
