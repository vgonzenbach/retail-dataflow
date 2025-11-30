"""Produced with assistance from ChatGPT"""
from faker import Faker
import random
import uuid
from datetime import datetime, timezone

fake = Faker()

def make_order_event() -> dict:
    items = []
    for _ in range(random.randint(1, 5)):
        price = round(random.uniform(5, 200), 2)
        quantity = random.randint(1, 3)
        items.append({
            "product_id": str(uuid.uuid4()),
            "product_name": fake.word(),
            "quantity": quantity,
            "price": price,
        })

    total_amount = sum(i["quantity"] * i["price"] for i in items)

    return {
        "event_type": "order",
        "order_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "order_date": datetime.now(timezone.utc).isoformat(),
        "status": random.choice(["pending", "processing", "shipped", "delivered"]),
        "items": items,
        "shipping_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "country": fake.country(),
        },
        "total_amount": round(total_amount, 2),
    }

def make_inventory_event() -> dict:
    # quantity_change can be negative (sale/damage) or positive (restock/return)
    reason = random.choice(["restock", "sale", "return", "damage"])
    # simple bias: big positive changes mostly for restock, negatives for sale/damage
    if reason in ("restock", "return"):
        quantity_change = random.randint(1, 100)
    elif reason in ("sale", "damage"):
        quantity_change = random.randint(1, 100) * -1 

    return {
        "event_type": "inventory",
        "inventory_id": str(uuid.uuid4()),
        "product_id": str(uuid.uuid4()),
        "warehouse_id": str(uuid.uuid4()),
        "quantity_change": quantity_change,
        "reason": reason,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

def make_invalid_order_event() -> dict:
    event = make_order_event()
    event['status'] = 'lost'
    return event

def make_invalid_inventory_event() -> dict:
    event = make_inventory_event()
    event['reason'] = 'lost'
    return event

if __name__ == '__main__':
    event = make_order_event()
    print(event)