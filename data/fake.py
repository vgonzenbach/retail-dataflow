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

if __name__ == '__main__':
    event = make_order_event()
    print(event)