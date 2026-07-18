"""
generate_retailflow_dataset.py

Generates the synthetic RetailFlow dataset used across Phases 4-7:
  customers.csv, products.csv,
  orders_day1.json / order_items_day1.json,
  orders_day2.json / order_items_day2.json   (schema evolution: discount_code)
  clickstream_day1.json / clickstream_day2.json

Requires:
  pip install faker --break-system-packages   (or inside a venv without the flag)

Run:
  python generate_retailflow_dataset.py

Output lands in ./retailflow_dataset/ , laid out ready to upload as-is:

  retailflow_dataset/
    customers.csv
    products.csv
    orders/order_date=<day1>/orders.json
    orders/order_date=<day2>/orders.json
    order_items/order_date=<day1>/order_items.json
    order_items/order_date=<day2>/order_items.json
    clickstream/event_date=<day1>/clickstream.json
    clickstream/event_date=<day2>/clickstream.json

Upload example:
  aws s3 cp retailflow_dataset/ s3://<YOUR_BUCKET>/raw/ --recursive
"""

import json
import os
import random
import string
from datetime import datetime, timedelta

from faker import Faker

# ---------------------------------------------------------------------------
# Config — tune sizes/dirtiness ratios here
# ----------------------------------------------------------------------------
SEED = 42
OUT_DIR = r"C:\Users\hp\Desktop\retailflow-capstone\phase-01-synthetic-data-generation-profiling\data"

N_CUSTOMERS = 5_000
N_PRODUCTS = 800
N_ORDERS_DAY1 = 20_000
N_ORDER_ITEMS_DAY1_TARGET = 55_000   # ~2.75 line items per order
N_ORDERS_DAY2 = 4_000
N_ORDER_ITEMS_DAY2_TARGET = 11_000
N_CLICKSTREAM_PER_DAY = 15_000

DAY1 = "2026-06-01"
DAY2 = "2026-06-02"

# Deliberate data-quality issue rates
PCT_BAD_EMAIL = 0.02          # customers.csv: null/malformed email
PCT_DUP_CUSTOMER_ID = 0.01    # customers.csv: duplicate customer_id rows
PCT_BAD_PRODUCT_REF = 0.005   # order_items: product_id not in products.csv
PCT_BAD_QUANTITY = 0.01       # order_items: negative or zero quantity

STORE_REGIONS = ["north", "south", "east", "west", "central"]
ORDER_STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]
SEGMENTS = ["consumer", "corporate", "home_office"]
CATEGORIES = ["Electronics", "Home", "Apparel", "Grocery", "Sports", "Toys", "Beauty", "Books"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout_start", "purchase"]
DISCOUNT_CODES = ["SUMMER10", "WELCOME5", "FLASH20", "VIP15", None, None, None]  # weighted toward no discount

random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

os.makedirs(OUT_DIR, exist_ok=True)


def write_json_lines(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def random_ts(day_str, start_hour=0, end_hour=23):
    base = datetime.strptime(day_str, "%Y-%m-%d")
    seconds_into_day = random.randint(start_hour * 3600, end_hour * 3600 - 1)
    return (base + timedelta(seconds=seconds_into_day)).strftime("%Y-%m-%dT%H:%M:%SZ")


def malformed_email(good_email):
    """Return a deliberately broken variant of an otherwise-valid email."""
    variant = random.choice(["no_at", "no_domain", "trailing_space", "empty"])
    if variant == "no_at":
        return good_email.replace("@", "_")
    if variant == "no_domain":
        return good_email.split("@")[0] + "@"
    if variant == "trailing_space":
        return " " + good_email + "  "
    return ""  # empty string, distinct from null


# ---------------------------------------------------------------------------
# customers.csv — 5,000 rows, ~2% bad email, ~1% duplicate customer_id
# ---------------------------------------------------------------------------
customers = []
customer_ids_pool = []

for i in range(1, N_CUSTOMERS + 1):
    cid = i
    customer_ids_pool.append(cid)
    first = fake.first_name()
    last = fake.last_name()
    good_email = f"{first.lower()}.{last.lower()}{i}@example.com"

    roll = random.random()
    if roll < PCT_BAD_EMAIL / 2:
        email = None  # null email
    elif roll < PCT_BAD_EMAIL:
        email = malformed_email(good_email)  # malformed but non-null
    else:
        email = good_email

    customers.append({
        "customer_id": cid,
        "first_name": first,
        "last_name": last,
        "email": email,
        "signup_date": fake.date_between(start_date="-2y", end_date="-1d").isoformat(),
        "city": fake.city(),
        "segment": random.choice(SEGMENTS),
    })

# Inject ~1% duplicate customer_id rows: re-use an existing id with fresh
# (and possibly conflicting) attribute values, appended to the end of the
# file — simulates the same customer being re-emitted by a dirty upstream
# system, or two different people accidentally sharing an id.
n_dupes = int(N_CUSTOMERS * PCT_DUP_CUSTOMER_ID)
for _ in range(n_dupes):
    dup_id = random.choice(customer_ids_pool)
    first = fake.first_name()
    last = fake.last_name()
    customers.append({
        "customer_id": dup_id,   # deliberately re-used, not new
        "first_name": first,
        "last_name": last,
        "email": f"{first.lower()}.{last.lower()}.dup@example.com",
        "signup_date": fake.date_between(start_date="-2y", end_date="-1d").isoformat(),
        "city": fake.city(),
        "segment": random.choice(SEGMENTS),
    })

random.shuffle(customers)

with open(f"{OUT_DIR}/customers.csv", "w", newline="") as f:
    import csv
    w = csv.DictWriter(f, fieldnames=list(customers[0].keys()))
    w.writeheader()
    for row in customers:
        w.writerow(row)

print(f"customers.csv: {len(customers)} rows "
      f"({n_dupes} duplicate customer_id rows, "
      f"~{int(N_CUSTOMERS * PCT_BAD_EMAIL)} bad emails)")

# ---------------------------------------------------------------------------
# products.csv — 800 rows
# ---------------------------------------------------------------------------
products = []
product_ids_pool = []
for i in range(1, N_PRODUCTS + 1):
    pid = f"PROD{i:05d}"
    product_ids_pool.append(pid)
    products.append({
        "product_id": pid,
        "product_name": fake.catch_phrase(),
        "category": random.choice(CATEGORIES),
        "unit_price": round(random.uniform(3, 500), 2),
        "active_flag": random.random() > 0.05,  # ~5% inactive/discontinued
    })

with open(f"{OUT_DIR}/products.csv", "w", newline="") as f:
    import csv
    w = csv.DictWriter(f, fieldnames=list(products[0].keys()))
    w.writeheader()
    w.writerows(products)

print(f"products.csv: {len(products)} rows")

# A pool of product_ids that deliberately do NOT exist in products.csv, used
# to inject referential-integrity failures into order_items.
bad_product_ids_pool = [f"PROD{i:05d}" for i in range(90000, 90020)]


# ---------------------------------------------------------------------------
# Shared order/order_items generator — used for both day1 and day2 so the
# two days are structurally identical except for discount_code on day2.
# ---------------------------------------------------------------------------
def generate_orders_and_items(n_orders, target_items, day_str, order_id_start, include_discount_code):
    orders = []
    order_items = []
    order_id_counter = order_id_start

    for _ in range(n_orders):
        oid = f"ORD{order_id_counter:07d}"
        order_id_counter += 1
        cust = random.choice(customer_ids_pool)

        order_row = {
            "order_id": oid,
            "customer_id": cust,
            "order_ts": random_ts(day_str),
            "store_region": random.choice(STORE_REGIONS),
            "status": random.choice(ORDER_STATUSES),
        }
        if include_discount_code:
            # ~40% of day2 orders carry a discount code — this is the new
            # column that day1 files never had (schema evolution test).
            order_row["discount_code"] = random.choice(DISCOUNT_CODES)
        orders.append(order_row)

        # 1-5 line items per order, averaged to roughly hit target_items
        avg_items_per_order = target_items / n_orders
        n_items = max(1, int(random.gauss(avg_items_per_order, 1)))

        for _ in range(n_items):
            roll = random.random()
            if roll < PCT_BAD_PRODUCT_REF:
                pid = random.choice(bad_product_ids_pool)  # referential integrity failure
            else:
                pid = random.choice(product_ids_pool)

            qty_roll = random.random()
            if qty_roll < PCT_BAD_QUANTITY / 2:
                qty = 0
            elif qty_roll < PCT_BAD_QUANTITY:
                qty = -random.randint(1, 3)
            else:
                qty = random.randint(1, 5)

            unit_price = round(random.uniform(3, 500), 2)
            line_total = round(unit_price * qty, 2)

            order_items.append({
                "order_id": oid,
                "product_id": pid,
                "quantity": qty,
                "unit_price": unit_price,
                "line_total": line_total,
            })

    return orders, order_items, order_id_counter


# ---------------------------------------------------------------------------
# day1: no discount_code
# ---------------------------------------------------------------------------
orders_day1, order_items_day1, next_order_id = generate_orders_and_items(
    n_orders=N_ORDERS_DAY1,
    target_items=N_ORDER_ITEMS_DAY1_TARGET,
    day_str=DAY1,
    order_id_start=1,
    include_discount_code=False,
)
write_json_lines(orders_day1, f"{OUT_DIR}/orders/order_date={DAY1}/orders.json")
write_json_lines(order_items_day1, f"{OUT_DIR}/order_items/order_date={DAY1}/order_items.json")
print(f"orders_day1: {len(orders_day1)} rows, order_items_day1: {len(order_items_day1)} rows")

# ---------------------------------------------------------------------------
# day2: adds discount_code (schema evolution) — orders only. order_items
# schema is unchanged from day1.
# ---------------------------------------------------------------------------
orders_day2, order_items_day2, _ = generate_orders_and_items(
    n_orders=N_ORDERS_DAY2,
    target_items=N_ORDER_ITEMS_DAY2_TARGET,
    day_str=DAY2,
    order_id_start=next_order_id,
    include_discount_code=True,
)
write_json_lines(orders_day2, f"{OUT_DIR}/orders/order_date={DAY2}/orders.json")
write_json_lines(order_items_day2, f"{OUT_DIR}/order_items/order_date={DAY2}/order_items.json")
print(f"orders_day2: {len(orders_day2)} rows, order_items_day2: {len(order_items_day2)} rows")


# ---------------------------------------------------------------------------
# clickstream_day1.json / clickstream_day2.json — ~15,000 events/day
# Used only in the streaming/Auto Loader phase; not tied to orders directly.
# ---------------------------------------------------------------------------
def generate_clickstream(n_events, day_str):
    events = []
    for _ in range(n_events):
        # ~25% of sessions are anonymous (no customer_id) — realistic for a
        # web session stream where not every visitor is logged in.
        cust = random.choice(customer_ids_pool) if random.random() > 0.25 else None
        events.append({
            "session_id": fake.uuid4(),
            "customer_id": cust,
            "event_type": random.choice(EVENT_TYPES),
            "page_url": "/" + "/".join(fake.uri_path().split("/")[:2]),
            "event_ts": random_ts(day_str),
            "device_type": random.choice(DEVICE_TYPES),
        })
    return events


for day_str, label in [(DAY1, "day1"), (DAY2, "day2")]:
    clicks = generate_clickstream(N_CLICKSTREAM_PER_DAY, day_str)
    write_json_lines(clicks, f"{OUT_DIR}/clickstream/event_date={day_str}/clickstream.json")
    print(f"clickstream_{label}: {len(clicks)} rows")

print("\nDone. Output written to:", os.path.abspath(OUT_DIR))