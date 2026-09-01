"""
Generates messy, realistic raw_sales data for the MySQL operational DB.
Simulates: duplicates, NULLs in required fields, inconsistent date formats,
and mistaken negative quantities -- exactly what the Validate stage must catch.
"""
import random
import datetime

PRODUCTS = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Thingamajig",
            "Doohickey", "Gizmo Pro", "Gizmo Lite", "SuperTool", "MegaKit"]
REGIONS = ["North", "South", "East", "West", "Central"]
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%d-%b-%Y"]

def random_date_str():
    d = datetime.date(2026, random.randint(1, 8), random.randint(1, 28))
    fmt = random.choice(DATE_FORMATS)
    return d.strftime(fmt)

def generate_rows(n=50000):
    rows = []
    order_id = 1
    for _ in range(n):
        customer_id = random.randint(1, 5000)
        product = random.choice(PRODUCTS)
        region = random.choice(REGIONS)
        quantity = random.randint(1, 20)
        # ~2% mistaken negative quantities
        if random.random() < 0.02:
            quantity = -quantity
        price = round(random.uniform(5, 500), 2)
        date_str = random_date_str()

        # ~3% NULL in required fields (customer_id or region)
        if random.random() < 0.015:
            customer_id = None
        if random.random() < 0.015:
            region = None

        rows.append((order_id, customer_id, product, region, quantity, price, date_str))
        order_id += 1

        # ~2% duplicate rows (simulate upstream retry bugs)
        if random.random() < 0.02:
            rows.append((order_id - 1, customer_id, product, region, quantity, price, date_str))

    return rows

if __name__ == "__main__":
    rows = generate_rows(50000)
    with open("/tmp/raw_sales_insert.sql", "w") as f:
        f.write("USE operational;\n")
        for i in range(0, len(rows), 1000):
            batch = rows[i:i+1000]
            values = ",".join(
                "({}, {}, '{}', {}, {}, {}, '{}')".format(
                    r[0],
                    "NULL" if r[1] is None else r[1],
                    r[2].replace("'", ""),
                    "NULL" if r[3] is None else f"'{r[3]}'",
                    r[4],
                    r[5],
                    r[6]
                ) for r in batch
            )
            f.write(f"INSERT INTO raw_sales (order_id, customer_id, product_name, region, quantity, unit_price, order_date) VALUES {values};\n")
    print(f"Generated {len(rows)} rows -> /tmp/raw_sales_insert.sql")
