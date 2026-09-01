"""
Simulates a daily CSV file drop for the second sales channel.
Slightly different schema quirks than the MySQL source (to make the
Transform stage's schema-unification step meaningful).
"""
import random
import csv
import datetime
import sys

PRODUCTS = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Thingamajig",
            "Doohickey", "Gizmo Pro", "Gizmo Lite", "SuperTool", "MegaKit"]
REGIONS = ["North", "South", "East", "West", "Central"]

def generate(date_str, n=8000, out_path="/tmp/csv_drop.csv"):
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["txn_id", "cust_id", "item", "region", "qty", "price", "txn_date"])
        for i in range(1, n + 1):
            writer.writerow([
                f"CSV-{date_str}-{i}",
                random.randint(1, 5000),
                random.choice(PRODUCTS),
                random.choice(REGIONS),
                random.randint(1, 15),
                round(random.uniform(5, 500), 2),
                date_str
            ])
    print(f"Generated {n} rows -> {out_path}")

if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.date.today().isoformat()
    generate(date_str)
