import os
import sys
import random
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([
    ("device_id", pa.string()),
    ("hour", pa.string()),
    ("avg_bpm", pa.float64()),
    ("max_bpm", pa.int32()),
    ("min_bpm", pa.int32()),
    ("total_steps", pa.int64()),
])


def generate(output_dir: str = "../data/gold") -> None:
    os.makedirs(output_dir, exist_ok=True)

    devices = ["device_001", "device_002", "device_003"]
    base_date = datetime(2024, 4, 22)
    rows: dict[str, list] = {col: [] for col in SCHEMA.names}

    random.seed(42)
    for device in devices:
        for day_offset in range(7):
            day = base_date + timedelta(days=day_offset)
            total_steps = random.randint(3000, 12000)
            for hour in range(6, 23):
                avg = round(random.uniform(55.0, 95.0), 2)
                spread = random.randint(10, 25)
                rows["device_id"].append(device)
                rows["hour"].append(day.strftime("%Y-%m-%d") + f" {hour:02d}")
                rows["avg_bpm"].append(avg)
                rows["max_bpm"].append(int(avg) + spread)
                rows["min_bpm"].append(max(40, int(avg) - spread))
                rows["total_steps"].append(total_steps)

    table = pa.table(rows, schema=SCHEMA)
    out_file = os.path.join(output_dir, "part-00000-sample.parquet")
    pq.write_table(table, out_file)
    open(os.path.join(output_dir, "_SUCCESS"), "w").close()

    end_date = (base_date + timedelta(days=6)).strftime("%Y-%m-%d")
    print(f"Generated {len(table)} rows in {output_dir}")
    print(f"Devices: {devices}")
    print(f"Period: {base_date.strftime('%Y-%m-%d')} to {end_date}")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "../data/gold"
    generate(out)
