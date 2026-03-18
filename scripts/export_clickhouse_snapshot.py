from __future__ import annotations

import argparse
import os
from pathlib import Path

import clickhouse_connect
import pyarrow.parquet as pq


TABLES = {
    "dm_datamart_monthly": {
        "date_column": "DT",
        "order_hint": "SUBS_ID",
    },
    "hfct_subs_short": {
        "date_column": "eff_dt",
        "order_hint": "subscription_id",
    },
}


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="DWH",
        secure=os.environ.get("CLICKHOUSE_PROTOCOL", "https") == "https",
        verify=False,
    )


def export_table(client, table_name: str, snapshot_date: str, output_dir: Path) -> Path:
    config = TABLES[table_name]
    date_column = config["date_column"]
    order_hint = config["order_hint"]
    output_path = output_dir / f"{table_name}_{snapshot_date}.parquet"

    query = (
        f"SELECT * FROM DWH.{table_name} "
        f"WHERE {date_column} = toDate('{snapshot_date}') "
        f"ORDER BY {order_hint}"
    )

    writer = None
    total_rows = 0
    try:
        with client.query_arrow_stream(query) as stream:
            for batch in stream:
                if writer is None:
                    writer = pq.ParquetWriter(output_path, batch.schema, compression="snappy")
                writer.write_batch(batch)
                total_rows += batch.num_rows
                print(f"{table_name}: wrote {total_rows} rows", flush=True)
    finally:
        if writer is not None:
            writer.close()

    print(f"{table_name}: completed -> {output_path}", flush=True)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Export ClickHouse snapshot slices to parquet.")
    parser.add_argument("--date", required=True, help="Snapshot date in YYYY-MM-DD format")
    parser.add_argument(
        "--output-dir",
        default="data/raw",
        help="Directory for parquet exports",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=list(TABLES.keys()),
        choices=list(TABLES.keys()),
        help="Tables to export",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir) / args.date
    output_dir.mkdir(parents=True, exist_ok=True)

    client = get_client()
    for table_name in args.tables:
        export_table(client, table_name, args.date, output_dir)


if __name__ == "__main__":
    main()
