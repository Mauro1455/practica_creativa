#!/usr/bin/env python3
"""
import_distances_cassandra.py
Imports origin-destination distances from JSONL into Cassandra.

Usage:
    python3 resources/import_distances_cassandra.py
"""

import json
import os
import sys

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_FILE = os.path.join(BASE_PATH, "data", "origin_dest_distances.jsonl")


def main():
    # Connect to Cassandra
    print("Connecting to Cassandra...")
    cluster = Cluster(["127.0.0.1"], port=9042,
                      load_balancing_policy=RoundRobinPolicy(),
                      protocol_version=4)
    session = cluster.connect()

    # Create keyspace and table
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS agile_data_science
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace("agile_data_science")
    session.execute("""
        CREATE TABLE IF NOT EXISTS origin_dest_distances (
            origin   TEXT,
            dest     TEXT,
            distance DOUBLE,
            PRIMARY KEY (origin, dest)
        )
    """)
    print("✅ Keyspace and table ready")

    # Prepare insert statement
    insert_stmt = session.prepare("""
        INSERT INTO origin_dest_distances (origin, dest, distance)
        VALUES (?, ?, ?)
    """)

    # Read and insert all records
    count = 0
    with open(DATA_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            session.execute(insert_stmt, (
                record["Origin"],
                record["Dest"],
                float(record["Distance"])
            ))
            count += 1
            if count % 500 == 0:
                print(f"  Inserted {count} records...")

    print(f"✅ {count} distances imported into Cassandra")

    # Verify
    row = session.execute("SELECT COUNT(*) FROM origin_dest_distances").one()
    print(f"✅ Verified: {row[0]} records in Cassandra")

    cluster.shutdown()


if __name__ == "__main__":
    main()
