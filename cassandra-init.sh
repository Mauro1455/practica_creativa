#!/bin/bash
set -e

echo "Installing cassandra-driver..."
pip install cassandra-driver -q

echo "Waiting for Cassandra to be ready..."
python3 <<'PYEOF'
import time
from cassandra.cluster import Cluster

while True:
    try:
        cluster = Cluster(['cassandra'])
        session = cluster.connect()
        print("Cassandra is ready!")
        break
    except Exception as e:
        print(f"Cassandra not ready yet: {e}, retrying in 5s...")
        time.sleep(5)

# Create keyspace and tables
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS agile_data_science
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace('agile_data_science')

session.execute("""
    CREATE TABLE IF NOT EXISTS origin_dest_distances (
        Origin text,
        Dest text,
        Distance double,
        PRIMARY KEY (Origin, Dest)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS flight_delay_ml_response (
        uuid text PRIMARY KEY,
        origin text,
        dest text,
        carrier text,
        dayofweek int,
        dayofyear int,
        dayofmonth int,
        depdelay double,
        distance double,
        prediction double,
        flightdate text,
        timestamp text,
        route text
    )
""")

print("Keyspace and tables created. Importing distances...")

import json
with open('/data/origin_dest_distances.jsonl') as f:
    for line in f:
        doc = json.loads(line)
        session.execute(
            "INSERT INTO origin_dest_distances (Origin, Dest, Distance) VALUES (%s, %s, %s)",
            (doc['Origin'], doc['Dest'], float(doc['Distance']))
        )

print("Importación completada")
cluster.shutdown()
PYEOF