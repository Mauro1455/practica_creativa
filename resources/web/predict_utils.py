import os
import datetime, iso8601

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

# ------------------------------------------------------------------ #
# Cassandra connection (module-level, reused across requests)         #
# ------------------------------------------------------------------ #
_cassandra_session = None

def get_cassandra_session():
    global _cassandra_session
    if _cassandra_session is None:
        cassandra_host = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
        cluster = Cluster(
            [cassandra_host],
            port=9042,
            load_balancing_policy=RoundRobinPolicy(),
            protocol_version=4
        )
        _cassandra_session = cluster.connect("agile_data_science")
    return _cassandra_session

def get_flight_distance(origin, dest):
    """Get the distance between a pair of airport codes from Cassandra"""
    session = get_cassandra_session()
    stmt = session.prepare("""
        SELECT distance FROM origin_dest_distances
        WHERE origin = ? AND dest = ?
    """)
    row = session.execute(stmt, (origin, dest)).one()
    if row is None:
        raise ValueError(f"No distance found for route {origin}-{dest}")
    return row.distance


def get_regression_date_args(iso_date):
    """Given an ISO Date, return the day of year, day of month, day of week as the API expects them."""
    dt = iso8601.parse_date(iso_date)
    day_of_year = dt.timetuple().tm_yday
    day_of_month = dt.day
    day_of_week = dt.weekday()
    return {
        "DayOfYear": day_of_year,
        "DayOfMonth": day_of_month,
        "DayOfWeek": day_of_week,
    }


def get_current_timestamp():
    iso_now = datetime.datetime.now().isoformat()
    return iso_now
