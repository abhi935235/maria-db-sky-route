import os
import pandas as pd
from sqlalchemy import text
from .db import get_engine


def _ensure_schema(conn):
    # Core tables (rowstore)
    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS airports (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            iata VARCHAR(4),
            icao VARCHAR(8),
            latitude DOUBLE,
            longitude DOUBLE,
            altitude INT,
            timezone DOUBLE,
            dst VARCHAR(8),
            tz VARCHAR(64),
            type VARCHAR(64),
            source VARCHAR(64)
        ) ENGINE=InnoDB
        """
    ))

    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS airlines (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            alias VARCHAR(255),
            iata VARCHAR(4),
            icao VARCHAR(8),
            callsign VARCHAR(255),
            country VARCHAR(255),
            active VARCHAR(8)
        ) ENGINE=InnoDB
        """
    ))

    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS routes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            airline VARCHAR(8),
            airline_id INT,
            src_airport VARCHAR(8),
            src_airport_id INT,
            dst_airport VARCHAR(8),
            dst_airport_id INT,
            codeshare VARCHAR(8),
            stops INT,
            equipment VARCHAR(64)
        ) ENGINE=InnoDB
        """
    ))

    # Helpful indexes
    conn.execute(
        text("CREATE INDEX IF NOT EXISTS idx_airports_iata ON airports(iata)"))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_routes_srcdst ON routes(src_airport_id, dst_airport_id)"))


def load_openflights(data_dir: str | None = None):
    data_dir = data_dir or os.path.join(os.getcwd(), "data")
    airports_path = os.path.join(data_dir, "airports.dat")
    airlines_path = os.path.join(data_dir, "airlines.dat")
    routes_path = os.path.join(data_dir, "routes.dat")

    engine = get_engine()
    with engine.begin() as conn:
        _ensure_schema(conn)

        # Airports
        airports_cols = [
            "id", "name", "city", "country", "iata", "icao",
            "latitude", "longitude", "altitude", "timezone", "dst",
            "tz", "type", "source"
        ]
        airports_df = pd.read_csv(airports_path, header=None, names=airports_cols, na_values=[
                                  "\\N"], keep_default_na=True)
        airports_df = airports_df.drop_duplicates(subset=["id"])  # sanity
        tmp_airports = airports_df.where(pd.notnull(airports_df), None)
        conn.execute(text("DELETE FROM airports"))
        conn.execute(text(
            """
            INSERT INTO airports (
                id, name, city, country, iata, icao, latitude, longitude, altitude, timezone, dst, tz, type, source
            ) VALUES (
                :id, :name, :city, :country, :iata, :icao, :latitude, :longitude, :altitude, :timezone, :dst, :tz, :type, :source
            )
            """
        ), tmp_airports.to_dict(orient="records"))

        # Airlines
        airlines_cols = ["id", "name", "alias", "iata",
                         "icao", "callsign", "country", "active"]
        airlines_df = pd.read_csv(airlines_path, header=None, names=airlines_cols, na_values=[
                                  "\\N"], keep_default_na=True)
        airlines_df = airlines_df.drop_duplicates(subset=["id"])  # sanity
        tmp_airlines = airlines_df.where(pd.notnull(airlines_df), None)
        conn.execute(text("DELETE FROM airlines"))
        conn.execute(text(
            """
            INSERT INTO airlines (
                id, name, alias, iata, icao, callsign, country, active
            ) VALUES (
                :id, :name, :alias, :iata, :icao, :callsign, :country, :active
            )
            """
        ), tmp_airlines.to_dict(orient="records"))

        # Routes
        routes_cols = [
            "airline", "airline_id", "src_airport", "src_airport_id",
            "dst_airport", "dst_airport_id", "codeshare", "stops", "equipment"
        ]
        routes_df = pd.read_csv(routes_path, header=None, names=routes_cols, na_values=[
                                "\\N"], keep_default_na=True)
        tmp_routes = routes_df.where(pd.notnull(routes_df), None)
        conn.execute(text("DELETE FROM routes"))
        conn.execute(text(
            """
            INSERT INTO routes (
                airline, airline_id, src_airport, src_airport_id, dst_airport, dst_airport_id, codeshare, stops, equipment
            ) VALUES (
                :airline, :airline_id, :src_airport, :src_airport_id, :dst_airport, :dst_airport_id, :codeshare, :stops, :equipment
            )
            """
        ), tmp_routes.to_dict(orient="records"))
