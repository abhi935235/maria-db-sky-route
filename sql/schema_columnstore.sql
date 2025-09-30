-- ColumnStore analytical copies of core tables
CREATE DATABASE IF NOT EXISTS openflights;
USE openflights;

CREATE TABLE IF NOT EXISTS airports_cs (
  id INT,
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
) ENGINE=Columnstore;

CREATE TABLE IF NOT EXISTS airlines_cs (
  id INT,
  name VARCHAR(255),
  alias VARCHAR(255),
  iata VARCHAR(4),
  icao VARCHAR(8),
  callsign VARCHAR(255),
  country VARCHAR(255),
  active VARCHAR(8)
) ENGINE=Columnstore;

CREATE TABLE IF NOT EXISTS routes_cs (
  airline VARCHAR(8),
  airline_id INT,
  src_airport VARCHAR(8),
  src_airport_id INT,
  dst_airport VARCHAR(8),
  dst_airport_id INT,
  codeshare VARCHAR(8),
  stops INT,
  equipment VARCHAR(64)
) ENGINE=Columnstore;

-- Example: load into ColumnStore from rowstore using INSERT-SELECT
-- INSERT INTO airports_cs SELECT * FROM airports;
-- INSERT INTO airlines_cs SELECT * FROM airlines;
-- INSERT INTO routes_cs SELECT airline, airline_id, src_airport, src_airport_id, dst_airport, dst_airport_id, codeshare, stops, equipment FROM routes;


