from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import os

from .db import get_engine, run_query, run_scalar
from .loader import load_openflights


load_dotenv()

app = FastAPI(title="MariaDB OpenFlights API")


@app.get("/health")
def health():
    try:
        value = run_scalar("SELECT 1")
        return {"status": "ok", "db": value}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(exc)})


@app.post("/load")
def load():
    try:
        load_openflights()
        return {"status": "loaded"}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(exc)})


@app.get("/analytics/top-airports")
def top_airports(limit: int = Query(10, ge=1, le=100)):
    sql = """
        SELECT a.iata, a.name, COUNT(r.id) AS route_count
        FROM airports a
        JOIN routes r ON r.src_airport_id = a.id OR r.dst_airport_id = a.id
        GROUP BY a.id
        ORDER BY route_count DESC
        LIMIT :limit
    """
    rows = run_query(sql, {"limit": limit})
    return {"items": rows}


@app.get("/analytics/routes-between")
def routes_between(src: str = Query(..., min_length=3, max_length=4), dst: str = Query(..., min_length=3, max_length=4)):
    sql = """
        SELECT r.id, sa.iata AS src_iata, da.iata AS dst_iata, r.stops, r.equipment
        FROM routes r
        JOIN airports sa ON sa.id = r.src_airport_id
        JOIN airports da ON da.id = r.dst_airport_id
        WHERE sa.iata = :src AND da.iata = :dst
        ORDER BY r.stops, r.id
        LIMIT 200
    """
    rows = run_query(sql, {"src": src.upper(), "dst": dst.upper()})
    return {"items": rows}


@app.get("/analytics/nearest-airports")
def nearest_airports(lat: float, lon: float, k: int = Query(5, ge=1, le=50)):
    # Haversine formula on the fly in SQL (km)
    sql = """
        SELECT id, iata, name,
               6371 * 2 * ASIN(
                 SQRT(
                   POWER(SIN(RADIANS((:lat - latitude)/2)), 2) +
                   COS(RADIANS(latitude)) * COS(RADIANS(:lat)) * POWER(SIN(RADIANS((:lon - longitude)/2)), 2)
                 )
               ) AS distance_km
        FROM airports
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY distance_km ASC
        LIMIT :k
    """
    rows = run_query(sql, {"lat": lat, "lon": lon, "k": k})
    return {"origin": {"lat": lat, "lon": lon}, "items": rows}
