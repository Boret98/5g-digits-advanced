import duckdb
from shapely import wkt
from pathlib import Path

PARQUET_GLOB = "./2024-10-01_performance_mobile_tiles.parquet"

# Approx Madrid bbox (lon_min, lat_min, lon_max, lat_max)
# You can refine this with a proper city boundary later.
LON_MIN, LAT_MIN, LON_MAX, LAT_MAX = -3.90, 40.30, -3.50, 40.55

con = duckdb.connect(database=":memory:")

# DuckDB can query Parquet directly; we keep only required columns.
# 'tile' is WKT geometry in EPSG:4326 per Ookla docs.
query = f"""
SELECT
  CAST(quadkey AS VARCHAR) AS quadkey,
  avg_d_kbps,
  avg_u_kbps,
  avg_lat_ms,
  tests,
  devices,
  tile
FROM read_parquet('{PARQUET_GLOB}')
WHERE
  -- quick bbox filter by centroid if tile_x/tile_y exist; otherwise parse WKT later
  avg_d_kbps IS NOT NULL
  AND avg_u_kbps IS NOT NULL
  AND avg_lat_ms IS NOT NULL
"""

df = con.execute(query).df()

def tile_centroid_lon_lat(tile_wkt: str):
    geom = wkt.loads(tile_wkt)
    c = geom.centroid
    return c.x, c.y  # lon, lat

# Compute centroids (can take a bit; OK for city-scale subsets)
centroids = df["tile"].map(tile_centroid_lon_lat)
df["lon"] = [c[0] for c in centroids]
df["lat"] = [c[1] for c in centroids]

# BBox filter to Madrid area
df_city = df[(df["lon"].between(LON_MIN, LON_MAX)) & (df["lat"].between(LAT_MIN, LAT_MAX))].copy()
print("Madrid Data")
print(df_city.shape)

# Save as Parquet
# Save the filtered city subset (keeps column types; fast)
df_city.to_parquet("madrid_q3_2025_city_tiles.parquet", index=False)
