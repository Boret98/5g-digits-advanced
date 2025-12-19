import pandas as pd
from tabulate import tabulate

df = pd.read_csv("madrid_q3_2025_indicators.csv", dtype={"quadkey": "string"})
df = df.sort_values("anomaly_score", ascending=False).head(15)

cols = ["quadkey", "anomaly_score", "avg_lat_ms", "avg_d_kbps", "indicator_text"]
print(tabulate(df[cols], headers="keys", tablefmt="github", showindex=False))

df_city = pd.read_parquet("madrid_q3_2025_city_tiles.parquet")
an = pd.read_csv("madrid_q3_2025_anomalies_ranked.csv")
