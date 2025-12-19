import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

df_city = pd.read_parquet("madrid_q3_2025_city_tiles.parquet")

features = ["avg_d_kbps", "avg_u_kbps", "avg_lat_ms", "tests", "devices"]

X = df_city[features].astype(float)

# Robust scaling is usually better than standard scaling for heavy-tailed KPI distributions
scaler = RobustScaler()
X_scaled = scaler.fit_transform(X)

# Contamination is your assumption of anomaly rate (start small)
iso = IsolationForest(
    n_estimators=400,
    contamination=0.02,   # 2% anomalies (tune later)
    random_state=42,
    n_jobs=-1
)
iso.fit(X_scaled)

# IsolationForest outputs:
# - predict: -1 anomaly, +1 normal
# - decision_function: higher means more normal
df_city["is_anomaly"] = (iso.predict(X_scaled) == -1)
df_city["anomaly_score"] = -iso.decision_function(X_scaled)  # invert so higher = more anomalous

# Top anomalies
top = df_city.sort_values("anomaly_score", ascending=False).head(20)
print(top[["quadkey", "avg_d_kbps", "avg_u_kbps", "avg_lat_ms", "tests", "devices", "anomaly_score", "lon", "lat"]])

# Saving the anomalies list for later
df_city.sort_values("anomaly_score", ascending=False).to_csv("madrid_q3_2025_anomalies_ranked.csv", index=False)
