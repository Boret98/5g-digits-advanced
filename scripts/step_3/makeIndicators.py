#!/usr/bin/env python3
import argparse
import pandas as pd

def add_snapshot_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # Expected columns from Step 2 snapshot:
    # quadkey, avg_d_kbps, avg_u_kbps, avg_lat_ms, tests, devices, anomaly_score, lon, lat (optional)
    # plus is_anomaly (optional)

    # Thresholds (tune if you need; these are reasonable starting points)
    LAT_HIGH_MS = 80
    DL_LOW_KBPS = 5000   # 5 Mbps
    UL_LOW_KBPS = 1000   # 1 Mbps
    TESTS_LOW = 5

    # Severity tiers based on anomaly_score quantiles in THIS dataset (data-driven, not hard-coded)
    q90 = df["anomaly_score"].quantile(0.90)
    q95 = df["anomaly_score"].quantile(0.95)
    q99 = df["anomaly_score"].quantile(0.99)

    def indicator_row(r):
        issues = []

        # KPI-based issue labels (Ookla fields)
        if pd.notna(r.get("avg_lat_ms")) and r["avg_lat_ms"] >= LAT_HIGH_MS:
            issues.append(f"High latency (>= {LAT_HIGH_MS}ms)")
        if pd.notna(r.get("avg_d_kbps")) and r["avg_d_kbps"] <= DL_LOW_KBPS:
            issues.append(f"Low downlink (<= {DL_LOW_KBPS/1000:.1f}Mbps)")
        if pd.notna(r.get("avg_u_kbps")) and r["avg_u_kbps"] <= UL_LOW_KBPS:
            issues.append(f"Low uplink (<= {UL_LOW_KBPS/1000:.1f}Mbps)")
        if pd.notna(r.get("tests")) and r["tests"] < TESTS_LOW:
            issues.append(f"Low sample count (tests < {TESTS_LOW})")

        # Severity (score-based)
        score = r["anomaly_score"]
        if score >= q99:
            issues.append("Severity: CRITICAL (top 1%)")
        elif score >= q95:
            issues.append("Severity: HIGH (top 5%)")
        elif score >= q90:
            issues.append("Severity: MEDIUM (top 10%)")
        else:
            issues.append("Severity: LOW")

        if not issues:
            return "No indicator triggered"
        return "; ".join(issues)

    df["indicator_text"] = df.apply(indicator_row, axis=1)
    return df


def add_degradation_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # Expected columns from Step 2 degradation:
    # quadkey, d_avg_lat_ms, d_avg_d_kbps, d_avg_u_kbps, degradation_score, lon, lat, ...
    # plus is_degradation_anomaly (optional)

    LAT_DEGR_MS = 20
    DL_DEGR_KBPS = -5000
    UL_DEGR_KBPS = -1000

    q90 = df["degradation_score"].quantile(0.90)
    q95 = df["degradation_score"].quantile(0.95)
    q99 = df["degradation_score"].quantile(0.99)

    def indicator_row(r):
        issues = []

        if pd.notna(r.get("d_avg_lat_ms")) and r["d_avg_lat_ms"] >= LAT_DEGR_MS:
            issues.append(f"Latency degradation (>= +{LAT_DEGR_MS}ms)")
        if pd.notna(r.get("d_avg_d_kbps")) and r["d_avg_d_kbps"] <= DL_DEGR_KBPS:
            issues.append("Downlink degradation (<= -5Mbps)")
        if pd.notna(r.get("d_avg_u_kbps")) and r["d_avg_u_kbps"] <= UL_DEGR_KBPS:
            issues.append("Uplink degradation (<= -1Mbps)")

        score = r["degradation_score"]
        if score >= q99:
            issues.append("Severity: CRITICAL (top 1%)")
        elif score >= q95:
            issues.append("Severity: HIGH (top 5%)")
        elif score >= q90:
            issues.append("Severity: MEDIUM (top 10%)")
        else:
            issues.append("Severity: LOW")

        return "; ".join(issues) if issues else "No indicator triggered"

    df["indicator_text"] = df.apply(indicator_row, axis=1)
    return df


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["snapshot", "degradation"], required=True)
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    args = p.parse_args()

    df = pd.read_csv(args.input, dtype={"quadkey": "string"})

    if args.mode == "snapshot":
        if "anomaly_score" not in df.columns:
            raise SystemExit("Missing anomaly_score column (expected snapshot Step 2 output).")
        df = add_snapshot_indicators(df)
        df.to_csv(args.output, index=False)
    else:
        if "degradation_score" not in df.columns:
            raise SystemExit("Missing degradation_score column (expected degradation Step 2 output).")
        df = add_degradation_indicators(df)
        df.to_csv(args.output, index=False)

    print(f"Wrote: {args.output}")
    print("Columns now include: indicator_text")

if __name__ == "__main__":
    main()
