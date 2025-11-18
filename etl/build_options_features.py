# etl/build_options_features.py

import os
import glob
import pandas as pd

BASE_DIR = os.path.join("data", "options")
DAILY_DIR = os.path.join(BASE_DIR, "daily")
FEAT_DIR = os.path.join(BASE_DIR, "features")
os.makedirs(FEAT_DIR, exist_ok=True)

if __name__ == "__main__":
    files = glob.glob(os.path.join(DAILY_DIR, "options_*.parquet"))
    if not files:
        print("[WARN] No hay snapshots diarios en data/options/daily/")
        raise SystemExit(0)

    df_hist = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    # aseguramos tipos
    df_hist["as_of_date"] = pd.to_datetime(df_hist["as_of_date"])

    # agregados diarios por ticker
    if "impliedVolatility" in df_hist.columns:
        df_hist["impliedVolatility"] = df_hist["impliedVolatility"].astype(float)

    def agg_group(g):
        calls = g[g["type"] == "call"]
        puts = g[g["type"] == "put"]

        vol_calls = calls["volume"].fillna(0).sum() if "volume" in calls else 0
        vol_puts = puts["volume"].fillna(0).sum() if "volume" in puts else 0

        oi_total = g["openInterest"].fillna(0).sum() if "openInterest" in g else 0
        iv_mean = g["impliedVolatility"].mean() if "impliedVolatility" in g else None
        put_call_ratio = (vol_puts / vol_calls) if vol_calls > 0 else None

        return pd.Series({
            "IV_mean": iv_mean,
            "Volume_total": vol_calls + vol_puts,
            "OI_total": oi_total,
            "PutCallRatio": put_call_ratio
        })

    df_feats = df_hist.groupby(["ticker", "as_of_date"]).apply(agg_group).reset_index()

    out_path = os.path.join(FEAT_DIR, "options_features_daily.parquet")
    df_feats.to_parquet(out_path, index=False)
    print(f"[OK] Features diarios guardados en {out_path}")
    print(df_feats.head())
