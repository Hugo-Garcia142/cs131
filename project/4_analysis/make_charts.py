from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

BASE = Path("4_analysis")
CHARTS = BASE / "charts"
CHARTS.mkdir(exist_ok=True)

def clean_bucket(series):
    return series.astype(str).str.replace(r"^\d+:\s*", "", regex=True)

duration = pd.read_csv(BASE / "tip_by_duration.csv")
duration["duration_bucket"] = clean_bucket(duration["duration_bucket"])

plt.figure(figsize=(9, 5))
plt.bar(duration["duration_bucket"], duration["avg_recorded_tip"])
plt.xlabel("Trip duration")
plt.ylabel("Average recorded tip ($)")
plt.title("Average Recorded Tip Increases with Trip Duration")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig(CHARTS / "tip_by_duration.png", dpi=300)
plt.close()

distance = pd.read_csv(BASE / "tip_by_distance.csv")

plt.figure(figsize=(9, 5))
plt.bar(distance["distance_bucket"], distance["avg_recorded_tip"])
plt.xlabel("Trip distance")
plt.ylabel("Average recorded tip ($)")
plt.title("Average Recorded Tip Increases with Trip Distance")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig(CHARTS / "tip_by_distance.png", dpi=300)
plt.close()
hour = pd.read_csv(BASE / "tip_by_hour.csv")

plt.figure(figsize=(9, 5))
plt.plot(hour["pickup_hour"], hour["total_trips"], marker="o")
plt.xlabel("Pickup hour")
plt.ylabel("Total trips")
plt.title("Yellow Taxi Demand by Pickup Hour")
plt.xticks(range(24))
plt.tight_layout()
plt.savefig(CHARTS / "trips_by_hour.png", dpi=300)
plt.close()

yearly = pd.read_csv(BASE / "yearly.csv")

plt.figure(figsize=(9, 5))
plt.plot(yearly["pickup_year"], yearly["avg_total_amount"], marker="o")
plt.xlabel("Year")
plt.ylabel("Average total amount ($)")
plt.title("Average Trip Cost by Year")
plt.xticks(yearly["pickup_year"])
plt.tight_layout()
plt.savefig(CHARTS / "avg_total_by_year.png", dpi=300)
plt.close()

print("Created charts in 4_analysis/charts/")
