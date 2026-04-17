# Power BI Desktop — water sensor dashboard

After the medallion pipeline runs:

```bash
python main.py
python scripts/export_for_powerbi.py
```

Exports live in **`data/exports/`**. Prefer **gold** CSVs for summary visuals; use **`silver_sensor_data.csv`** (or alias **`sensor_data.csv`**) for time-series and slicers.

| File | Layer | Use for |
|------|-------|---------|
| `gold_sensor_agg_by_location.csv` | Gold | Bars/columns: avg pressure & flow by site |
| `gold_leaks_by_location.csv` | Gold | Leak counts by location |
| `gold_daily_pressure_summary.csv` | Gold | Line charts by calendar day |
| `silver_sensor_data.csv` | Silver | Line chart vs **timestamp**, slicers, `is_leak` |

---

## 1. Get data

**Home** → **Get data** → **Text/CSV** → load the files you need → **Transform data** (recommended).

### Types (Power Query)

- **`silver_sensor_data` / `sensor_data`**: `timestamp` → **Date/Time**; `pressure`, `flow_rate` → **Decimal**; `is_leak` → **Whole number**.
- **Gold** files: decimals and integers as appropriate; `reading_date` → **Date** for daily summary.

**Close & apply.**

---

## 2. Model

- Relate **`location`** between **`gold_sensor_agg_by_location`** and **`silver_sensor_data`** if both are loaded (one-to-many from gold summary to detail).
- **`gold_leaks_by_location`** can stand alone for leak bar charts.

---

## 3. Suggested visuals

### KPI cards

From **silver** (detail): **Average** of `pressure` and `flow_rate` (not Sum).

### Leak counts (gold)

**Clustered bar**: `location` (axis), `leak_count` from **`gold_leaks_by_location`**.

### Averages by site (gold)

**Clustered column**: `location`, `avg_pressure`, `avg_flow` from **`gold_sensor_agg_by_location`**.

### Pressure over time (silver)

**Line chart**: `timestamp` on **X** — use the **timestamp column**, not only the **Date hierarchy** (expand hierarchy or pick non-hierarchy field so 5-minute steps show).

**Y-axis**: **Average** of `pressure` (not Sum).

### Leak detection (one station)

**Slicer** or **filter on visual**: `location` = e.g. **Mason Hill Booster**.

**Analytics** (format pane) → **Constant line** on Y at **30** (demo low-pressure threshold).

### Daily trend (gold)

**Line or area**: `reading_date` (X), `avg_pressure` (Y), **Legend** `location` — from **`gold_daily_pressure_summary`**.

---

## 4. PostgreSQL instead of CSV

**Get data** → **PostgreSQL** → load **`gold_*`** and **`silver_sensor_data`** tables. Same visuals; gold is the default for reporting consumers.
