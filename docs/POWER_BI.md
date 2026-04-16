# Power BI Desktop — build a water sensor dashboard

Follow these steps after the pipeline has run and you have exported CSVs:

```bash
python main.py
python scripts/export_for_powerbi.py
```

Files to use: `data/exports/sensor_data.csv`, `sensor_agg_by_location.csv`, `leaks_by_location.csv`.

---

## 1. Create a new report and load data

1. Open **Power BI Desktop** → **File** → **New**.
2. **Home** → **Get data** → **Text/CSV**.
3. Browse to `data/exports/` and select all three CSVs (Ctrl+click) → **Open** → **Transform Data** (recommended so you can fix types before loading).

---

## 2. Power Query — types and names

In the **Power Query Editor**:

### `sensor_data`

1. Select the **`timestamp`** column → **Transform** tab → **Data type** → **Date/Time** (or **Using Locale** if dates misparse).
2. Set **`pressure`** and **`flow_rate`** to **Decimal number**.
3. Set **`is_leak`** to **Whole number**.
4. Rename the query to **`Sensor detail`** (optional but clearer): right-click query name → **Rename**.

### `sensor_agg_by_location`

1. **`avg_pressure`** and **`avg_flow`** → **Decimal number**.
2. Rename query to **`By location`**.

### `leaks_by_location`

1. **`leak_count`** → **Whole number**.
2. Rename query to **`Leaks by location`**.

Click **Close & Apply** (Home tab).

---

## 3. Model (relationships)

1. Left sidebar → **Model** (diagram view).

**Recommended for this demo:**

- **`Leaks by location`** is already aggregated — use it **as-is** for bar charts (no relationship required to other tables).
- For **`Sensor detail`** + **`By location`**: drag **`location`** from **`By location`** onto **`location`** in **`Sensor detail`** to create a **one-to-many** relationship ( **`By location`** filters **`Sensor detail`**).

   If Power BI warns about ambiguity, ensure only **one** active relationship between those two tables on `location`.

- Do **not** relate **`Leaks by location`** to **`Sensor detail`** unless you rename one `location` column or use a shared *Location* dimension — for a portfolio demo, **keeping `Leaks by location` separate** is simplest.

---

## 4. Report — one-page dashboard

Go to **Report** view. **View** → turn on **Gridlines** and **Snap to grid** if you want alignment.

### Visual A — Title

**Insert** → **Text box** → title: `Water sensor monitoring` (optional subtitle: `Demo ETL output`).

### Visual B — KPI cards (overall)

1. **Visualizations** pane → **Card** (or **Multi-row card**).
2. Fields: from **`Sensor detail`**, drag **`pressure`** → set aggregation to **Average** (not Sum).

Add a second **Card**:

- **`flow_rate`** → **Average**.

Rename the field labels in the **Format** pane → **General** → **Title** if needed (e.g. `Avg pressure (psi)`, `Avg flow`).

### Visual C — Leaks by site (clustered bar)

1. **Clustered bar chart** (horizontal bars).
2. **Y-axis**: `location` from **`Leaks by location`**.
3. **X-axis**: `leak_count`.
4. **Format** → enable **Data labels** so counts show on bars.

### Visual D — Average pressure vs flow by location (clustered column)

1. **Clustered column chart**.
2. **X-axis**: `location` from **`By location`**.
3. **Y-axis**: drag **`avg_pressure`** and **`avg_flow`** (both appear as columns).

In **Format** → **Data colors**, pick two distinct colors (e.g. blue vs teal).

### Visual E — Pressure over time (line chart)

1. **Line chart**.
2. **X-axis**: `timestamp` from **`Sensor detail`**.
3. **Y-axis**: `pressure`.
4. **Format** → **X-axis** → **Type** = **Continuous** (works well when timestamp is true date/time).

### Visual F — Slicer (filter by site)

1. **Slicer** visual.
2. **Field**: `location` from **`Sensor detail`** (or from **`By location`** if you want fewer duplicates).

Arrange: **KPIs** on top, **leaks bar** and **column** middle, **line** wide at bottom, **slicer** on the side or top.

---

## 5. Optional polish

- **Filters on this page**: drag **`is_leak`** to **Filters on this page** → show **only** `1` to see only low-pressure events (then the line chart shows only “alert” points if you prefer).
- **Tooltip**: on the line chart, add **`location`** to **Tooltips** so hover shows site.
- **Theme**: **View** → **Themes** → pick a built-in theme, or **Customize current theme** → set **Main color** to a blue.

---

## 6. Save

**File** → **Save As** → name your `.pbix` (e.g. `water_sensor_dashboard.pbix`) anywhere outside the repo or inside your project folder if you track it in git (binary; many teams gitignore `.pbix`).

---

## If you connect to PostgreSQL instead

**Get data** → **PostgreSQL database** → server `localhost`, database name from `.env`, credentials from `.env`. Select **`sensor_data`** and **`sensor_agg_by_location`**. You can skip `leaks_by_location` and recreate the leak bar chart with a **measure**:

```dax
Leak rows = CALCULATE(COUNTROWS(sensor_data), sensor_data[is_leak] = 1)
```

Use that in a bar chart with **`location`** from `sensor_data` (may need to set visual to not sum incorrectly — use **COUNT** of rows or the measure above per location in a **matrix** first).

For portfolio use, **CSV + the steps above** is usually enough.
