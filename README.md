# FoodX — Big Data Analytics for Food Delivery

FoodX analyses food-delivery order history to understand demand patterns, restaurant performance, delivery operations, customer segments, restaurant recommendations, and distributed-processing behavior.

## Project components

- **PySpark + Spark SQL:** data loading, cleaning, and analytical queries.
- **Demand analysis:** hourly and day-of-week order patterns.
- **Restaurant and delivery analysis:** order volume, order value, ratings, kitchen preparation time (KPT), rider wait, and distance.
- **K-Means (Spark MLlib):** customer segmentation using order frequency, spending, average order value, and discounts.
- **ALS (Spark MLlib):** collaborative-filtering restaurant recommendations.
- **RDD MapReduce:** order-status and customer-spending aggregation using transformations and actions.
- **Partitioning:** measured baseline, `repartition()`, and `coalesce()` comparisons.
- **Storage comparison:** measured CSV and Parquet size/read performance when Windows Spark file output is configured.
- **Spark SQL Window Functions:** restaurant rankings, customer order sequence, previous order value, and running spend.
- **Spark optimization:** caching, persistence, and broadcast joins.
- **Streamlit + Plotly:** interactive dashboard using exported result CSVs.

## Repository layout

The active project uses this layout:

```text
FoodX/
├── app.py
├── requirements.txt
├── README.md
├── .gitignore
├── notebooks/
│   ├── FoodX_Big_Data_Analytics.ipynb
│   └── backups/
│       └── FoodX_Big_Data_Analytics_original.ipynb
├── scripts/
│   ├── run_phases_2_3.py
│   └── run_phases_4_5.py
├── src/foodx_analytics/
├── data/
│   ├── raw/order_history_kaggle_data.csv
│   ├── results/
│   ├── processed/
│   └── benchmarks/
└── FoodX_GitHub_Ready/       # untouched backup copy
```

## Run the dashboard locally

Use Python 3.10 or newer.

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

The dashboard opens in your browser. It reads existing analytical CSVs from `data/results/` and benchmark CSVs from `data/benchmarks/`.

## Re-run the analytics

Open `notebooks/FoodX_Big_Data_Analytics.ipynb` in Google Colab, upload the raw CSV when prompted, and run the notebook cells in order. The notebook preserves the original analytics and exports the six core analytical result tables.

The modular extensions can be run locally from PowerShell:

```powershell
python scripts\run_phases_2_3.py
python scripts\run_phases_4_5.py
```

The scripts use the actual file at `data/raw/order_history_kaggle_data.csv`. They write new result tables without replacing the existing core outputs.

## Windows Spark note

RDD transformations, Window Functions, caching, persistence, and broadcast joins can run with a local Spark session. Spark CSV/Parquet file output on Windows may additionally require Hadoop `winutils.exe`; configure `HADOOP_HOME` and its `bin` directory before running the storage comparison. If it is not configured, the Phase 2/3 runner skips only that benchmark and reports the reason instead of fabricating values.

## Important notes

- The dashboard displays exported analytical results; it does not retrain K-Means or ALS when the app starts.
- Run the notebook to regenerate core result CSVs after changing the source data or model settings.
- Run the modular scripts to regenerate RDD, partition, Window Function, caching, persistence, and broadcast-join outputs.
- The raw dataset is included for reproducibility. Check the dataset's original Kaggle licence/terms before redistributing it publicly.
