from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="FoodX | Big Data Analytics",
    page_icon="🍽️",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent

# The GitHub repository currently stores result CSVs in its root directory.
# The second location also supports the structured data/results layout.
def find_csv(filename: str, include_benchmarks: bool = False) -> Path | None:
    candidates = [BASE_DIR / filename, BASE_DIR / "data" / "results" / filename]
    if include_benchmarks:
        candidates.append(BASE_DIR / "data" / "benchmarks" / filename)
    return next((path for path in candidates if path.is_file()), None)


@st.cache_data(show_spinner=False)
def load_csv(filename: str, include_benchmarks: bool = False) -> pd.DataFrame:
    path = find_csv(filename, include_benchmarks=include_benchmarks)
    if path is None:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as exc:
        st.warning(f"Could not read {filename}: {exc}")
        return pd.DataFrame()


restaurant = load_csv("restaurant_analysis.csv")
hourly = load_csv("hourly_demand.csv")
daily = load_csv("daily_demand.csv")
delivery = load_csv("delivery_analysis.csv")
segments = load_csv("customer_segments.csv")
recommendations = load_csv("recommendations.csv")
rdd_status = load_csv("rdd_order_status_counts.csv")
rdd_customer_spend = load_csv("rdd_customer_spend.csv")
partition_benchmark = load_csv("partition_benchmark.csv", include_benchmarks=True)
storage_benchmark = load_csv("storage_benchmark.csv", include_benchmarks=True)
window_rankings = load_csv("window_restaurant_rankings.csv")
window_customer_orders = load_csv("window_customer_orders.csv")
broadcast_summary = load_csv("broadcast_restaurant_summary.csv")
persistence_metrics = load_csv("persistence_metrics.csv")

st.title("🍽️ FoodX")
st.caption("Food Delivery Demand, Customer Behaviour & Restaurant Recommendation Analytics")

required_restaurant_columns = {"Restaurant name", "total_orders", "delivered_orders"}
if restaurant.empty or not required_restaurant_columns.issubset(restaurant.columns):
    st.error(
        "Restaurant analysis data is missing or has an unexpected format. "
        "Keep restaurant_analysis.csv beside app.py or in data/results/."
    )
    st.stop()

# Ensure numeric columns are usable even if a CSV was exported as text.
for frame, columns in [
    (restaurant, ["total_orders", "delivered_orders", "average_order_value", "average_rating"]),
    (hourly, ["Order Hour", "total_orders"]),
    (daily, ["total_orders"]),
    (delivery, ["avg_kpt", "avg_rider_wait", "avg_distance"]),
    (segments, ["customer_count", "avg_orders", "avg_spending", "avg_order_value", "avg_discount"]),
    (recommendations, ["predicted_preference", "average_rating", "total_orders"]),
    (rdd_status, ["total_orders"]),
    (rdd_customer_spend, ["total_spending"]),
    (partition_benchmark, ["partitions", "record_count", "elapsed_seconds"]),
    (storage_benchmark, ["record_count", "storage_bytes", "read_elapsed_seconds"]),
    (window_rankings, ["total_orders", "average_order_value", "restaurant_rank", "order_share"]),
    (window_customer_orders, ["Order ID", "Total", "customer_order_number", "previous_order_total",
                              "running_customer_spend"]),
    (broadcast_summary, ["total_orders", "orders_with_customer_id"]),
    (persistence_metrics, ["input_records", "cached_records", "persisted_records"]),
]:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

# KPI row
c1, c2, c3 = st.columns(3)
c1.metric("Restaurants analysed", f"{len(restaurant):,}")
c2.metric("Orders in restaurant summary", f"{int(restaurant['total_orders'].fillna(0).sum()):,}")
c3.metric("Delivered orders in summary", f"{int(restaurant['delivered_orders'].fillna(0).sum()):,}")

st.divider()
st.subheader("Restaurant performance")
top_restaurants = restaurant.sort_values("total_orders", ascending=False)
color_column = "average_rating" if "average_rating" in top_restaurants.columns else None
st.plotly_chart(
    px.bar(
        top_restaurants,
        x="Restaurant name",
        y="total_orders",
        color=color_column,
        title="Orders by restaurant",
        labels={"total_orders": "Total orders", "average_rating": "Average rating"},
        template="plotly_dark",
    ),
    use_container_width=True,
)
st.dataframe(restaurant, use_container_width=True, hide_index=True)

left, right = st.columns(2)
with left:
    st.subheader("Demand by hour")
    if not hourly.empty and {"Order Hour", "total_orders"}.issubset(hourly.columns):
        hourly_sorted = hourly.sort_values("Order Hour")
        st.plotly_chart(
            px.line(hourly_sorted, x="Order Hour", y="total_orders", markers=True,
                    title="Orders across the day", template="plotly_dark"),
            use_container_width=True,
        )
    else:
        st.info("Hourly demand data is not available.")

with right:
    st.subheader("Demand by day")
    if not daily.empty and {"Order Day", "total_orders"}.issubset(daily.columns):
        st.plotly_chart(
            px.bar(daily, x="Order Day", y="total_orders", title="Orders by day of week",
                   template="plotly_dark"),
            use_container_width=True,
        )
    else:
        st.info("Daily demand data is not available.")

st.divider()
st.subheader("Delivery operations")
if not delivery.empty and {"Restaurant name", "avg_kpt", "avg_rider_wait"}.issubset(delivery.columns):
    st.plotly_chart(
        px.bar(
            delivery,
            x="Restaurant name",
            y=["avg_kpt", "avg_rider_wait"],
            barmode="group",
            title="Average kitchen preparation and rider wait time",
            labels={"value": "Minutes", "variable": "Metric"},
            template="plotly_dark",
        ),
        use_container_width=True,
    )
    st.dataframe(delivery, use_container_width=True, hide_index=True)
else:
    st.info("Delivery analysis data is not available or is missing required columns.")

left, right = st.columns(2)
with left:
    st.subheader("Customer segments (K-Means)")
    if not segments.empty and {"Customer Segment", "customer_count"}.issubset(segments.columns):
        st.plotly_chart(
            px.pie(segments, names="Customer Segment", values="customer_count", hole=0.45,
                   title="Customer distribution by segment", template="plotly_dark"),
            use_container_width=True,
        )
        st.dataframe(segments, use_container_width=True, hide_index=True)
    else:
        st.info("Customer segmentation data is not available.")

with right:
    st.subheader("ALS restaurant recommendations")
    if not recommendations.empty:
        show_columns = [
            column for column in
            ["Restaurant name", "predicted_preference", "average_rating", "total_orders"]
            if column in recommendations.columns
        ]
        st.dataframe(recommendations[show_columns], use_container_width=True, hide_index=True)
    else:
        st.info("Recommendation data is not available.")

st.divider()
st.subheader("RDD MapReduce analytics")
if not rdd_status.empty and {"Order Status", "total_orders"}.issubset(rdd_status.columns):
    st.plotly_chart(
        px.bar(
            rdd_status,
            x="Order Status",
            y="total_orders",
            title="Order counts from RDD reduceByKey",
            template="plotly_dark",
        ),
        use_container_width=True,
    )
    st.dataframe(rdd_status, use_container_width=True, hide_index=True)
else:
    st.info("Run scripts/run_phases_2_3.py to generate RDD results.")

if not rdd_customer_spend.empty:
    st.dataframe(
        rdd_customer_spend.head(10),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Partitioning and storage comparison")
left, right = st.columns(2)
with left:
    if not partition_benchmark.empty:
        st.plotly_chart(
            px.bar(
                partition_benchmark,
                x="strategy",
                y="elapsed_seconds",
                color="partitions",
                title="Measured partition strategy time",
                labels={"elapsed_seconds": "Seconds"},
                template="plotly_dark",
            ),
            use_container_width=True,
        )
        st.dataframe(partition_benchmark, use_container_width=True, hide_index=True)
    else:
        st.info("Partition benchmark data is not available.")
with right:
    if not storage_benchmark.empty:
        st.plotly_chart(
            px.bar(
                storage_benchmark,
                x="format",
                y="storage_bytes",
                title="CSV versus Parquet storage size",
                labels={"storage_bytes": "Bytes"},
                template="plotly_dark",
            ),
            use_container_width=True,
        )
        st.dataframe(storage_benchmark, use_container_width=True, hide_index=True)
    else:
        st.info("Storage benchmark data is not available on this machine.")

st.subheader("Spark SQL Window Functions")
if not window_rankings.empty:
    st.plotly_chart(
        px.bar(
            window_rankings.head(10),
            x="Restaurant name",
            y="total_orders",
            color="restaurant_rank",
            title="Restaurant ranking from Window Functions",
            template="plotly_dark",
        ),
        use_container_width=True,
    )
    st.dataframe(window_rankings, use_container_width=True, hide_index=True)
else:
    st.info("Window Function results are not available.")

if not window_customer_orders.empty:
    st.caption("Sample of customer order sequence and running-spend Window results.")
    st.dataframe(
        window_customer_orders.head(20),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Spark optimization analytics")
left, right = st.columns(2)
with left:
    if not broadcast_summary.empty:
        st.dataframe(
            broadcast_summary,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Broadcast-join results are not available.")
with right:
    if not persistence_metrics.empty:
        st.dataframe(
            persistence_metrics,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Cache and persistence metrics are not available.")

st.divider()
st.caption(
    "Dashboard displays precomputed outputs exported by the PySpark notebook and modular "
    "analytics scripts. Run the relevant script to regenerate each output."
)
