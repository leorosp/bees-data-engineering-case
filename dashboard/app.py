from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

from bees_case.dashboard_data import DashboardData, load_dashboard_data, load_demo_dashboard_data

REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_ROOT = REPO_ROOT / "dashboard" / "demo_data"
KNOWN_OUTPUTS = {
    "Local Output": REPO_ROOT / "local_output",
    "Quality Gate Exercise": REPO_ROOT / "local_output_bad",
}

BEES_YELLOW = "#f5c400"
BEES_BLACK = "#141414"
BEES_CHARCOAL = "#232323"
BEES_RED = "#a52333"
BEES_GREEN = "#5f7c35"
BEES_CREAM = "#fff9de"


def inject_styles() -> None:
    st.markdown(
        f"""
        <style>
        .stApp {{
            background: linear-gradient(180deg, #fffef7 0%, #fff9de 100%);
            color: {BEES_BLACK};
        }}
        .block-container {{
            padding-top: 2rem;
            padding-bottom: 3rem;
            max-width: 1200px;
        }}
        h1, h2, h3 {{
            color: {BEES_BLACK};
            letter-spacing: -0.02em;
        }}
        .hero-card {{
            background: linear-gradient(135deg, {BEES_BLACK} 0%, {BEES_CHARCOAL} 100%);
            border-radius: 24px;
            padding: 1.5rem 1.7rem;
            color: white;
            border: 1px solid rgba(245, 196, 0, 0.24);
            box-shadow: 0 18px 40px rgba(20, 20, 20, 0.12);
            margin-bottom: 1rem;
        }}
        .hero-kicker {{
            color: {BEES_YELLOW};
            font-size: 0.88rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}
        .hero-title {{
            font-size: 2rem;
            font-weight: 800;
            margin: 0.45rem 0 0.35rem 0;
            line-height: 1.05;
        }}
        .hero-copy {{
            margin: 0;
            color: rgba(255, 255, 255, 0.82);
            font-size: 1rem;
            line-height: 1.55;
            max-width: 60rem;
        }}
        .badge-row {{
            display: flex;
            gap: 0.65rem;
            flex-wrap: wrap;
            margin-top: 1rem;
        }}
        .badge {{
            display: inline-flex;
            align-items: center;
            padding: 0.45rem 0.85rem;
            border-radius: 999px;
            font-size: 0.9rem;
            font-weight: 700;
            background: rgba(255, 255, 255, 0.08);
            color: white;
            border: 1px solid rgba(255, 255, 255, 0.12);
        }}
        .badge--healthy {{
            background: rgba(95, 124, 53, 0.18);
            border-color: rgba(95, 124, 53, 0.35);
            color: #d6f0b0;
        }}
        .badge--attention {{
            background: rgba(165, 35, 51, 0.18);
            border-color: rgba(165, 35, 51, 0.35);
            color: #ffd7dc;
        }}
        [data-testid="stMetric"] {{
            background: rgba(255, 255, 255, 0.88);
            border: 1px solid rgba(20, 20, 20, 0.08);
            border-radius: 18px;
            padding: 0.9rem 1rem;
            box-shadow: 0 10px 24px rgba(20, 20, 20, 0.06);
        }}
        [data-testid="stMetricLabel"] {{
            color: #575757;
            font-weight: 600;
        }}
        [data-testid="stMetricValue"] {{
            color: {BEES_BLACK};
            font-weight: 800;
        }}
        .section-label {{
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #7a6a1e;
            font-weight: 700;
            margin-bottom: 0.25rem;
        }}
        .section-copy {{
            color: #4f4f4f;
            margin-bottom: 0;
        }}
        div[data-testid="stDataFrame"] {{
            border-radius: 18px;
            overflow: hidden;
            border: 1px solid rgba(20, 20, 20, 0.08);
        }}
        section[data-testid="stSidebar"] {{
            background: #fffdf4;
            border-right: 1px solid rgba(20, 20, 20, 0.06);
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def discover_available_outputs() -> dict[str, Path]:
    return {label: path for label, path in KNOWN_OUTPUTS.items() if path.exists()}


def filter_gold(gold: pd.DataFrame, selected_types: list[str], selected_states: list[str]) -> pd.DataFrame:
    filtered = gold.copy()
    if selected_types:
        filtered = filtered[filtered["brewery_type"].isin(selected_types)]
    if selected_states:
        filtered = filtered[filtered["state_province"].isin(selected_states)]
    return filtered.sort_values(["brewery_count", "state_province", "brewery_type"], ascending=[False, True, True])


def build_health_state(quality: pd.DataFrame) -> str:
    if quality.empty:
        return "Unknown"
    return "Attention" if (quality["status"].str.lower() == "fail").any() else "Healthy"


def latest_run_id(data: DashboardData) -> str:
    if not data.execution.empty and "run_id" in data.execution.columns:
        return str(data.execution.iloc[-1]["run_id"])
    if not data.quality.empty and "run_id" in data.quality.columns:
        return str(data.quality.iloc[-1]["run_id"])
    if not data.gold.empty and "run_id" in data.gold.columns:
        return str(data.gold.iloc[-1]["run_id"])
    return "n/a"


def latest_execution_summary(execution: pd.DataFrame) -> dict[str, str]:
    if execution.empty:
        return {
            "stage": "n/a",
            "status": "n/a",
            "records_in": "n/a",
            "records_out": "n/a",
            "details": "No execution event was found.",
            "timestamp": "n/a",
            "source_type": "n/a",
            "fallback_used": "n/a",
            "fallback_reason": "",
            "pages_requested": "n/a",
            "records_fetched": "n/a",
        }

    latest = execution.sort_values("event_timestamp_utc").iloc[-1]
    timestamp = latest.get("event_timestamp_utc")
    timestamp_text = timestamp.strftime("%Y-%m-%d %H:%M UTC") if pd.notna(timestamp) else "n/a"
    return {
        "stage": str(latest.get("stage", "n/a")),
        "status": str(latest.get("status", "n/a")).title(),
        "records_in": str(latest.get("records_in", "n/a")),
        "records_out": str(latest.get("records_out", "n/a")),
        "details": str(latest.get("details", "No execution details are available.")),
        "timestamp": timestamp_text,
        "source_type": str(latest.get("source_type", "n/a")),
        "fallback_used": "yes" if bool(latest.get("fallback_used", False)) else "no",
        "fallback_reason": str(latest.get("fallback_reason", "")),
        "pages_requested": str(latest.get("pages_requested", "n/a")),
        "records_fetched": str(latest.get("records_fetched", "n/a")),
    }


def format_stage_label(stage: str) -> str:
    if not stage or stage == "n/a":
        return "Pipeline"
    normalized = stage.replace("_", " ").title().replace("Pyspark", "PySpark")
    if normalized == "Local PySpark Pipeline":
        return "Local PySpark Pipeline"
    return normalized


def make_type_chart(gold: pd.DataFrame):
    chart_df = (
        gold.groupby("brewery_type", as_index=False)["brewery_count"]
        .sum()
        .sort_values("brewery_count", ascending=False)
    )
    fig = px.bar(
        chart_df,
        x="brewery_type",
        y="brewery_count",
        text="brewery_count",
        labels={"brewery_type": "Brewery Type", "brewery_count": "Breweries"},
    )
    fig.update_traces(marker_color=BEES_YELLOW, textposition="outside")
    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=20, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=BEES_BLACK),
    )
    return fig


def make_state_chart(gold: pd.DataFrame):
    chart_df = (
        gold.groupby("state_province", as_index=False)["brewery_count"]
        .sum()
        .sort_values("brewery_count", ascending=True)
    )
    fig = px.bar(
        chart_df,
        x="brewery_count",
        y="state_province",
        orientation="h",
        text="brewery_count",
        labels={"state_province": "State", "brewery_count": "Breweries"},
    )
    fig.update_traces(marker_color=BEES_BLACK, textposition="outside")
    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=20, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=BEES_BLACK),
    )
    return fig


def make_quality_chart(quality: pd.DataFrame):
    chart_df = (
        quality.assign(
            status=quality["status"].replace({"pass": "Passed", "fail": "Failed"})
        )
        .groupby(["layer", "status"], as_index=False)["check_name"]
        .count()
        .rename(columns={"check_name": "checks"})
    )
    fig = px.bar(
        chart_df,
        x="layer",
        y="checks",
        color="status",
        barmode="group",
        labels={"layer": "Layer", "checks": "Checks"},
        color_discrete_map={"Passed": BEES_GREEN, "Failed": BEES_RED},
    )
    fig.update_layout(
        height=330,
        margin=dict(l=10, r=10, t=20, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=BEES_BLACK),
        legend_title_text="Status",
    )
    return fig


def quality_status_by_layer(quality: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, str | int]] = []
    for layer, layer_df in quality.groupby("layer"):
        failed = int((layer_df["status"].str.lower() == "fail").sum())
        status = "Attention" if failed else "Passed"
        rows.append(
            {
                "layer": str(layer).title(),
                "status": status,
                "failed_checks": failed,
                "total_checks": int(len(layer_df)),
            }
        )
    return pd.DataFrame(rows, columns=["layer", "status", "failed_checks", "total_checks"]).sort_values(
        "layer"
    )


def describe_source(data: DashboardData) -> str:
    if data.source_mode == "demo":
        return "Demo Dataset"
    if not data.execution.empty and "source_type" in data.execution.columns:
        latest = data.execution.sort_values("event_timestamp_utc").iloc[-1]
        source_type = str(latest.get("source_type", "")).lower()
        fallback_used = bool(latest.get("fallback_used", False))
        if source_type == "api":
            return "Open Brewery DB API"
        if source_type == "sample" and fallback_used:
            return "Local Sample Dataset (API fallback)"
        if source_type == "sample":
            return "Local Sample Dataset"
    return data.source_root.name.replace("_", " ").title()


def load_selected_data(source_label: str, available_outputs: dict[str, Path]) -> tuple[DashboardData, str | None]:
    if source_label == "Demo Dataset":
        return load_demo_dashboard_data(DEMO_ROOT), None

    target = available_outputs.get(source_label)
    if not target:
        return (
            load_demo_dashboard_data(DEMO_ROOT),
            "Local artifacts are not available yet. The dashboard is showing the demo dataset.",
        )

    try:
        return load_dashboard_data(target), None
    except FileNotFoundError:
        return (
            load_demo_dashboard_data(DEMO_ROOT),
            f"{source_label} is incomplete right now. The dashboard is showing the demo dataset.",
        )


def run_local_pipeline(output_dir: str = "local_output") -> tuple[bool, str]:
    command = [sys.executable, "scripts/run_local_pyspark_demo.py", "--output-dir", output_dir]
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode == 0:
        return True, f"{output_dir} was generated successfully."
    return False, (
        "Demo mode is active for presentation. "
        "Local execution can be enabled in a fully configured runtime."
    )


def render_hero(source_label: str, health_label: str) -> None:
    health_class = "badge--healthy" if health_label == "Healthy" else "badge--attention"
    st.markdown(
        f"""
        <section class="hero-card">
            <div class="hero-kicker">BEES Pipeline Overview</div>
            <div class="hero-title">Brewery distribution and pipeline health</div>
            <p class="hero-copy">
                Executive summary of gold-layer metrics, geographic coverage, and pipeline quality status.
            </p>
            <div class="badge-row">
                <span class="badge">Data source: {source_label}</span>
                <span class="badge {health_class}">Pipeline health: {health_label}</span>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_kpis(gold: pd.DataFrame, quality: pd.DataFrame, latest_run: str, health_label: str) -> None:
    total_breweries = int(gold["brewery_count"].sum()) if not gold.empty else 0
    total_types = int(gold["brewery_type"].nunique()) if not gold.empty else 0
    total_states = int(gold["state_province"].nunique()) if not gold.empty else 0
    failed_checks = int((quality["status"].str.lower() == "fail").sum()) if not quality.empty else 0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Breweries", f"{total_breweries}")
    col2.metric("Brewery Types", f"{total_types}")
    col3.metric("States Covered", f"{total_states}")
    col4.metric("Pipeline Health", health_label)
    col5.metric("Latest Run", latest_run)
    if failed_checks:
        st.caption(f"{failed_checks} quality check(s) require attention.")


def render_overview_tab(data: DashboardData, filtered_gold: pd.DataFrame) -> None:
    summary = latest_execution_summary(data.execution)
    quality_by_layer = quality_status_by_layer(data.quality)
    stage_label = format_stage_label(summary["stage"])
    execution_text = (
        f"{stage_label} completed successfully on {summary['timestamp']}."
        if summary["status"].lower() == "success" and summary["timestamp"] != "n/a"
        else (
            f"{stage_label} finished with status {summary['status']}."
            if summary["status"] != "n/a"
            else "No execution summary is available right now."
        )
    )

    left, right = st.columns([1.35, 1])
    with left:
        st.markdown("#### Breweries by Type")
        if filtered_gold.empty:
            st.info("No rows match the current filters.")
        else:
            st.plotly_chart(
                make_type_chart(filtered_gold),
                use_container_width=True,
                key="overview_breweries_by_type",
            )

    with right:
        st.markdown("#### Pipeline Quality Checks")
        layer_columns = st.columns(max(len(quality_by_layer), 1))
        if quality_by_layer.empty:
            layer_columns[0].metric("Pipeline", "Unknown")
        else:
            for col, row in zip(layer_columns, quality_by_layer.itertuples(index=False)):
                col.metric(row.layer, row.status)

        st.markdown("#### Latest Execution Summary")
        st.markdown(
            f"""
            <div class="section-label">Latest execution</div>
            <p class="section-copy">
                {execution_text}
            </p>
            """,
            unsafe_allow_html=True,
        )
        metric_a, metric_b = st.columns(2)
        metric_a.metric("Source Records", summary["records_in"])
        metric_b.metric("Curated Records", summary["records_out"])
        st.caption(summary["details"])
        st.caption(
            "Effective source: "
            f"{summary['source_type']} | "
            f"Fallback used: {summary['fallback_used']} | "
            f"Pages requested: {summary['pages_requested']} | "
            f"Records fetched: {summary['records_fetched']}"
        )


def render_analytics_tab(filtered_gold: pd.DataFrame) -> None:
    if filtered_gold.empty:
        st.info("No rows match the current filters.")
        return

    chart_a, chart_b = st.columns(2)
    with chart_a:
        st.markdown("#### Breweries by Type")
        st.plotly_chart(
            make_type_chart(filtered_gold),
            use_container_width=True,
            key="analytics_breweries_by_type",
        )
    with chart_b:
        st.markdown("#### Top States by Brewery Count")
        st.plotly_chart(
            make_state_chart(filtered_gold),
            use_container_width=True,
            key="analytics_top_states",
        )

    st.markdown("#### Filtered Gold Output")
    display_df = filtered_gold.rename(
        columns={
            "brewery_type": "Brewery Type",
            "country": "Country",
            "state_province": "State",
            "brewery_count": "Breweries",
            "run_id": "Run ID",
            "generated_at_utc": "Generated At UTC",
        }
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_operational_tab(data: DashboardData, available_outputs: dict[str, Path]) -> None:
    chart_col, table_col = st.columns([1.05, 1])
    with chart_col:
        st.markdown("#### Quality Checks by Layer")
        st.plotly_chart(
            make_quality_chart(data.quality),
            use_container_width=True,
            key="operations_quality_by_layer",
        )

    with table_col:
        st.markdown("#### Latest Execution Summary")
        execution_df = data.execution.sort_values("event_timestamp_utc", ascending=False).rename(
            columns={
                "layer": "Layer",
                "stage": "Stage",
                "status": "Status",
                "run_id": "Run ID",
                "records_in": "Source Records",
                "records_out": "Curated Records",
                "details": "Details",
                "event_timestamp_utc": "Timestamp UTC",
                "requested_source_mode": "Requested Source Mode",
                "source_type": "Effective Source",
                "fallback_used": "Fallback Used",
                "fallback_reason": "Fallback Reason",
                "source_api_base_url": "API URL",
                "sample_file": "Sample File",
                "pages_requested": "Pages Requested",
                "pages_with_data": "Pages With Data",
                "records_fetched": "Records Fetched",
            }
        )
        execution_df["Status"] = execution_df["Status"].replace(
            {"success": "Success", "failed": "Failed"}
        )
        if "Fallback Used" in execution_df.columns:
            execution_df["Fallback Used"] = execution_df["Fallback Used"].replace(
                {True: "yes", False: "no"}
            )
        st.dataframe(execution_df, use_container_width=True, hide_index=True)

    st.markdown("#### Detailed Quality Checks")
    quality_df = data.quality.sort_values(["layer", "status", "check_name"]).rename(
        columns={
            "check_name": "Check",
            "checked_at_utc": "Checked At UTC",
            "layer": "Layer",
            "message": "Message",
            "metric_name": "Metric",
            "metric_value": "Metric Value",
            "run_id": "Run ID",
            "status": "Status",
        }
    )
    quality_df["Status"] = quality_df["Status"].replace({"pass": "Passed", "fail": "Failed"})
    st.dataframe(quality_df, use_container_width=True, hide_index=True)

    with st.expander("Operational details", expanded=False):
        availability = ["Demo Dataset: available"]
        for label in KNOWN_OUTPUTS:
            availability.append(
                f"{label}: {'available' if label in available_outputs else 'unavailable'}"
            )

        st.markdown("**How to run locally**")
        st.code(
            "\n".join(
                [
                    'pip install -e ".[dev,local,dashboard]"',
                    "python scripts/run_api_pyspark_pipeline.py --output-dir local_output",
                    "# or, for a deterministic demo:",
                    "python scripts/run_local_pyspark_demo.py",
                    "python -m streamlit run dashboard/app.py",
                ]
            ),
            language="bash",
        )
        st.markdown("**Available data sources**")
        for item in availability:
            st.write(f"- {item}")


def main() -> None:
    st.set_page_config(
        page_title="BEES Data Engineering Case Dashboard",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    inject_styles()

    available_outputs = discover_available_outputs()
    source_options = ["Demo Dataset", *available_outputs.keys()]

    st.sidebar.title("Controls")
    selected_source = st.sidebar.radio("Data Source", options=source_options, index=0)

    feedback_level = st.session_state.pop("pipeline_feedback_level", None)
    feedback_message = st.session_state.pop("pipeline_feedback_message", None)
    if st.sidebar.button("Refresh Dashboard", use_container_width=True):
        st.rerun()

    if st.sidebar.button("Generate Demo Output", use_container_width=True):
        success, message = run_local_pipeline("local_output")
        st.session_state["pipeline_feedback_level"] = "success" if success else "info"
        st.session_state["pipeline_feedback_message"] = message
        st.rerun()

    data, info_message = load_selected_data(selected_source, available_outputs)
    all_types = sorted(str(item) for item in data.gold["brewery_type"].dropna().unique())
    all_states = sorted(str(item) for item in data.gold["state_province"].dropna().unique())

    selected_types = st.sidebar.multiselect("Brewery Types", options=all_types, default=all_types)
    selected_states = st.sidebar.multiselect("States", options=all_states, default=all_states)
    st.sidebar.caption("Keep all filters selected to view the full dashboard.")

    filtered_gold = filter_gold(data.gold, selected_types, selected_states)
    health_label = build_health_state(data.quality)
    run_id = latest_run_id(data)

    render_hero(describe_source(data), health_label)
    if feedback_message:
        getattr(st, feedback_level or "info")(feedback_message)
    if info_message:
        st.caption(info_message)
    render_kpis(filtered_gold, data.quality, run_id, health_label)

    overview_tab, analytics_tab, operations_tab = st.tabs(
        ["Overview", "Analytics", "Operational Details"]
    )
    with overview_tab:
        render_overview_tab(data, filtered_gold)
    with analytics_tab:
        render_analytics_tab(filtered_gold)
    with operations_tab:
        render_operational_tab(data, available_outputs)


if __name__ == "__main__":
    main()
