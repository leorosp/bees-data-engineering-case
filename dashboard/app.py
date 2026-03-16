from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from bees_case.dashboard_data import DashboardData, load_dashboard_data


st.set_page_config(
    page_title="BEES Brewery Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

        :root {
            --cream: #f8f3e8;
            --sand: #e7d7b8;
            --ink: #1f1a17;
            --amber: #c7772e;
            --lager: #e0a33a;
            --teal: #1f6f6d;
            --brick: #7b3f21;
            --mint: #d8efe7;
        }

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(224, 163, 58, 0.15), transparent 30%),
                radial-gradient(circle at top right, rgba(31, 111, 109, 0.16), transparent 28%),
                linear-gradient(180deg, #fcf8ef 0%, #f6efe1 100%);
            color: var(--ink);
            font-family: "Space Grotesk", "Segoe UI", sans-serif;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #221a14 0%, #2d2118 100%);
            color: #f7efe1;
        }

        [data-testid="stSidebar"] * {
            color: #f7efe1;
        }

        .hero-card {
            background: linear-gradient(135deg, rgba(31, 111, 109, 0.96), rgba(123, 63, 33, 0.96));
            color: #fff9f0;
            border-radius: 24px;
            padding: 1.6rem 1.8rem;
            box-shadow: 0 18px 40px rgba(45, 33, 24, 0.14);
            border: 1px solid rgba(255, 255, 255, 0.12);
            margin-bottom: 1rem;
        }

        .hero-kicker {
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-size: 0.78rem;
            opacity: 0.86;
        }

        .hero-title {
            font-size: 2.2rem;
            line-height: 1.05;
            font-weight: 700;
            margin: 0.35rem 0 0.65rem 0;
        }

        .hero-copy {
            font-size: 1rem;
            max-width: 48rem;
            opacity: 0.92;
        }

        .metric-card {
            background: rgba(255, 251, 244, 0.95);
            border: 1px solid rgba(123, 63, 33, 0.12);
            border-radius: 20px;
            padding: 1rem 1.1rem;
            box-shadow: 0 10px 25px rgba(45, 33, 24, 0.06);
        }

        .metric-label {
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            color: #7d6b5d;
            margin-bottom: 0.35rem;
        }

        .metric-value {
            font-size: 2rem;
            font-weight: 700;
            color: var(--ink);
            line-height: 1;
        }

        .metric-caption {
            margin-top: 0.45rem;
            font-size: 0.9rem;
            color: #5b4b40;
        }

        .panel-title {
            font-size: 1.05rem;
            font-weight: 700;
            color: var(--brick);
            margin-bottom: 0.3rem;
        }

        .section-caption {
            color: #6b5a4d;
            margin-top: -0.2rem;
            margin-bottom: 0.8rem;
        }

        .status-pill {
            display: inline-block;
            border-radius: 999px;
            padding: 0.35rem 0.75rem;
            font-size: 0.82rem;
            font-weight: 700;
            margin-top: 0.6rem;
        }

        .status-healthy {
            background: rgba(216, 239, 231, 0.9);
            color: #155d47;
        }

        .status-attention {
            background: rgba(255, 226, 183, 0.95);
            color: #8a4b00;
        }

        .mono {
            font-family: "IBM Plex Mono", Consolas, monospace;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def build_status_label(quality_df: pd.DataFrame) -> tuple[str, str]:
    has_failure = quality_df["status"].astype(str).str.lower().eq("fail").any()
    if has_failure:
        return "Attention", "status-attention"
    return "Healthy", "status-healthy"


def render_metric_card(label: str, value: str, caption: str) -> None:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-caption">{caption}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def prepare_type_chart(gold_df: pd.DataFrame):
    by_type = (
        gold_df.groupby("brewery_type", as_index=False)["brewery_count"]
        .sum()
        .sort_values("brewery_count", ascending=True)
    )
    return px.bar(
        by_type,
        x="brewery_count",
        y="brewery_type",
        orientation="h",
        text="brewery_count",
        color="brewery_count",
        color_continuous_scale=["#d8efe7", "#c7772e"],
    )


def prepare_state_chart(gold_df: pd.DataFrame):
    by_state = (
        gold_df.groupby("state_province", as_index=False)["brewery_count"]
        .sum()
        .sort_values("brewery_count", ascending=False)
        .head(10)
    )
    return px.bar(
        by_state,
        x="state_province",
        y="brewery_count",
        text="brewery_count",
        color="brewery_count",
        color_continuous_scale=["#f1d8a3", "#1f6f6d"],
    )


def prepare_quality_chart(quality_df: pd.DataFrame):
    summary = (
        quality_df.groupby(["layer", "status"], as_index=False)
        .size()
        .rename(columns={"size": "checks"})
    )
    return px.bar(
        summary,
        x="layer",
        y="checks",
        color="status",
        barmode="group",
        text="checks",
        color_discrete_map={"pass": "#1f6f6d", "fail": "#c7772e"},
    )


def render_business_view(data: DashboardData) -> None:
    gold_df = data.gold.copy()
    quality_df = data.quality.copy()

    total_breweries = int(gold_df["brewery_count"].sum())
    total_types = int(gold_df["brewery_type"].nunique())
    total_states = int(gold_df["state_province"].nunique())
    latest_run = (
        quality_df.sort_values("checked_at_utc")["run_id"].dropna().astype(str).iloc[-1]
        if not quality_df.empty
        else "unknown"
    )
    status_label, status_class = build_status_label(quality_df)

    st.markdown(
        f"""
        <div class="hero-card">
            <div class="hero-kicker">BEES Data Engineering Case</div>
            <div class="hero-title">Brewery footprint and data quality in one place</div>
            <div class="hero-copy">
                This dashboard reads the curated <span class="mono">gold</span> and <span class="mono">ops</span>
                artifacts produced by the pipeline and turns them into a business-facing view plus a technical
                health check for the latest run.
            </div>
            <div class="status-pill {status_class}">Latest run status: {status_label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    metric_cols = st.columns(4)
    with metric_cols[0]:
        render_metric_card("Total Breweries", f"{total_breweries}", "Aggregated from the gold layer")
    with metric_cols[1]:
        render_metric_card("Brewery Types", f"{total_types}", "Distinct brewery_type values")
    with metric_cols[2]:
        render_metric_card("States Covered", f"{total_states}", "Distinct state_province values")
    with metric_cols[3]:
        render_metric_card("Latest Run", latest_run, "Run identifier from ops quality results")

    chart_left, chart_right = st.columns((1.05, 0.95))
    with chart_left:
        st.markdown('<div class="panel-title">Breweries by Type</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">What kinds of breweries dominate the curated dataset.</div>',
            unsafe_allow_html=True,
        )
        type_chart = prepare_type_chart(gold_df)
        type_chart.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            coloraxis_showscale=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.65)",
            xaxis_title="Brewery count",
            yaxis_title="",
        )
        type_chart.update_traces(textposition="outside")
        st.plotly_chart(type_chart, use_container_width=True)

    with chart_right:
        st.markdown('<div class="panel-title">Top States by Brewery Count</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">A quick ranking of the strongest geographic clusters.</div>',
            unsafe_allow_html=True,
        )
        state_chart = prepare_state_chart(gold_df)
        state_chart.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            coloraxis_showscale=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.65)",
            xaxis_title="",
            yaxis_title="Brewery count",
        )
        state_chart.update_traces(textposition="outside")
        st.plotly_chart(state_chart, use_container_width=True)

    st.markdown('<div class="panel-title">Detailed Gold Output</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-caption">Breakdown by brewery_type, country, and state_province.</div>',
        unsafe_allow_html=True,
    )
    st.dataframe(
        gold_df.sort_values(["brewery_count", "brewery_type"], ascending=[False, True]),
        use_container_width=True,
        hide_index=True,
    )


def render_operational_view(data: DashboardData) -> None:
    quality_df = data.quality.copy()
    execution_df = data.execution.copy()

    ops_left, ops_right = st.columns((0.95, 1.05))
    with ops_left:
        st.markdown('<div class="panel-title">Quality Checks by Layer</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">Pass/fail split for the latest artifact set.</div>',
            unsafe_allow_html=True,
        )
        quality_chart = prepare_quality_chart(quality_df)
        quality_chart.update_layout(
            height=320,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.65)",
            xaxis_title="",
            yaxis_title="Checks",
            legend_title="Status",
        )
        quality_chart.update_traces(textposition="outside")
        st.plotly_chart(quality_chart, use_container_width=True)

    with ops_right:
        st.markdown('<div class="panel-title">Latest Execution Event</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">Operational telemetry for the last successful local run.</div>',
            unsafe_allow_html=True,
        )
        if execution_df.empty:
            st.info("No execution events found.")
        else:
            latest_event = execution_df.sort_values("event_timestamp_utc").iloc[-1].to_dict()
            event_cols = st.columns(3)
            with event_cols[0]:
                render_metric_card("Stage", str(latest_event.get("stage", "-")), "Pipeline stage")
            with event_cols[1]:
                render_metric_card("Records In", str(latest_event.get("records_in", "-")), "Incoming records")
            with event_cols[2]:
                render_metric_card("Records Out", str(latest_event.get("records_out", "-")), "Produced records")
            st.markdown(
                f"""
                <div class="metric-card" style="margin-top: 0.8rem;">
                    <div class="metric-label">Details</div>
                    <div class="metric-caption">{latest_event.get("details", "-")}</div>
                    <div class="metric-caption"><span class="mono">run_id</span>: {latest_event.get("run_id", "-")}</div>
                    <div class="metric-caption"><span class="mono">status</span>: {latest_event.get("status", "-")}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown('<div class="panel-title">Quality Result Table</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-caption">Useful both for demos and for explaining why a run is healthy or not.</div>',
        unsafe_allow_html=True,
    )
    st.dataframe(
        quality_df.sort_values(["status", "layer", "check_name"]),
        use_container_width=True,
        hide_index=True,
    )


def render_missing_state(output_root: str, error: Exception) -> None:
    st.markdown(
        """
        <div class="hero-card">
            <div class="hero-kicker">Dashboard setup</div>
            <div class="hero-title">Artifacts not found yet</div>
            <div class="hero-copy">
                The dashboard is ready, but it needs the pipeline outputs under
                <span class="mono">local_output/</span> or another folder you choose in the sidebar.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.error(str(error))
    st.code(
        "\n".join(
            [
                "pip install -e \".[local,dashboard]\"",
                "python scripts/run_local_pyspark_demo.py",
                "streamlit run dashboard/app.py",
            ]
        ),
        language="bash",
    )
    st.info(f"Current output root: {output_root}")


def main() -> None:
    inject_styles()

    st.sidebar.markdown("## Dashboard Control Room")
    output_root = st.sidebar.text_input("Artifacts folder", value="local_output")
    st.sidebar.caption("Point this to the folder generated by the local PySpark run.")
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        """
        **Suggested flow**

        1. Run `python scripts/run_local_pyspark_demo.py`
        2. Open this dashboard
        3. Switch to `local_output_bad` to show quality failures
        """
    )

    try:
        data = load_dashboard_data(output_root)
    except Exception as error:
        render_missing_state(output_root, error)
        return

    tabs = st.tabs(["Executive View", "Operational View"])

    with tabs[0]:
        render_business_view(data)

    with tabs[1]:
        render_operational_view(data)

    st.sidebar.markdown("---")
    st.sidebar.metric("Artifacts root", str(Path(output_root)))
    st.sidebar.metric("Rows in gold", str(len(data.gold)))
    st.sidebar.metric(
        "Failed checks",
        str(data.quality["status"].astype(str).str.lower().eq("fail").sum()),
    )


if __name__ == "__main__":
    main()
