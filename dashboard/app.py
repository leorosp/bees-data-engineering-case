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
    "Saida local": REPO_ROOT / "local_output",
    "Exercicio do gate de qualidade": REPO_ROOT / "local_output_bad",
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
        return "Desconhecida"
    return "Atencao" if (quality["status"].str.lower() == "fail").any() else "Saudavel"


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
            "details": "Nenhum evento de execucao foi encontrado.",
            "timestamp": "n/a",
        }

    latest = execution.sort_values("event_timestamp_utc").iloc[-1]
    timestamp = latest.get("event_timestamp_utc")
    timestamp_text = timestamp.strftime("%Y-%m-%d %H:%M UTC") if pd.notna(timestamp) else "n/a"
    return {
        "stage": str(latest.get("stage", "n/a")),
        "status": str(latest.get("status", "n/a")).title(),
        "records_in": str(latest.get("records_in", "n/a")),
        "records_out": str(latest.get("records_out", "n/a")),
        "details": str(latest.get("details", "Nenhum detalhe de execucao disponivel.")),
        "timestamp": timestamp_text,
    }


def format_stage_label(stage: str) -> str:
    if not stage or stage == "n/a":
        return "Pipeline"
    normalized = stage.replace("_", " ").title().replace("Pyspark", "PySpark")
    if normalized == "Local PySpark Pipeline":
        return "Pipeline local PySpark"
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
        labels={"brewery_type": "Tipo de cervejaria", "brewery_count": "Cervejarias"},
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
        labels={"state_province": "Estado", "brewery_count": "Cervejarias"},
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
            status=quality["status"].replace({"pass": "Aprovado", "fail": "Falhou"})
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
        labels={"layer": "Camada", "checks": "Verificacoes"},
        color_discrete_map={"Aprovado": BEES_GREEN, "Falhou": BEES_RED},
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
        status = "Atencao" if failed else "Aprovado"
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
        return "Dataset demo"
    return data.source_root.name.replace("_", " ").title()


def load_selected_data(source_label: str, available_outputs: dict[str, Path]) -> tuple[DashboardData, str | None]:
    if source_label == "Dataset demo":
        return load_demo_dashboard_data(DEMO_ROOT), None

    target = available_outputs.get(source_label)
    if not target:
        return (
            load_demo_dashboard_data(DEMO_ROOT),
            "Os artefatos locais ainda nao estao disponiveis. O dashboard esta exibindo o dataset demo.",
        )

    try:
        return load_dashboard_data(target), None
    except FileNotFoundError:
        return (
            load_demo_dashboard_data(DEMO_ROOT),
            f"{source_label} esta incompleta no momento. O dashboard esta exibindo o dataset demo.",
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
        return True, f"{output_dir} foi gerado com sucesso."
    return False, (
        "O modo demo esta ativo para apresentacao. "
        "A execucao local pode ser habilitada em um ambiente totalmente configurado."
    )


def render_hero(source_label: str, health_label: str) -> None:
    health_class = "badge--healthy" if health_label == "Saudavel" else "badge--attention"
    st.markdown(
        f"""
        <section class="hero-card">
            <div class="hero-kicker">Visao Geral do Pipeline BEES</div>
            <div class="hero-title">Distribuicao de cervejarias e saude do pipeline</div>
            <p class="hero-copy">
                Resumo executivo das metricas da camada gold, cobertura geografica e status de qualidade do pipeline.
            </p>
            <div class="badge-row">
                <span class="badge">Fonte de dados: {source_label}</span>
                <span class="badge {health_class}">Saude do pipeline: {health_label}</span>
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
    col1.metric("Cervejarias", f"{total_breweries}")
    col2.metric("Tipos de cervejaria", f"{total_types}")
    col3.metric("Estados cobertos", f"{total_states}")
    col4.metric("Saude do pipeline", health_label)
    col5.metric("Ultima execucao", latest_run)
    if failed_checks:
        st.caption(f"{failed_checks} verificacao(oes) de qualidade exigem atencao.")


def render_overview_tab(data: DashboardData, filtered_gold: pd.DataFrame) -> None:
    summary = latest_execution_summary(data.execution)
    quality_by_layer = quality_status_by_layer(data.quality)
    stage_label = format_stage_label(summary["stage"])
    execution_text = (
        f"{stage_label} foi concluido com sucesso em {summary['timestamp']}."
        if summary["status"].lower() == "success" and summary["timestamp"] != "n/a"
        else (
            f"{stage_label} terminou com status {summary['status']}."
            if summary["status"] != "n/a"
            else "Nenhum resumo de execucao esta disponivel no momento."
        )
    )

    left, right = st.columns([1.35, 1])
    with left:
        st.markdown("#### Cervejarias por tipo")
        if filtered_gold.empty:
            st.info("Nenhuma linha corresponde aos filtros atuais.")
        else:
            st.plotly_chart(
                make_type_chart(filtered_gold),
                use_container_width=True,
                key="overview_breweries_by_type",
            )

    with right:
        st.markdown("#### Verificacoes de qualidade do pipeline")
        layer_columns = st.columns(max(len(quality_by_layer), 1))
        if quality_by_layer.empty:
            layer_columns[0].metric("Pipeline", "Desconhecido")
        else:
            for col, row in zip(layer_columns, quality_by_layer.itertuples(index=False)):
                col.metric(row.layer, row.status)

        st.markdown("#### Resumo da ultima execucao")
        st.markdown(
            f"""
            <div class="section-label">Ultima execucao</div>
            <p class="section-copy">
                {execution_text}
            </p>
            """,
            unsafe_allow_html=True,
        )
        metric_a, metric_b = st.columns(2)
        metric_a.metric("Registros de origem", summary["records_in"])
        metric_b.metric("Registros tratados", summary["records_out"])
        st.caption(summary["details"])


def render_analytics_tab(filtered_gold: pd.DataFrame) -> None:
    if filtered_gold.empty:
        st.info("Nenhuma linha corresponde aos filtros atuais.")
        return

    chart_a, chart_b = st.columns(2)
    with chart_a:
        st.markdown("#### Cervejarias por tipo")
        st.plotly_chart(
            make_type_chart(filtered_gold),
            use_container_width=True,
            key="analytics_breweries_by_type",
        )
    with chart_b:
        st.markdown("#### Estados com mais cervejarias")
        st.plotly_chart(
            make_state_chart(filtered_gold),
            use_container_width=True,
            key="analytics_top_states",
        )

    st.markdown("#### Camada gold filtrada")
    display_df = filtered_gold.rename(
        columns={
            "brewery_type": "Tipo de cervejaria",
            "country": "Pais",
            "state_province": "Estado",
            "brewery_count": "Cervejarias",
            "run_id": "ID da execucao",
            "generated_at_utc": "Gerado em UTC",
        }
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_operational_tab(data: DashboardData, available_outputs: dict[str, Path]) -> None:
    chart_col, table_col = st.columns([1.05, 1])
    with chart_col:
        st.markdown("#### Verificacoes de qualidade por camada")
        st.plotly_chart(
            make_quality_chart(data.quality),
            use_container_width=True,
            key="operations_quality_by_layer",
        )

    with table_col:
        st.markdown("#### Resumo da ultima execucao")
        execution_df = data.execution.sort_values("event_timestamp_utc", ascending=False).rename(
            columns={
                "layer": "Camada",
                "stage": "Etapa",
                "status": "Status",
                "run_id": "ID da execucao",
                "records_in": "Registros de origem",
                "records_out": "Registros tratados",
                "details": "Detalhes",
                "event_timestamp_utc": "Data e hora UTC",
            }
        )
        execution_df["Status"] = execution_df["Status"].replace(
            {"success": "Sucesso", "failed": "Falhou"}
        )
        st.dataframe(execution_df, use_container_width=True, hide_index=True)

    st.markdown("#### Verificacoes detalhadas de qualidade")
    quality_df = data.quality.sort_values(["layer", "status", "check_name"]).rename(
        columns={
            "check_name": "Verificacao",
            "checked_at_utc": "Verificado em UTC",
            "layer": "Camada",
            "message": "Mensagem",
            "metric_name": "Metrica",
            "metric_value": "Valor da metrica",
            "run_id": "ID da execucao",
            "status": "Status",
        }
    )
    quality_df["Status"] = quality_df["Status"].replace({"pass": "Aprovado", "fail": "Falhou"})
    st.dataframe(quality_df, use_container_width=True, hide_index=True)

    with st.expander("Detalhes operacionais", expanded=False):
        availability = ["Dataset demo: disponivel"]
        for label in KNOWN_OUTPUTS:
            availability.append(
                f"{label}: {'disponivel' if label in available_outputs else 'indisponivel'}"
            )

        st.markdown("**Como executar localmente**")
        st.code(
            "\n".join(
                [
                    'pip install -e ".[dev,local,dashboard]"',
                    "python scripts/run_api_pyspark_pipeline.py --output-dir local_output",
                    "# ou, para uma demo deterministica:",
                    "python scripts/run_local_pyspark_demo.py",
                    "python -m streamlit run dashboard/app.py",
                ]
            ),
            language="bash",
        )
        st.markdown("**Fontes de dados disponiveis**")
        for item in availability:
            st.write(f"- {item}")


def main() -> None:
    st.set_page_config(
        page_title="Dashboard do Case de Engenharia de Dados BEES",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    inject_styles()

    available_outputs = discover_available_outputs()
    source_options = ["Dataset demo", *available_outputs.keys()]

    st.sidebar.title("Controles")
    selected_source = st.sidebar.radio("Fonte de dados", options=source_options, index=0)

    feedback_level = st.session_state.pop("pipeline_feedback_level", None)
    feedback_message = st.session_state.pop("pipeline_feedback_message", None)
    if st.sidebar.button("Atualizar painel", use_container_width=True):
        st.rerun()

    if st.sidebar.button("Gerar saida local de demo", use_container_width=True):
        success, message = run_local_pipeline("local_output")
        st.session_state["pipeline_feedback_level"] = "success" if success else "info"
        st.session_state["pipeline_feedback_message"] = message
        st.rerun()

    data, info_message = load_selected_data(selected_source, available_outputs)
    all_types = sorted(str(item) for item in data.gold["brewery_type"].dropna().unique())
    all_states = sorted(str(item) for item in data.gold["state_province"].dropna().unique())

    selected_types = st.sidebar.multiselect("Tipos de cervejaria", options=all_types, default=all_types)
    selected_states = st.sidebar.multiselect("Estados", options=all_states, default=all_states)
    st.sidebar.caption("Mantenha todos os filtros selecionados para ver o painel completo.")

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
        ["Visao geral", "Analises", "Detalhes operacionais"]
    )
    with overview_tab:
        render_overview_tab(data, filtered_gold)
    with analytics_tab:
        render_analytics_tab(filtered_gold)
    with operations_tab:
        render_operational_tab(data, available_outputs)


if __name__ == "__main__":
    main()
