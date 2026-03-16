from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from bees_case.dashboard_data import DashboardData, load_dashboard_data, load_demo_dashboard_data


st.set_page_config(
    page_title="BEES Brewery Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

REPO_ROOT = Path(__file__).resolve().parents[1]
KNOWN_OUTPUTS = ("local_output", "local_output_bad")
DEMO_ROOT = REPO_ROOT / "dashboard" / "demo_data"

SOURCE_MODE_LABELS = {
    "demo": "Demo do projeto",
    "artifacts": "Artefatos locais",
}


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
        return "Em atencao", "status-attention"
    return "Saudavel", "status-healthy"


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


def rename_gold_for_display(gold_df: pd.DataFrame) -> pd.DataFrame:
    return gold_df.rename(
        columns={
            "brewery_type": "tipo",
            "country": "pais",
            "state_province": "estado",
            "brewery_count": "quantidade",
            "run_id": "run_id",
            "generated_at_utc": "gerado_em_utc",
        }
    )


def rename_quality_for_display(quality_df: pd.DataFrame) -> pd.DataFrame:
    display_df = quality_df.rename(
        columns={
            "check_name": "check",
            "checked_at_utc": "checado_em_utc",
            "layer": "camada",
            "message": "mensagem",
            "metric_name": "metrica",
            "metric_value": "valor",
            "run_id": "run_id",
            "status": "status",
        }
    ).copy()
    display_df["status"] = display_df["status"].replace({"pass": "passou", "fail": "falhou"})
    return display_df


def source_copy(mode: str) -> tuple[str, str]:
    if mode == "demo":
        return (
            "Demo do projeto",
            "Visualizacao carregada com o dataset demonstrativo validado no projeto.",
        )
    return (
        "Artefatos locais",
        "Visualizacao alimentada pelos artefatos gerados pela execucao local do pipeline.",
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
    summary["status"] = summary["status"].replace({"pass": "passou", "fail": "falhou"})
    return px.bar(
        summary,
        x="layer",
        y="checks",
        color="status",
        barmode="group",
        text="checks",
        color_discrete_map={"passou": "#1f6f6d", "falhou": "#c7772e"},
    )


def render_business_view(data: DashboardData) -> None:
    gold_df = data.gold.copy()
    quality_df = data.quality.copy()
    source_title, source_description = source_copy(data.source_mode)

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
            <div class="hero-title">Panorama de cervejarias e qualidade do pipeline</div>
            <div class="hero-copy">
                Este painel transforma os artefatos das camadas <span class="mono">gold</span> e
                <span class="mono">ops</span> em uma leitura simples do case: distribuicao de cervejarias,
                cobertura geografica e saude da ultima execucao.
            </div>
            <div class="status-pill {status_class}">Status da ultima execucao: {status_label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.caption(f"Fonte atual: {source_title}. {source_description}")

    metric_cols = st.columns(4)
    with metric_cols[0]:
        render_metric_card("Cervejarias", f"{total_breweries}", "Total agregado na camada gold")
    with metric_cols[1]:
        render_metric_card("Tipos", f"{total_types}", "Quantidade distinta de brewery_type")
    with metric_cols[2]:
        render_metric_card("Estados", f"{total_states}", "Cobertura geografica da camada gold")
    with metric_cols[3]:
        render_metric_card("Ultimo run", latest_run, "Identificador vindo dos checks operacionais")

    chart_left, chart_right = st.columns((1.05, 0.95))
    with chart_left:
        st.markdown('<div class="panel-title">Distribuicao por Tipo</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">Mostra quais tipos de cervejaria concentram mais registros no resultado final.</div>',
            unsafe_allow_html=True,
        )
        type_chart = prepare_type_chart(gold_df)
        type_chart.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            coloraxis_showscale=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.65)",
            xaxis_title="Quantidade de cervejarias",
            yaxis_title="",
        )
        type_chart.update_traces(textposition="outside")
        st.plotly_chart(type_chart, use_container_width=True)

    with chart_right:
        st.markdown('<div class="panel-title">Estados com Maior Concentração</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">Ranking rapido das localidades com maior volume de cervejarias agregadas.</div>',
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
            yaxis_title="Quantidade de cervejarias",
        )
        state_chart.update_traces(textposition="outside")
        st.plotly_chart(state_chart, use_container_width=True)

    st.markdown('<div class="panel-title">Detalhamento da Camada Gold</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-caption">Tabela final com os agrupamentos por tipo, pais e estado.</div>',
        unsafe_allow_html=True,
    )
    st.dataframe(
        rename_gold_for_display(
            gold_df.sort_values(["brewery_count", "brewery_type"], ascending=[False, True])
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_operational_view(data: DashboardData) -> None:
    quality_df = data.quality.copy()
    execution_df = data.execution.copy()

    ops_left, ops_right = st.columns((0.95, 1.05))
    with ops_left:
        st.markdown('<div class="panel-title">Checks de Qualidade por Camada</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">Resume quantos checks passaram ou falharam em cada camada do pipeline.</div>',
            unsafe_allow_html=True,
        )
        quality_chart = prepare_quality_chart(quality_df)
        quality_chart.update_layout(
            height=320,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.65)",
            xaxis_title="",
            yaxis_title="Quantidade de checks",
            legend_title="Status",
        )
        quality_chart.update_traces(textposition="outside")
        st.plotly_chart(quality_chart, use_container_width=True)

    with ops_right:
        st.markdown('<div class="panel-title">Ultimo Evento Operacional</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-caption">Mostra o resumo tecnico da ultima execucao registrada pelo pipeline.</div>',
            unsafe_allow_html=True,
        )
        if execution_df.empty:
            st.info("Nenhum evento operacional foi encontrado.")
        else:
            latest_event = execution_df.sort_values("event_timestamp_utc").iloc[-1].to_dict()
            event_cols = st.columns(3)
            with event_cols[0]:
                render_metric_card("Etapa", str(latest_event.get("stage", "-")), "Fase registrada")
            with event_cols[1]:
                render_metric_card("Entradas", str(latest_event.get("records_in", "-")), "Quantidade recebida")
            with event_cols[2]:
                render_metric_card("Saidas", str(latest_event.get("records_out", "-")), "Quantidade produzida")
            st.markdown(
                f"""
                <div class="metric-card" style="margin-top: 0.8rem;">
                    <div class="metric-label">Detalhes</div>
                    <div class="metric-caption">{latest_event.get("details", "-")}</div>
                    <div class="metric-caption"><span class="mono">run_id</span>: {latest_event.get("run_id", "-")}</div>
                    <div class="metric-caption"><span class="mono">status</span>: {latest_event.get("status", "-")}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown('<div class="panel-title">Tabela de Qualidade</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-caption">Ajuda a explicar com clareza por que uma execucao foi considerada saudavel ou nao.</div>',
        unsafe_allow_html=True,
    )
    st.dataframe(
        rename_quality_for_display(
            quality_df.sort_values(["status", "layer", "check_name"])
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_missing_state(output_root: str, error: Exception) -> None:
    st.markdown(
        """
        <div class="hero-card">
            <div class="hero-kicker">Configuracao do dashboard</div>
            <div class="hero-title">Os artefatos locais ainda nao foram gerados</div>
            <div class="hero-copy">
                O painel esta pronto, mas precisa dos arquivos produzidos pelo pipeline local para abrir no modo
                de artefatos.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.info("Enquanto isso, voce pode usar o modo demo do projeto pela barra lateral.")
    st.caption(str(error))
    st.code(
        "\n".join(
            [
                'cd "C:\\Users\\leona\\Documents\\GitHub\\bees-data-engineering-case"',
                "python scripts/run_local_pyspark_demo.py",
                "python -m streamlit run dashboard/app.py",
            ]
        ),
        language="powershell",
    )
    st.caption(f"Pasta informada: {output_root}")


def resolve_output_root(raw_value: str) -> Path:
    raw_path = Path(raw_value).expanduser()
    if raw_path.is_absolute():
        return raw_path

    repo_relative = REPO_ROOT / raw_path
    if repo_relative.exists():
        return repo_relative

    cwd_relative = Path.cwd() / raw_path
    if cwd_relative.exists():
        return cwd_relative

    return repo_relative


def discover_available_outputs() -> list[Path]:
    available: list[Path] = []
    for name in KNOWN_OUTPUTS:
        candidate = REPO_ROOT / name
        if candidate.exists():
            available.append(candidate)
    return available


def main() -> None:
    inject_styles()

    st.sidebar.markdown("## Painel do Projeto")
    available_outputs = discover_available_outputs()
    source_options = ["demo"]
    if available_outputs:
        source_options.append("artifacts")

    selected_source = st.sidebar.radio(
        "Fonte dos dados",
        options=source_options,
        format_func=lambda value: SOURCE_MODE_LABELS[value],
        index=0,
    )

    output_root = REPO_ROOT / "local_output"
    if selected_source == "artifacts":
        default_root = available_outputs[0]
        output_root_input = st.sidebar.selectbox(
            "Conjunto de artefatos",
            options=available_outputs,
            format_func=lambda value: value.name,
        )
        output_root = resolve_output_root(str(output_root_input))
        st.sidebar.caption("Use o conjunto gerado pelo pipeline local.")
    else:
        st.sidebar.caption("Modo recomendado para apresentacao rapida do projeto.")
        if not available_outputs:
            st.sidebar.info("Os artefatos locais ainda nao foram gerados nesta maquina.")

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        """
        **Como usar**

        1. Abra em `Demo do projeto` para ver o dashboard imediatamente
        2. Gere `local_output` se quiser usar artefatos locais
        3. Troque para `Artefatos locais` quando esses arquivos existirem
        """
    )

    if selected_source == "demo":
        data = load_demo_dashboard_data(DEMO_ROOT)
    else:
        try:
            data = load_dashboard_data(output_root)
        except Exception as error:
            render_missing_state(str(output_root), error)
            return

    tabs = st.tabs(["Resumo Executivo", "Saude Operacional"])

    with tabs[0]:
        render_business_view(data)

    with tabs[1]:
        render_operational_view(data)

    st.sidebar.markdown("---")
    st.sidebar.metric("Fonte atual", SOURCE_MODE_LABELS.get(data.source_mode, data.source_mode))
    st.sidebar.metric("Linhas na gold", str(len(data.gold)))
    st.sidebar.metric(
        "Checks com falha",
        str(data.quality["status"].astype(str).str.lower().eq("fail").sum()),
    )


if __name__ == "__main__":
    main()
