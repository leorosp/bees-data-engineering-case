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
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap');

        :root {
            --bg: #f5f1eb;
            --surface: rgba(255, 255, 255, 0.92);
            --surface-strong: #ffffff;
            --text: #1f2428;
            --muted: #67717d;
            --line: rgba(31, 36, 40, 0.08);
            --brand: #0f766e;
            --brand-soft: #d7f0ec;
            --warn: #b45309;
            --warn-soft: #fde7c7;
            --navy: #173042;
        }

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(15, 118, 110, 0.08), transparent 28%),
                linear-gradient(180deg, #f9f7f2 0%, #f3eee7 100%);
            color: var(--text);
            font-family: "Manrope", "Segoe UI", sans-serif;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #132b3b 0%, #173042 100%);
        }

        [data-testid="stSidebar"] * {
            color: #f4f7fa;
        }

        .hero {
            background: linear-gradient(135deg, #173042 0%, #0f766e 100%);
            color: #fbfffe;
            border-radius: 24px;
            padding: 1.7rem 1.8rem;
            border: 1px solid rgba(255, 255, 255, 0.14);
            box-shadow: 0 18px 40px rgba(23, 48, 66, 0.12);
            margin-bottom: 1rem;
        }

        .hero-kicker {
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.78rem;
            opacity: 0.82;
        }

        .hero-title {
            font-size: 2.1rem;
            font-weight: 800;
            line-height: 1.05;
            margin: 0.35rem 0 0.7rem 0;
        }

        .hero-copy {
            max-width: 48rem;
            line-height: 1.6;
            opacity: 0.94;
        }

        .badge-row {
            margin-top: 1rem;
            display: flex;
            gap: 0.6rem;
            flex-wrap: wrap;
        }

        .badge {
            display: inline-block;
            border-radius: 999px;
            padding: 0.38rem 0.8rem;
            font-size: 0.84rem;
            font-weight: 700;
        }

        .badge-good {
            background: rgba(215, 240, 236, 0.95);
            color: #0f5d57;
        }

        .badge-warn {
            background: rgba(253, 231, 199, 0.96);
            color: #9a4b00;
        }

        .badge-neutral {
            background: rgba(255, 255, 255, 0.18);
            color: #fbfffe;
            border: 1px solid rgba(255, 255, 255, 0.18);
        }

        .section-title {
            font-size: 1.15rem;
            font-weight: 800;
            color: #16202a;
            margin-bottom: 0.25rem;
        }

        .section-copy {
            color: var(--muted);
            margin-bottom: 0.85rem;
        }

        .insight-card {
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 20px;
            padding: 1rem 1.05rem;
            box-shadow: 0 10px 25px rgba(24, 33, 41, 0.05);
            min-height: 140px;
        }

        .insight-label {
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            color: var(--muted);
            margin-bottom: 0.45rem;
        }

        .insight-value {
            font-size: 1.6rem;
            font-weight: 800;
            color: var(--text);
            line-height: 1.1;
            margin-bottom: 0.45rem;
        }

        .insight-copy {
            color: #55616d;
            line-height: 1.5;
        }

        .mono {
            font-family: "IBM Plex Mono", Consolas, monospace;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def discover_available_outputs() -> list[Path]:
    return [REPO_ROOT / name for name in KNOWN_OUTPUTS if (REPO_ROOT / name).exists()]


def resolve_output_root(raw_value: str) -> Path:
    raw_path = Path(raw_value).expanduser()
    if raw_path.is_absolute():
        return raw_path
    return REPO_ROOT / raw_path


def load_selected_data(source_mode: str, output_root: Path) -> DashboardData:
    if source_mode == "demo":
        return load_demo_dashboard_data(DEMO_ROOT)
    return load_dashboard_data(output_root)


def build_health_summary(quality_df: pd.DataFrame) -> tuple[str, str, str]:
    failed_checks = int(quality_df["status"].astype(str).str.lower().eq("fail").sum())
    if failed_checks:
        return (
            "Em atencao",
            "badge-warn",
            f"{failed_checks} check(s) precisam de revisao antes de publicar esse resultado.",
        )
    return (
        "Saudavel",
        "badge-good",
        "Nenhum check critico falhou na ultima execucao exibida pelo painel.",
    )


def render_badges(source_mode: str, quality_df: pd.DataFrame) -> None:
    health_label, health_css, health_copy = build_health_summary(quality_df)
    source_label = SOURCE_MODE_LABELS.get(source_mode, source_mode)
    st.markdown(
        f"""
        <div class="badge-row">
            <span class="badge badge-neutral">Fonte: {source_label}</span>
            <span class="badge {health_css}">Saude: {health_label}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(health_copy)


def render_hero(source_mode: str, quality_df: pd.DataFrame) -> None:
    st.markdown(
        """
        <div class="hero">
            <div class="hero-kicker">BEES Data Engineering Case</div>
            <div class="hero-title">Painel executivo e operacional do pipeline</div>
            <div class="hero-copy">
                Um resumo direto do que o projeto entrega: volume de cervejarias agregadas,
                distribuicao geografica, concentracao por tipo e qualidade da ultima execucao.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    render_badges(source_mode, quality_df)


def filter_gold(gold_df: pd.DataFrame, selected_types: list[str], selected_states: list[str]) -> pd.DataFrame:
    filtered = gold_df.copy()
    if selected_types:
        filtered = filtered[filtered["brewery_type"].isin(selected_types)]
    if selected_states:
        filtered = filtered[filtered["state_province"].isin(selected_states)]
    return filtered


def summarize_business(filtered_gold: pd.DataFrame, quality_df: pd.DataFrame) -> dict[str, str]:
    latest_run = (
        quality_df.sort_values("checked_at_utc")["run_id"].dropna().astype(str).iloc[-1]
        if not quality_df.empty
        else "-"
    )
    top_type = (
        filtered_gold.groupby("brewery_type", as_index=False)["brewery_count"]
        .sum()
        .sort_values("brewery_count", ascending=False)
        .head(1)
    )
    top_state = (
        filtered_gold.groupby("state_province", as_index=False)["brewery_count"]
        .sum()
        .sort_values("brewery_count", ascending=False)
        .head(1)
    )
    passed_checks = int(quality_df["status"].astype(str).str.lower().eq("pass").sum())
    total_checks = int(len(quality_df))

    return {
        "total_breweries": str(int(filtered_gold["brewery_count"].sum())),
        "total_types": str(int(filtered_gold["brewery_type"].nunique())),
        "total_states": str(int(filtered_gold["state_province"].nunique())),
        "latest_run": latest_run,
        "top_type": (
            f"{top_type.iloc[0]['brewery_type']} ({int(top_type.iloc[0]['brewery_count'])})"
            if not top_type.empty
            else "-"
        ),
        "top_state": (
            f"{top_state.iloc[0]['state_province']} ({int(top_state.iloc[0]['brewery_count'])})"
            if not top_state.empty
            else "-"
        ),
        "quality_summary": f"{passed_checks} de {total_checks} checks passaram",
    }


def render_insight_card(label: str, value: str, copy: str) -> None:
    st.markdown(
        f"""
        <div class="insight-card">
            <div class="insight-label">{label}</div>
            <div class="insight-value">{value}</div>
            <div class="insight-copy">{copy}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def prepare_type_chart(filtered_gold: pd.DataFrame):
    chart_df = (
        filtered_gold.groupby("brewery_type", as_index=False)["brewery_count"]
        .sum()
        .sort_values("brewery_count", ascending=True)
        .rename(columns={"brewery_type": "Tipo", "brewery_count": "Quantidade"})
    )
    fig = px.bar(
        chart_df,
        x="Quantidade",
        y="Tipo",
        orientation="h",
        text="Quantidade",
        color_discrete_sequence=["#0f766e"],
    )
    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.72)",
    )
    fig.update_traces(marker_line_width=0, textposition="outside")
    return fig


def prepare_state_chart(filtered_gold: pd.DataFrame):
    chart_df = (
        filtered_gold.groupby("state_province", as_index=False)["brewery_count"]
        .sum()
        .sort_values("brewery_count", ascending=False)
        .head(10)
        .rename(columns={"state_province": "Estado", "brewery_count": "Quantidade"})
    )
    fig = px.bar(
        chart_df,
        x="Estado",
        y="Quantidade",
        text="Quantidade",
        color="Quantidade",
        color_continuous_scale=["#d7f0ec", "#173042"],
    )
    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
        coloraxis_showscale=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.72)",
    )
    fig.update_traces(marker_line_width=0, textposition="outside")
    return fig


def prepare_quality_chart(quality_df: pd.DataFrame):
    chart_df = (
        quality_df.assign(
            camada=lambda df: df["layer"].replace(
                {"bronze": "Bronze", "silver": "Silver", "gold": "Gold", "ops": "Ops"}
            ),
            status_label=lambda df: df["status"].replace({"pass": "Passou", "fail": "Falhou"}),
        )
        .groupby(["camada", "status_label"], as_index=False)
        .size()
        .rename(columns={"size": "checks"})
    )
    fig = px.bar(
        chart_df,
        x="camada",
        y="checks",
        color="status_label",
        barmode="group",
        text="checks",
        color_discrete_map={"Passou": "#0f766e", "Falhou": "#b45309"},
    )
    fig.update_layout(
        height=320,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.72)",
        xaxis_title="",
        yaxis_title="Checks",
        legend_title="Resultado",
    )
    fig.update_traces(textposition="outside")
    return fig


def build_gold_table(filtered_gold: pd.DataFrame) -> pd.DataFrame:
    display_df = filtered_gold.rename(
        columns={
            "brewery_type": "Tipo",
            "country": "Pais",
            "state_province": "Estado",
            "brewery_count": "Quantidade",
            "run_id": "Run ID",
            "generated_at_utc": "Gerado em UTC",
        }
    )
    return display_df.sort_values(["Quantidade", "Tipo"], ascending=[False, True])


def build_quality_table(quality_df: pd.DataFrame) -> pd.DataFrame:
    display_df = quality_df.rename(
        columns={
            "check_name": "Check",
            "checked_at_utc": "Checado em UTC",
            "layer": "Camada",
            "message": "Mensagem",
            "metric_name": "Metrica",
            "metric_value": "Valor",
            "run_id": "Run ID",
            "status": "Status",
        }
    ).copy()
    display_df["Status"] = display_df["Status"].replace({"pass": "Passou", "fail": "Falhou"})
    display_df["Camada"] = display_df["Camada"].replace(
        {"bronze": "Bronze", "silver": "Silver", "gold": "Gold", "ops": "Ops"}
    )
    return display_df.sort_values(["Status", "Camada", "Check"])


def render_business_section(filtered_gold: pd.DataFrame, quality_df: pd.DataFrame) -> None:
    st.markdown('<div class="section-title">Resumo executivo</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-copy">Aqui fica a leitura mais rapida do valor entregue pela camada gold.</div>',
        unsafe_allow_html=True,
    )

    if filtered_gold.empty:
        st.info("Nenhum registro atende aos filtros selecionados.")
        return

    summary = summarize_business(filtered_gold, quality_df)

    metric_cols = st.columns(4)
    metric_cols[0].metric("Cervejarias", summary["total_breweries"])
    metric_cols[1].metric("Tipos", summary["total_types"])
    metric_cols[2].metric("Estados", summary["total_states"])
    metric_cols[3].metric("Ultimo run", summary["latest_run"])

    insight_cols = st.columns(3)
    with insight_cols[0]:
        render_insight_card(
            "Tipo lider",
            summary["top_type"],
            "Mostra qual categoria concentrou mais cervejarias no conjunto exibido.",
        )
    with insight_cols[1]:
        render_insight_card(
            "Estado lider",
            summary["top_state"],
            "Ajuda a identificar rapidamente a principal concentracao geografica.",
        )
    with insight_cols[2]:
        render_insight_card(
            "Qualidade",
            summary["quality_summary"],
            "Resume se a ultima execucao chegou ao painel com validacoes em ordem.",
        )

    chart_left, chart_right = st.columns(2)
    with chart_left:
        st.markdown('<div class="section-title">Distribuicao por tipo</div>', unsafe_allow_html=True)
        st.caption("Quanto cada tipo de cervejaria representa no resultado final.")
        st.plotly_chart(prepare_type_chart(filtered_gold), use_container_width=True)

    with chart_right:
        st.markdown('<div class="section-title">Top estados</div>', unsafe_allow_html=True)
        st.caption("Os estados com maior quantidade agregada de cervejarias.")
        st.plotly_chart(prepare_state_chart(filtered_gold), use_container_width=True)

    st.markdown('<div class="section-title">Tabela final da camada gold</div>', unsafe_allow_html=True)
    st.caption("Detalhamento pronto para consumo por negocio ou apresentacao.")
    st.dataframe(build_gold_table(filtered_gold), use_container_width=True, hide_index=True)


def render_operations_section(quality_df: pd.DataFrame, execution_df: pd.DataFrame) -> None:
    st.markdown('<div class="section-title">Saude do pipeline</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-copy">Uma leitura simples dos checks e do ultimo evento operacional registrado.</div>',
        unsafe_allow_html=True,
    )

    passed_checks = int(quality_df["status"].astype(str).str.lower().eq("pass").sum())
    failed_checks = int(quality_df["status"].astype(str).str.lower().eq("fail").sum())
    latest_run = (
        quality_df.sort_values("checked_at_utc")["run_id"].dropna().astype(str).iloc[-1]
        if not quality_df.empty
        else "-"
    )

    ops_metrics = st.columns(3)
    ops_metrics[0].metric("Checks que passaram", passed_checks)
    ops_metrics[1].metric("Checks com falha", failed_checks)
    ops_metrics[2].metric("Run analisado", latest_run)

    chart_left, chart_right = st.columns((1.05, 0.95))
    with chart_left:
        st.plotly_chart(prepare_quality_chart(quality_df), use_container_width=True)

    with chart_right:
        st.markdown('<div class="section-title">Ultimo evento</div>', unsafe_allow_html=True)
        st.caption("Resumo operacional da ultima execucao registrada em ops.")
        if execution_df.empty:
            st.info("Nenhum evento operacional foi encontrado.")
        else:
            latest_event = execution_df.sort_values("event_timestamp_utc").iloc[-1].to_dict()
            render_insight_card(
                "Etapa",
                str(latest_event.get("stage", "-")),
                str(latest_event.get("details", "-")),
            )
            event_cols = st.columns(2)
            event_cols[0].metric("Entradas", str(latest_event.get("records_in", "-")))
            event_cols[1].metric("Saidas", str(latest_event.get("records_out", "-")))
            st.caption(
                "Run ID: "
                f"{latest_event.get('run_id', '-')} | Status: {latest_event.get('status', '-')}"
            )

    show_failures_only = st.toggle("Mostrar apenas checks com falha", value=False)
    table_df = build_quality_table(quality_df)
    if show_failures_only:
        table_df = table_df[table_df["Status"] == "Falhou"]

    st.dataframe(table_df, use_container_width=True, hide_index=True)


def render_artifact_error(output_root: Path, error: Exception) -> None:
    st.error("Nao foi possivel abrir os artefatos locais.")
    st.caption("Se quiser usar o painel agora, volte a fonte para `Demo do projeto`.")
    with st.expander("Como gerar os artefatos locais"):
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
        st.caption(str(error))


def build_sidebar(available_outputs: list[Path]) -> tuple[str, Path]:
    st.sidebar.markdown("## Configuracao")

    source_options = ["demo"]
    if available_outputs:
        source_options.append("artifacts")

    selected_source = st.sidebar.radio(
        "Fonte de dados",
        options=source_options,
        format_func=lambda value: SOURCE_MODE_LABELS[value],
        index=0,
    )

    selected_output = REPO_ROOT / "local_output"
    if selected_source == "artifacts":
        selected_name = st.sidebar.selectbox(
            "Conjunto local",
            options=[path.name for path in available_outputs],
            index=0,
        )
        selected_output = resolve_output_root(selected_name)
        st.sidebar.caption("Use esse modo quando o pipeline local ja tiver gerado os arquivos.")
    else:
        st.sidebar.caption("Modo recomendado para demonstracao imediata do projeto.")

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Como ler o painel**")
    st.sidebar.caption(
        "Resumo executivo mostra o valor de negocio. Saude do pipeline mostra checks, eventos e possiveis falhas."
    )

    return selected_source, selected_output


def build_filter_sidebar(gold_df: pd.DataFrame) -> tuple[list[str], list[str]]:
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Filtros da visao executiva**")

    available_types = sorted(gold_df["brewery_type"].dropna().astype(str).unique().tolist())
    available_states = sorted(gold_df["state_province"].dropna().astype(str).unique().tolist())

    selected_types = st.sidebar.multiselect(
        "Tipos",
        options=available_types,
        default=available_types,
    )
    selected_states = st.sidebar.multiselect(
        "Estados",
        options=available_states,
        default=available_states,
    )
    return selected_types, selected_states


def main() -> None:
    inject_styles()

    available_outputs = discover_available_outputs()
    selected_source, selected_output = build_sidebar(available_outputs)

    try:
        data = load_selected_data(selected_source, selected_output)
    except Exception as error:
        render_hero("demo", pd.DataFrame({"status": ["pass"]}))
        render_artifact_error(selected_output, error)
        return

    selected_types, selected_states = build_filter_sidebar(data.gold)
    filtered_gold = filter_gold(data.gold, selected_types, selected_states)

    render_hero(data.source_mode, data.quality)

    with st.expander("O que este painel responde", expanded=False):
        st.markdown(
            """
            - Quantas cervejarias aparecem no resultado final.
            - Quais tipos concentram mais volume.
            - Quais estados lideram a distribuicao.
            - Se a ultima execucao passou pelos checks principais de qualidade.
            """
        )

    render_business_section(filtered_gold, data.quality)
    st.divider()
    render_operations_section(data.quality, data.execution)


if __name__ == "__main__":
    main()
