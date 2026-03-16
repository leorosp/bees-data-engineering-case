from pathlib import Path
import subprocess
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

from bees_case.dashboard_data import DashboardData, load_dashboard_data, load_demo_dashboard_data


st.set_page_config(
    page_title="BEES Brewery Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
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
        @import url('https://fonts.googleapis.com/css2?family=Sora:wght@400;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap');

        :root {
            --abi-blue: #325a6d;
            --abi-gold: #e5b611;
            --abi-red: #921a28;
            --abi-sage: #959b7b;
            --abi-stone: #d69e77;
            --page: #f7f5f1;
            --surface: rgba(255, 255, 255, 0.96);
            --surface-strong: #ffffff;
            --ink: #19232c;
            --muted: #66737d;
            --line: rgba(50, 90, 109, 0.10);
        }

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(229, 182, 17, 0.12), transparent 26%),
                linear-gradient(180deg, #fbfaf7 0%, #f3f0ea 100%);
            color: var(--ink);
            font-family: "Sora", "Segoe UI", sans-serif;
        }

        [data-testid="stHeader"] {
            background: rgba(0, 0, 0, 0);
        }

        .page-shell {
            padding-top: 0.4rem;
        }

        .hero {
            background:
                linear-gradient(135deg, rgba(50, 90, 109, 0.98) 0%, rgba(37, 66, 80, 0.98) 58%, rgba(25, 35, 44, 0.98) 100%);
            border: 1px solid rgba(229, 182, 17, 0.32);
            border-radius: 28px;
            padding: 1.55rem 1.6rem 1.35rem 1.6rem;
            color: #fbfcfd;
            box-shadow: 0 18px 42px rgba(25, 35, 44, 0.12);
            margin-bottom: 1rem;
            position: relative;
            overflow: hidden;
        }

        .hero::after {
            content: "";
            position: absolute;
            inset: auto -10% -45% auto;
            width: 240px;
            height: 240px;
            border-radius: 999px;
            background: radial-gradient(circle, rgba(229, 182, 17, 0.28), rgba(229, 182, 17, 0));
            pointer-events: none;
        }

        .eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-size: 0.74rem;
            opacity: 0.72;
        }

        .hero-title {
            font-size: 2rem;
            font-weight: 800;
            line-height: 1.05;
            margin: 0.45rem 0 0.55rem 0;
            max-width: 34rem;
        }

        .hero-copy {
            max-width: 40rem;
            color: rgba(251, 252, 253, 0.88);
            line-height: 1.6;
            font-size: 0.96rem;
        }

        .hero-meta {
            display: flex;
            gap: 0.55rem;
            flex-wrap: wrap;
            margin-top: 1rem;
        }

        .hero-chip {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.38rem 0.8rem;
            font-size: 0.82rem;
            font-weight: 700;
        }

        .chip-neutral {
            background: rgba(255, 255, 255, 0.10);
            border: 1px solid rgba(255, 255, 255, 0.14);
            color: #fbfcfd;
        }

        .chip-ok {
            background: rgba(149, 155, 123, 0.20);
            color: #f2f6ea;
        }

        .chip-warn {
            background: rgba(146, 26, 40, 0.22);
            color: #fff2f3;
        }

        .toolbar,
        .story-card,
        .panel,
        .kpi-card,
        .signal-card,
        .feedback-card {
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 24px;
            box-shadow: 0 12px 30px rgba(25, 35, 44, 0.05);
        }

        .toolbar {
            padding: 1rem 1rem 0.4rem 1rem;
            margin-bottom: 1rem;
        }

        .story-card {
            padding: 1rem 1.1rem;
            margin-bottom: 1rem;
            border-left: 6px solid var(--abi-gold);
        }

        .story-title,
        .panel-title {
            font-size: 1rem;
            font-weight: 700;
            color: var(--ink);
            margin-bottom: 0.2rem;
        }

        .story-copy,
        .panel-copy {
            color: var(--muted);
            line-height: 1.55;
        }

        .kpi-card {
            padding: 0.95rem 1rem;
            min-height: 132px;
        }

        .kpi-label {
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.14em;
            color: var(--muted);
            margin-bottom: 0.6rem;
        }

        .kpi-value {
            font-size: 1.85rem;
            font-weight: 800;
            color: var(--ink);
            line-height: 1;
            margin-bottom: 0.45rem;
        }

        .kpi-copy {
            color: var(--muted);
            font-size: 0.92rem;
            line-height: 1.45;
        }

        .signal-card {
            padding: 1rem 1.05rem;
            min-height: 148px;
        }

        .signal-title {
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.14em;
            color: var(--muted);
            margin-bottom: 0.5rem;
        }

        .signal-value {
            font-size: 1.2rem;
            font-weight: 700;
            color: var(--ink);
            line-height: 1.3;
            margin-bottom: 0.45rem;
        }

        .signal-copy {
            color: var(--muted);
            line-height: 1.45;
            font-size: 0.9rem;
        }

        .panel {
            padding: 1rem 1.05rem;
        }

        .feedback-card {
            padding: 0.9rem 1rem;
            margin-bottom: 1rem;
        }

        .feedback-ok {
            border-left: 6px solid var(--abi-sage);
        }

        .feedback-error {
            border-left: 6px solid var(--abi-red);
        }

        .feedback-title {
            font-weight: 700;
            color: var(--ink);
            margin-bottom: 0.2rem;
        }

        .feedback-copy {
            color: var(--muted);
            line-height: 1.5;
        }

        .mono {
            font-family: "IBM Plex Mono", Consolas, monospace;
        }

        .stButton button {
            background: var(--abi-gold);
            color: #0f1418;
            border: 0;
            border-radius: 14px;
            font-weight: 700;
            min-height: 2.8rem;
            box-shadow: none;
        }

        .stButton button:hover {
            background: #f2c63c;
            color: #0f1418;
        }

        div[data-baseweb="select"] > div,
        div[data-baseweb="select"] input,
        .stMultiSelect div[data-baseweb="select"] > div {
            border-radius: 14px !important;
        }

        .stRadio [role="radiogroup"] {
            gap: 0.45rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def discover_available_outputs() -> list[Path]:
    return [REPO_ROOT / name for name in KNOWN_OUTPUTS if (REPO_ROOT / name).exists()]


def load_selected_data(source_mode: str, output_root: Path) -> tuple[DashboardData, str | None]:
    if source_mode == "demo":
        return load_demo_dashboard_data(DEMO_ROOT), None

    try:
        return load_dashboard_data(output_root), None
    except Exception as error:
        fallback = load_demo_dashboard_data(DEMO_ROOT)
        return fallback, (
            "Nao foi possivel abrir os artefatos locais. O painel voltou para o modo demo. "
            f"Detalhe: {error}"
        )


def run_local_pipeline(output_dir: str = "local_output") -> None:
    command = [
        sys.executable,
        "scripts/run_local_pyspark_demo.py",
        "--output-dir",
        output_dir,
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=240,
            check=False,
        )
    except Exception as error:
        st.session_state["dashboard_feedback"] = {
            "status": "error",
            "title": "Nao foi possivel executar o pipeline local",
            "message": str(error),
        }
        return

    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    combined = "\n".join(part for part in [stdout, stderr] if part).strip()

    if completed.returncode == 0:
        st.session_state["dashboard_feedback"] = {
            "status": "success",
            "title": "Artefatos locais atualizados",
            "message": "O pipeline local foi executado com sucesso. Agora voce pode trocar a fonte para artefatos locais.",
        }
        return

    friendly_message = "A execucao local falhou."
    if "JAVA_HOME" in combined or "Java not found" in combined:
        friendly_message = (
            "O PySpark local depende de Java. Instale Java 17 e configure a variavel JAVA_HOME "
            "para liberar a geracao do local_output."
        )
    elif combined:
        friendly_message = combined.splitlines()[-1]

    st.session_state["dashboard_feedback"] = {
        "status": "error",
        "title": "Nao foi possivel gerar os artefatos locais",
        "message": friendly_message,
    }


def build_health_state(quality_df: pd.DataFrame) -> tuple[str, str, str]:
    failed_checks = int(quality_df["status"].astype(str).str.lower().eq("fail").sum())
    if failed_checks:
        return (
            "Em atencao",
            "chip-warn",
            f"{failed_checks} check(s) falharam neste recorte.",
        )
    return (
        "Saudavel",
        "chip-ok",
        "Nenhum check critico falhou no conjunto atual.",
    )


def build_gold_summary(filtered_gold: pd.DataFrame, quality_df: pd.DataFrame) -> dict[str, str]:
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
    failed_checks = int(quality_df["status"].astype(str).str.lower().eq("fail").sum())
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
        "quality_note": (
            f"{failed_checks} falha(s) em {total_checks} check(s)." if failed_checks else "Todos os checks passaram."
        ),
    }


def build_story(summary: dict[str, str], source_mode: str) -> str:
    source_label = SOURCE_MODE_LABELS.get(source_mode, source_mode).lower()
    return (
        f"No conjunto atual ({source_label}), o painel mostra {summary['total_breweries']} cervejarias "
        f"agregadas em {summary['total_states']} estado(s). O tipo dominante e {summary['top_type']} "
        f"e a maior concentracao geografica aparece em {summary['top_state']}."
    )


def filter_gold(gold_df: pd.DataFrame, selected_types: list[str], selected_states: list[str]) -> pd.DataFrame:
    filtered = gold_df.copy()
    if selected_types:
        filtered = filtered[filtered["brewery_type"].isin(selected_types)]
    if selected_states:
        filtered = filtered[filtered["state_province"].isin(selected_states)]
    return filtered


def render_hero(source_mode: str, quality_df: pd.DataFrame) -> None:
    health_label, health_class, health_copy = build_health_state(quality_df)
    source_label = SOURCE_MODE_LABELS.get(source_mode, source_mode)
    st.markdown(
        f"""
        <div class="hero">
            <div class="eyebrow">BEES Data Engineering Case</div>
            <div class="hero-title">Gold layer, qualidade e operacao em uma leitura unica</div>
            <div class="hero-copy">
                Uma visao objetiva do resultado final do pipeline: onde estao as cervejarias,
                quais tipos lideram e se a ultima execucao passou pelos checks mais importantes.
            </div>
            <div class="hero-meta">
                <span class="hero-chip chip-neutral">Fonte: {source_label}</span>
                <span class="hero-chip {health_class}">Saude: {health_label}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(health_copy)


def render_feedback(message: dict[str, str] | None = None, fallback_message: str | None = None) -> None:
    payload = message
    if not payload and fallback_message:
        payload = {
            "status": "error",
            "title": "Os artefatos locais nao foram carregados",
            "message": fallback_message,
        }

    if not payload:
        return

    css_class = "feedback-ok" if payload["status"] == "success" else "feedback-error"
    st.markdown(
        f"""
        <div class="feedback-card {css_class}">
            <div class="feedback-title">{payload['title']}</div>
            <div class="feedback-copy">{payload['message']}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_toolbar(available_outputs: list[Path]) -> tuple[str, Path]:
    st.markdown(
        """
        <div class="toolbar">
            <div class="panel-title">Controles do painel</div>
            <div class="panel-copy">Escolha a fonte dos dados, atualize a tela ou tente gerar os artefatos locais do projeto.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    source_options = ["demo"]
    if available_outputs:
        source_options.append("artifacts")

    toolbar_cols = st.columns((1.4, 1.2, 0.9, 1.0))
    with toolbar_cols[0]:
        source_mode = st.radio(
            "Fonte",
            options=source_options,
            format_func=lambda value: SOURCE_MODE_LABELS[value],
            horizontal=True,
            label_visibility="collapsed",
        )

    selected_output = REPO_ROOT / "local_output"
    with toolbar_cols[1]:
        if source_mode == "artifacts":
            selected_name = st.selectbox(
                "Conjunto local",
                options=[path.name for path in available_outputs],
                label_visibility="collapsed",
            )
            selected_output = REPO_ROOT / selected_name
        else:
            st.markdown("&nbsp;", unsafe_allow_html=True)
            st.caption("Modo mais rapido para apresentar o projeto.")

    with toolbar_cols[2]:
        if st.button("Atualizar", use_container_width=True):
            st.rerun()

    with toolbar_cols[3]:
        if st.button("Gerar local_output", use_container_width=True):
            with st.spinner("Executando pipeline local..."):
                run_local_pipeline()
            st.rerun()

    return source_mode, selected_output


def render_filters(gold_df: pd.DataFrame) -> tuple[list[str], list[str]]:
    filter_cols = st.columns((1, 1, 1.2))
    available_types = sorted(gold_df["brewery_type"].dropna().astype(str).unique().tolist())
    available_states = sorted(gold_df["state_province"].dropna().astype(str).unique().tolist())

    with filter_cols[0]:
        selected_types = st.multiselect("Filtrar tipos", options=available_types, default=available_types)
    with filter_cols[1]:
        selected_states = st.multiselect("Filtrar estados", options=available_states, default=available_states)
    with filter_cols[2]:
        st.markdown(
            """
            <div class="story-card">
                <div class="story-title">Como usar</div>
                <div class="story-copy">
                    Use os filtros para focar a visao executiva em um recorte especifico do resultado final.
                    O bloco de operacao abaixo continua mostrando a saude do conjunto exibido.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    return selected_types, selected_states


def render_story(summary: dict[str, str], source_mode: str) -> None:
    st.markdown(
        f"""
        <div class="story-card">
            <div class="story-title">Leitura automatica do recorte</div>
            <div class="story-copy">{build_story(summary, source_mode)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_card(label: str, value: str, copy: str) -> None:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-copy">{copy}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_signal_card(label: str, value: str, copy: str) -> None:
    st.markdown(
        f"""
        <div class="signal-card">
            <div class="signal-title">{label}</div>
            <div class="signal-value">{value}</div>
            <div class="signal-copy">{copy}</div>
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
        color_discrete_sequence=["#325a6d"],
    )
    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.7)",
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
        color_continuous_scale=["#e5b611", "#325a6d"],
    )
    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
        coloraxis_showscale=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.7)",
    )
    fig.update_traces(marker_line_width=0, textposition="outside")
    return fig


def prepare_quality_chart(quality_df: pd.DataFrame):
    chart_df = (
        quality_df.assign(
            camada=lambda df: df["layer"].replace(
                {"bronze": "Bronze", "silver": "Silver", "gold": "Gold", "ops": "Ops"}
            ),
            resultado=lambda df: df["status"].replace({"pass": "Passou", "fail": "Falhou"}),
        )
        .groupby(["camada", "resultado"], as_index=False)
        .size()
        .rename(columns={"size": "checks"})
    )
    fig = px.bar(
        chart_df,
        x="camada",
        y="checks",
        color="resultado",
        barmode="group",
        text="checks",
        color_discrete_map={"Passou": "#959b7b", "Falhou": "#921a28"},
    )
    fig.update_layout(
        height=320,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.7)",
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


def render_business_section(filtered_gold: pd.DataFrame, quality_df: pd.DataFrame, source_mode: str) -> None:
    if filtered_gold.empty:
        st.info("Nenhum registro atende aos filtros atuais.")
        return

    summary = build_gold_summary(filtered_gold, quality_df)
    render_story(summary, source_mode)

    kpi_cols = st.columns(4)
    with kpi_cols[0]:
        render_kpi_card("Cervejarias", summary["total_breweries"], "Volume agregado da camada gold.")
    with kpi_cols[1]:
        render_kpi_card("Tipos", summary["total_types"], "Categorias distintas no recorte atual.")
    with kpi_cols[2]:
        render_kpi_card("Estados", summary["total_states"], "Cobertura geografica do resultado.")
    with kpi_cols[3]:
        render_kpi_card("Ultimo run", summary["latest_run"], "Identificador operacional usado neste painel.")

    signal_cols = st.columns(3)
    with signal_cols[0]:
        render_signal_card("Tipo dominante", summary["top_type"], "Categoria com maior representacao.")
    with signal_cols[1]:
        render_signal_card("Maior concentracao", summary["top_state"], "Estado com mais cervejarias agregadas.")
    with signal_cols[2]:
        render_signal_card("Qualidade", summary["quality_note"], "Sintese direta dos checks visiveis.")

    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.markdown(
            """
            <div class="panel">
                <div class="panel-title">Distribuicao por tipo</div>
                <div class="panel-copy">Quanto cada tipo de cervejaria representa no resultado final.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.plotly_chart(prepare_type_chart(filtered_gold), use_container_width=True)

    with chart_cols[1]:
        st.markdown(
            """
            <div class="panel">
                <div class="panel-title">Top estados</div>
                <div class="panel-copy">Onde esta a maior concentracao de cervejarias no recorte atual.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.plotly_chart(prepare_state_chart(filtered_gold), use_container_width=True)

    st.markdown(
        """
        <div class="panel">
            <div class="panel-title">Tabela final da camada gold</div>
            <div class="panel-copy">Detalhamento pronto para apresentacao, consumo e conferencia.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.dataframe(build_gold_table(filtered_gold), use_container_width=True, hide_index=True)


def render_operations_section(quality_df: pd.DataFrame, execution_df: pd.DataFrame) -> None:
    failed_checks = int(quality_df["status"].astype(str).str.lower().eq("fail").sum())
    latest_run = (
        quality_df.sort_values("checked_at_utc")["run_id"].dropna().astype(str).iloc[-1]
        if not quality_df.empty
        else "-"
    )

    st.markdown(
        """
        <div class="panel">
            <div class="panel-title">Saude do pipeline</div>
            <div class="panel-copy">O bloco abaixo resume o comportamento dos checks e o ultimo evento operacional registrado.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    ops_cols = st.columns((1.2, 1.1, 1.4))
    with ops_cols[0]:
        render_signal_card("Checks com falha", str(failed_checks), "Numero de falhas presentes na exibicao atual.")
    with ops_cols[1]:
        render_signal_card("Run analisado", latest_run, "Identificador usado para leitura da ultima execucao.")
    with ops_cols[2]:
        if execution_df.empty:
            render_signal_card("Ultimo evento", "-", "Nenhum evento operacional foi encontrado.")
        else:
            latest_event = execution_df.sort_values("event_timestamp_utc").iloc[-1].to_dict()
            render_signal_card(
                "Ultimo evento",
                str(latest_event.get("stage", "-")),
                str(latest_event.get("details", "-")),
            )

    chart_cols = st.columns((1.05, 0.95))
    with chart_cols[0]:
        st.plotly_chart(prepare_quality_chart(quality_df), use_container_width=True)

    with chart_cols[1]:
        if execution_df.empty:
            st.info("Nenhum evento operacional foi encontrado.")
        else:
            latest_event = execution_df.sort_values("event_timestamp_utc").iloc[-1].to_dict()
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


def main() -> None:
    inject_styles()
    st.markdown('<div class="page-shell">', unsafe_allow_html=True)

    available_outputs = discover_available_outputs()
    source_mode, output_root = render_toolbar(available_outputs)
    data, fallback_message = load_selected_data(source_mode, output_root)

    render_hero(data.source_mode, data.quality)
    render_feedback(st.session_state.pop("dashboard_feedback", None), fallback_message)

    selected_types, selected_states = render_filters(data.gold)
    filtered_gold = filter_gold(data.gold, selected_types, selected_states)

    render_business_section(filtered_gold, data.quality, data.source_mode)
    st.divider()
    render_operations_section(data.quality, data.execution)
    st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
