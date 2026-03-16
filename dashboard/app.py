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

BEES_YELLOW = "#f5c400"
BEES_BLACK = "#141414"
BEES_CHARCOAL = "#232323"
BEES_RED = "#a52333"
BEES_GREEN = "#6f8a4b"


def inject_styles() -> None:
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Sora:wght@400;600;700;800&display=swap');

        .stApp {{
            background:
                radial-gradient(circle at top left, rgba(245, 196, 0, 0.12), transparent 24%),
                linear-gradient(180deg, #fffef9 0%, #f5f1e7 100%);
            font-family: "Sora", "Segoe UI", sans-serif;
        }}

        [data-testid="stHeader"] {{
            background: transparent;
        }}

        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, {BEES_BLACK} 0%, {BEES_CHARCOAL} 100%);
        }}

        [data-testid="stSidebar"] * {{
            color: #f7fafc;
        }}

        .hero-box {{
            background: linear-gradient(135deg, {BEES_BLACK} 0%, {BEES_CHARCOAL} 100%);
            border: 1px solid rgba(245, 196, 0, 0.34);
            border-radius: 24px;
            padding: 1.4rem 1.5rem;
            color: #fbfcfd;
            margin-bottom: 1rem;
        }}

        .hero-eyebrow {{
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-size: 0.72rem;
            opacity: 0.75;
        }}

        .hero-title {{
            font-size: 1.9rem;
            font-weight: 800;
            line-height: 1.08;
            margin: 0.45rem 0 0.55rem 0;
            max-width: 34rem;
        }}

        .hero-copy {{
            max-width: 38rem;
            line-height: 1.6;
            color: rgba(251, 252, 253, 0.88);
        }}

        .info-chip {{
            display: inline-block;
            border-radius: 999px;
            padding: 0.35rem 0.8rem;
            margin-right: 0.45rem;
            margin-top: 0.85rem;
            font-size: 0.82rem;
            font-weight: 700;
        }}

        .info-neutral {{
            background: rgba(255,255,255,0.12);
            color: #fbfcfd;
        }}

        .info-ok {{
            background: rgba(111, 138, 75, 0.24);
            color: #f4f8ee;
        }}

        .info-warn {{
            background: rgba(165, 35, 51, 0.24);
            color: #fff3f4;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def discover_available_outputs() -> list[Path]:
    return [REPO_ROOT / name for name in KNOWN_OUTPUTS if (REPO_ROOT / name).exists()]


def detect_environment_status() -> dict[str, str]:
    has_local_output = (REPO_ROOT / "local_output").exists()
    has_local_output_bad = (REPO_ROOT / "local_output_bad").exists()
    java_home = bool(st.session_state.get("java_home_override")) or bool(__import__("os").environ.get("JAVA_HOME"))

    return {
        "python": sys.executable,
        "java": "configurado" if java_home else "nao detectado",
        "local_output": "disponivel" if has_local_output else "nao encontrado",
        "local_output_bad": "disponivel" if has_local_output_bad else "nao encontrado",
    }


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


def load_selected_data(source_mode: str, output_root: Path) -> tuple[DashboardData, str | None]:
    if source_mode == "demo":
        return load_demo_dashboard_data(DEMO_ROOT), None

    try:
        return load_dashboard_data(output_root), None
    except Exception as error:
        fallback = load_demo_dashboard_data(DEMO_ROOT)
        return fallback, (
            "Nao foi possivel abrir os artefatos locais. O painel voltou automaticamente para o modo demo. "
            f"Detalhe: {error}"
        )


def build_health_state(quality_df: pd.DataFrame) -> tuple[str, str]:
    failed_checks = int(quality_df["status"].astype(str).str.lower().eq("fail").sum())
    if failed_checks:
        return "Em atencao", "info-warn"
    return "Saudavel", "info-ok"


def build_summary(filtered_gold: pd.DataFrame, quality_df: pd.DataFrame) -> dict[str, str]:
    latest_run = (
        quality_df.sort_values("checked_at_utc")["run_id"].dropna().astype(str).iloc[-1]
        if not quality_df.empty
        else "-"
    )
    failed_checks = int(quality_df["status"].astype(str).str.lower().eq("fail").sum())

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

    return {
        "total_breweries": str(int(filtered_gold["brewery_count"].sum())),
        "total_types": str(int(filtered_gold["brewery_type"].nunique())),
        "total_states": str(int(filtered_gold["state_province"].nunique())),
        "latest_run": latest_run,
        "top_type": top_type.iloc[0]["brewery_type"] if not top_type.empty else "-",
        "top_state": top_state.iloc[0]["state_province"] if not top_state.empty else "-",
        "failed_checks": str(failed_checks),
    }


def filter_gold(gold_df: pd.DataFrame, selected_types: list[str], selected_states: list[str]) -> pd.DataFrame:
    filtered = gold_df.copy()
    if selected_types:
        filtered = filtered[filtered["brewery_type"].isin(selected_types)]
    if selected_states:
        filtered = filtered[filtered["state_province"].isin(selected_states)]
    return filtered


def render_sidebar_controls(available_outputs: list[Path]) -> tuple[str, Path, list[str], list[str]]:
    st.sidebar.markdown("## Controles")

    source_options = ["demo"]
    if available_outputs:
        source_options.append("artifacts")

    source_mode = st.sidebar.radio(
        "Fonte dos dados",
        options=source_options,
        format_func=lambda value: SOURCE_MODE_LABELS[value],
    )

    output_root = REPO_ROOT / "local_output"
    if source_mode == "artifacts":
        selected_name = st.sidebar.selectbox(
            "Conjunto local",
            options=[path.name for path in available_outputs],
        )
        output_root = REPO_ROOT / selected_name
    else:
        st.sidebar.caption("Modo recomendado para apresentacao rapida.")

    st.sidebar.markdown("---")
    if st.sidebar.button("Atualizar painel", use_container_width=True):
        st.rerun()

    if st.sidebar.button("Gerar local_output", use_container_width=True):
        with st.spinner("Executando pipeline local..."):
            run_local_pipeline()
        st.rerun()

    preview_data, _ = load_selected_data(source_mode, output_root)
    available_types = sorted(preview_data.gold["brewery_type"].dropna().astype(str).unique().tolist())
    available_states = sorted(preview_data.gold["state_province"].dropna().astype(str).unique().tolist())

    st.sidebar.markdown("---")
    st.sidebar.markdown("## Filtros")
    selected_types = st.sidebar.multiselect("Tipos", options=available_types, default=available_types)
    selected_states = st.sidebar.multiselect("Estados", options=available_states, default=available_states)
    st.sidebar.caption("Deixe tudo marcado para ver o painel completo.")

    return source_mode, output_root, selected_types, selected_states


def render_hero(source_mode: str, quality_df: pd.DataFrame) -> None:
    source_label = SOURCE_MODE_LABELS.get(source_mode, source_mode)
    health_label, health_class = build_health_state(quality_df)
    st.markdown(
        f"""
        <div class="hero-box">
            <div class="hero-eyebrow">BEES Data Engineering Case</div>
            <div class="hero-title">Gold layer e saude do pipeline em uma leitura direta</div>
            <div class="hero-copy">
                Um resumo objetivo do resultado final: distribuicao de cervejarias, concentracao por tipo,
                cobertura geografica e qualidade da execucao.
            </div>
            <span class="info-chip info-neutral">Fonte: {source_label}</span>
            <span class="info-chip {health_class}">Saude: {health_label}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_feedback(feedback: dict[str, str] | None, fallback_message: str | None) -> None:
    payload = feedback
    if not payload and fallback_message:
        payload = {
            "status": "warning",
            "title": "Artefatos locais nao encontrados",
            "message": fallback_message,
        }

    if not payload:
        return

    if payload["status"] == "success":
        st.success(f"{payload['title']}: {payload['message']}")
    else:
        st.warning(f"{payload['title']}: {payload['message']}")


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
        color_discrete_sequence=[BEES_BLACK],
    )
    fig.update_layout(
        height=340,
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
        color_continuous_scale=[BEES_YELLOW, BEES_BLACK],
    )
    fig.update_layout(
        height=340,
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
        color_discrete_map={"Passou": BEES_GREEN, "Falhou": BEES_RED},
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
    if filtered_gold.empty:
        st.info("Nenhum registro atende aos filtros atuais.")
        return

    summary = build_summary(filtered_gold, quality_df)
    kpi_cols = st.columns(4)
    kpi_cols[0].metric("Cervejarias", summary["total_breweries"])
    kpi_cols[1].metric("Tipos", summary["total_types"])
    kpi_cols[2].metric("Estados", summary["total_states"])
    kpi_cols[3].metric("Ultimo run", summary["latest_run"])

    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.subheader("Distribuicao por tipo")
        st.caption("Quais categorias dominam o resultado final.")
        st.plotly_chart(prepare_type_chart(filtered_gold), use_container_width=True)

    with chart_cols[1]:
        st.subheader("Top estados")
        st.caption("Onde esta a maior concentracao de cervejarias.")
        st.plotly_chart(prepare_state_chart(filtered_gold), use_container_width=True)


def render_operations_section(quality_df: pd.DataFrame, execution_df: pd.DataFrame) -> None:
    latest_run = (
        quality_df.sort_values("checked_at_utc")["run_id"].dropna().astype(str).iloc[-1]
        if not quality_df.empty
        else "-"
    )

    left, right = st.columns((1.05, 0.95))
    with left:
        st.subheader("Saude do pipeline")
        st.caption("Distribuicao dos checks por camada.")
        st.plotly_chart(prepare_quality_chart(quality_df), use_container_width=True)

    with right:
        st.subheader("Ultima execucao")
        st.metric("Run analisado", latest_run)
        if execution_df.empty:
            st.info("Nenhum evento operacional foi encontrado.")
        else:
            latest_event = execution_df.sort_values("event_timestamp_utc").iloc[-1].to_dict()
            event_cols = st.columns(2)
            event_cols[0].metric("Entradas", str(latest_event.get("records_in", "-")))
            event_cols[1].metric("Saidas", str(latest_event.get("records_out", "-")))
            st.caption(str(latest_event.get("details", "-")))

    with st.expander("Ver tabela de qualidade"):
        st.dataframe(build_quality_table(quality_df), use_container_width=True, hide_index=True)


def render_operational_guide(source_mode: str) -> None:
    env = detect_environment_status()
    st.subheader("Guia operacional do projeto")
    st.caption("Um bloco rapido para saber como rodar o projeto e o que ja esta disponivel nesta maquina.")

    status_cols = st.columns(4)
    status_cols[0].metric("Modo atual", SOURCE_MODE_LABELS.get(source_mode, source_mode))
    status_cols[1].metric("Java", env["java"])
    status_cols[2].metric("local_output", env["local_output"])
    status_cols[3].metric("local_output_bad", env["local_output_bad"])

    with st.expander("Como executar o projeto", expanded=False):
        st.markdown("**1. Entrar na pasta do repositorio**")
        st.code('cd "C:\\Users\\leona\\Documents\\GitHub\\bees-data-engineering-case"', language="powershell")
        st.markdown("**2. Instalar dependencias**")
        st.code('python -m pip install -e ".[local,dashboard]"', language="powershell")
        st.markdown("**3. Gerar artefatos locais**")
        st.code("python scripts/run_local_pyspark_demo.py", language="powershell")
        st.markdown("**4. Abrir o dashboard**")
        st.code("python -m streamlit run dashboard/app.py", language="powershell")

    with st.expander("Leitura rapida dos modos", expanded=False):
        st.markdown(
            """
            - `Demo do projeto`: usa o dataset demonstrativo embutido no repositorio.
            - `Artefatos locais`: usa os arquivos gerados pelo pipeline local em `local_output/`.
            - `local_output_bad`: serve para mostrar um cenario com falhas de qualidade.
            """
        )

    with st.expander("Proximos passos recomendados", expanded=False):
        st.markdown(
            """
            - gerar `local_output` para testar o painel com os artefatos locais
            - alternar entre `Demo do projeto` e `Artefatos locais`
            - abrir a tabela de qualidade para validar os checks
            - usar `local_output_bad` para demonstrar o comportamento em falhas
            """
        )


def main() -> None:
    inject_styles()

    available_outputs = discover_available_outputs()
    source_mode, output_root, selected_types, selected_states = render_sidebar_controls(available_outputs)
    data, fallback_message = load_selected_data(source_mode, output_root)

    render_hero(data.source_mode, data.quality)
    render_feedback(st.session_state.pop("dashboard_feedback", None), fallback_message)

    filtered_gold = filter_gold(data.gold, selected_types, selected_states)
    render_business_section(filtered_gold, data.quality)
    st.divider()
    render_operations_section(data.quality, data.execution)
    st.divider()
    render_operational_guide(data.source_mode)


if __name__ == "__main__":
    main()
