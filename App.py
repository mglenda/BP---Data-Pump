from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
from scipy.stats import linregress, pearsonr, spearmanr

import Utilities.FileToolkits as FileToolkits
from Constants import Constants
from DataPumps._core import (
    Route_CorrelationExplorer,
    Route_CountryRegionLookup,
    Route_MainDataset,
)
from DatabaseRouting.DatabaseRoute import DatabaseRoute
from DatabaseRouting.Engines import PostgreSQL


LOG_PATH = Path("app.log")
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
SETTINGS_PATH = "settings.json"

CORRELATION_PAGE_LABELS = {
    "realtime": "Realtime Correlation",
    "precalculated": "Precalculated Correlations",
}


def configure_logging() -> logging.Logger:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    log_path_resolved = str(LOG_PATH.resolve())
    has_file_handler = any(
        isinstance(handler, logging.FileHandler) and getattr(handler, "baseFilename", None) == log_path_resolved
        for handler in root_logger.handlers
    )

    if not has_file_handler:
        file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        root_logger.addHandler(file_handler)

    for logger_name in ("streamlit", "streamlit.runtime", "streamlit.web"):
        streamlit_logger = logging.getLogger(logger_name)
        streamlit_logger.setLevel(logging.INFO)
        streamlit_logger.propagate = True

    app_logger = logging.getLogger(__name__)
    app_logger.info("Application logger initialized.")
    return app_logger


LOG = configure_logging()


def is_streamlit_runtime() -> bool:
    try:
        try:
            from streamlit.runtime.scriptrunner import get_script_run_ctx
        except ImportError:
            from streamlit.runtime.scriptrunner.script_run_context import get_script_run_ctx
        return get_script_run_ctx() is not None
    except ImportError:
        LOG.warning("Could not import Streamlit run context helper.")
        return False
    except Exception:
        LOG.exception("Failed to inspect Streamlit run context.")
        return False


def configure_page() -> None:
    st.set_page_config(
        page_title="Correlation Explorer",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def inject_styles() -> None:
    st.markdown(
        """
        <style>
            header[data-testid="stHeader"] {
                display: none;
            }
            .stAppToolbar {
                display: none;
            }
            [data-testid="stToolbar"] {
                display: none;
            }
            [data-testid="stDecoration"] {
                display: none;
            }
            .stApp {
                background:
                    radial-gradient(circle at top left, rgba(196, 106, 47, 0.14), transparent 26%),
                    radial-gradient(circle at top right, rgba(42, 157, 143, 0.12), transparent 20%),
                    linear-gradient(180deg, #f4efe7 0%, #f8f5ef 100%);
            }
            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #efe3d2 0%, #f7f2ea 100%);
            }
            div[data-testid="stMetric"] {
                background: rgba(251, 248, 243, 0.92);
                border: 1px solid rgba(31, 41, 51, 0.08);
                border-radius: 18px;
                padding: 10px 14px;
            }
            .block-container {
                padding-top: 2.2rem;
                padding-bottom: 2rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_toggle() -> None:
    components.html(
        """
        <script>
        const doc = window.parent.document;
        const buttonId = "custom-sidebar-toggle";

        function clickSidebarControl() {
            const collapsed = doc.querySelector('[data-testid="collapsedControl"] button');
            if (collapsed) {
                collapsed.click();
                return;
            }

            const sidebarButton = doc.querySelector('[data-testid="stSidebarCollapseButton"] button');
            if (sidebarButton) {
                sidebarButton.click();
            }
        }

        let button = doc.getElementById(buttonId);
        if (!button) {
            button = doc.createElement("button");
            button.id = buttonId;
            button.innerText = "Filters";
            button.onclick = clickSidebarControl;
            Object.assign(button.style, {
                position: "fixed",
                top: "16px",
                left: "16px",
                zIndex: "1000000",
                background: "rgba(251, 248, 243, 0.96)",
                color: "#1f2933",
                border: "1px solid rgba(31, 41, 51, 0.12)",
                borderRadius: "999px",
                padding: "8px 14px",
                fontSize: "14px",
                fontWeight: "600",
                cursor: "pointer",
                boxShadow: "0 8px 20px rgba(31, 41, 51, 0.12)"
            });
            doc.body.appendChild(button);
        }
        </script>
        """,
        height=0,
        width=0,
    )


@st.cache_data(show_spinner=False, ttl=600)
def load_indicator_config() -> list[dict]:
    config = FileToolkits.load_json(SETTINGS_PATH)
    LOG.info("Indicator configuration loaded [count]: %s", len(config))
    return config


def build_indicator_labels(indicator_config: list[dict]) -> dict[str, str]:
    return {
        item["column_name"]: item.get("description", item["column_name"].replace("_", " ").title())
        for item in indicator_config
    }


@st.cache_data(show_spinner=False, ttl=86400)
def load_country_names(country_codes: tuple[str, ...]) -> dict[str, str]:
    country_str = ";".join(country_codes)
    url = f"https://api.worldbank.org/v2/country/{country_str}"
    try:
        response = requests.get(
            url,
            params={"format": "json", "per_page": 500},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
        country_names = {
            row["id"]: row["name"]
            for row in rows
            if row.get("id") and row.get("name")
        }
        LOG.info("Country names loaded from World Bank API [count]: %s", len(country_names))
        return country_names
    except Exception:
        LOG.exception("Failed to load country names from World Bank API. Falling back to ISO codes.")
        return {code: code for code in country_codes}


def format_country_label(country_iso: str, country_names: dict[str, str]) -> str:
    return f"{country_names.get(country_iso, country_iso)}"


@st.cache_data(show_spinner=False, ttl=600)
def load_indicator_data(indicator_columns: tuple[str, ...], country_names: dict[str, str]) -> pd.DataFrame:
    LOG.info("Loading indicator dataset.")
    with DatabaseRoute(definition=Route_MainDataset, engine_type=PostgreSQL) as route:
        data = route.get_data()

    for column in indicator_columns:
        if column in data.columns:
            data[column] = pd.to_numeric(data[column], errors="coerce")

    data["year"] = pd.to_numeric(data["year"], errors="coerce").astype("Int64")
    data["country_iso"] = data["country_iso"].astype(str)
    data = data[data["country_iso"].isin(Constants.country_codes)].copy()
    data["country_label"] = data["country_iso"].map(lambda value: format_country_label(value, country_names))

    LOG.info("Indicator dataset loaded [row_count]: %s", len(data))
    return data


@st.cache_data(show_spinner=False, ttl=600)
def load_precalculated_correlations() -> pd.DataFrame:
    LOG.info("Loading precalculated correlations.")
    with DatabaseRoute(definition=Route_CorrelationExplorer, engine_type=PostgreSQL) as route:
        data = route.get_data()

    data["correlation_value"] = pd.to_numeric(data["correlation_value"], errors="coerce")
    data["abs_correlation_value"] = pd.to_numeric(data["abs_correlation_value"], errors="coerce")
    data["p_value"] = pd.to_numeric(data["p_value"], errors="coerce")
    data["observation_count"] = pd.to_numeric(data["observation_count"], errors="coerce")
    data["calculated_at"] = pd.to_datetime(data["calculated_at"], errors="coerce")
    data["pair_label"] = data["variable_x"] + " vs " + data["variable_y"]

    LOG.info("Precalculated correlations loaded [row_count]: %s", len(data))
    return data


@st.cache_data(show_spinner=False, ttl=600)
def load_country_region_lookup() -> pd.DataFrame:
    LOG.info("Loading country-region lookup.")
    with DatabaseRoute(definition=Route_CountryRegionLookup, engine_type=PostgreSQL) as route:
        data = route.get_data()

    data["country_iso"] = data["country_iso"].astype(str)
    data["region_name"] = data["region_name"].fillna("Unknown")
    data = data[data["country_iso"].isin(Constants.country_codes)].copy()
    LOG.info("Country-region lookup loaded [row_count]: %s", len(data))
    return data


def initialize_state(data: pd.DataFrame, indicator_options: list[str]) -> None:
    default_x = "gdp_per_capita" if "gdp_per_capita" in indicator_options else indicator_options[0]
    default_y_candidates = [option for option in indicator_options if option != default_x]
    default_y = "inflation" if "inflation" in default_y_candidates else default_y_candidates[0]

    if "selected_x_indicator" not in st.session_state:
        st.session_state.selected_x_indicator = default_x
    if "selected_y_indicator" not in st.session_state:
        st.session_state.selected_y_indicator = default_y
    if st.session_state.selected_y_indicator == st.session_state.selected_x_indicator:
        st.session_state.selected_y_indicator = default_y_candidates[0]

    min_year = int(data["year"].min())
    max_year = int(data["year"].max())

    if "selected_year_range" not in st.session_state:
        st.session_state.selected_year_range = (min_year, max_year)
    if "selected_region_template" not in st.session_state:
        st.session_state.selected_region_template = None
    if "countries_updated_from_region" not in st.session_state:
        st.session_state.countries_updated_from_region = False
    if "selected_countries" not in st.session_state:
        country_defaults = sorted(data["country_iso"].dropna().unique().tolist())
        preferred = [code for code in Constants.country_codes if code in country_defaults]
        st.session_state.selected_countries = preferred if preferred else country_defaults
    if "selected_method" not in st.session_state:
        st.session_state.selected_method = "pearson"


def on_x_indicator_change() -> None:
    if st.session_state.selected_y_indicator == st.session_state.selected_x_indicator:
        y_options = [
            option
            for option in st.session_state.indicator_options
            if option != st.session_state.selected_x_indicator
        ]
        st.session_state.selected_y_indicator = y_options[0]


def on_region_template_change() -> None:
    region = st.session_state.selected_region_template
    lookup = st.session_state.country_region_lookup

    if not region:
        return

    region_countries = sorted(
        lookup.loc[lookup["region_name"] == region, "country_iso"].dropna().unique().tolist()
    )
    st.session_state.countries_updated_from_region = True
    st.session_state.selected_countries = region_countries


def on_countries_change() -> None:
    if st.session_state.countries_updated_from_region:
        st.session_state.countries_updated_from_region = False
        return
    st.session_state.selected_region_template = None


def build_sidebar_filters(
    data: pd.DataFrame,
    country_region_lookup: pd.DataFrame,
    indicator_labels: dict[str, str],
    country_names: dict[str, str],
) -> tuple[str, str, str, tuple[int, int], list[str]]:
    st.session_state.country_region_lookup = country_region_lookup
    st.session_state.indicator_options = list(indicator_labels.keys())

    all_countries = sorted(data["country_iso"].dropna().unique().tolist())
    region_options = [None] + sorted(country_region_lookup["region_name"].dropna().unique().tolist())

    st.sidebar.subheader("Inputs")
    st.sidebar.selectbox(
        "X indicator",
        options=list(indicator_labels.keys()),
        key="selected_x_indicator",
        format_func=lambda value: indicator_labels.get(value, value),
        on_change=on_x_indicator_change,
    )

    y_options = [
        option
        for option in indicator_labels.keys()
        if option != st.session_state.selected_x_indicator
    ]
    if st.session_state.selected_y_indicator not in y_options:
        st.session_state.selected_y_indicator = y_options[0]

    st.sidebar.selectbox(
        "Y indicator",
        options=y_options,
        key="selected_y_indicator",
        format_func=lambda value: indicator_labels.get(value, value),
    )

    st.sidebar.radio(
        "Correlation method",
        options=["pearson", "spearman"],
        key="selected_method",
        format_func=str.capitalize,
    )

    st.sidebar.slider(
        "Year range",
        min_value=int(data["year"].min()),
        max_value=int(data["year"].max()),
        key="selected_year_range",
    )

    st.sidebar.subheader("Country filters")
    st.sidebar.selectbox(
        "Region template",
        options=region_options,
        key="selected_region_template",
        format_func=lambda value: "All regions" if value is None else value,
        on_change=on_region_template_change,
    )

    st.sidebar.multiselect(
        "Countries",
        options=all_countries,
        key="selected_countries",
        format_func=lambda value: format_country_label(value, country_names),
        on_change=on_countries_change,
    )

    return (
        st.session_state.selected_x_indicator,
        st.session_state.selected_y_indicator,
        st.session_state.selected_method,
        st.session_state.selected_year_range,
        st.session_state.selected_countries,
    )


def compute_realtime_correlation(
    data: pd.DataFrame,
    x_indicator: str,
    y_indicator: str,
    method: str,
    year_range: tuple[int, int],
    selected_countries: list[str],
) -> tuple[pd.DataFrame, float | None, float | None]:
    filtered = data[
        data["year"].between(year_range[0], year_range[1], inclusive="both")
        & data["country_iso"].isin(selected_countries)
    ].copy()

    filtered = filtered[["country_iso", "country_label", "year", x_indicator, y_indicator]].dropna()
    filtered = filtered.rename(columns={x_indicator: "x_value", y_indicator: "y_value"})

    if len(filtered) < 3:
        LOG.info("Realtime correlation skipped because less than 3 overlapping rows were found.")
        return filtered, None, None

    if method == "pearson":
        corr_value, p_value = pearsonr(filtered["x_value"], filtered["y_value"])
    else:
        corr_value, p_value = spearmanr(filtered["x_value"], filtered["y_value"])

    LOG.info(
        "Realtime correlation computed [method]: %s, [rows]: %s, [corr]: %.4f",
        method,
        len(filtered),
        corr_value,
    )
    return filtered, float(corr_value), float(p_value)


def render_header() -> None:
    st.title("Realtime Correlation")
    st.caption(
        "Choose any two indicators from the raw country-year dataset and compute correlation in real time."
    )


def render_metrics(corr_value: float, p_value: float, row_count: int) -> None:
    metric_cols = st.columns(3)
    metric_cols[0].metric("Correlation", f"{corr_value:.3f}")
    metric_cols[1].metric("P-value", f"{p_value:.5f}")
    metric_cols[2].metric("Overlapping points", f"{row_count:,}")


def render_scatter_plot(
    scatter_data: pd.DataFrame,
    x_indicator: str,
    y_indicator: str,
    indicator_labels: dict[str, str],
) -> None:
    try:
        import plotly.express as px
    except ImportError:
        LOG.warning("Plotly is not installed. Realtime scatter cannot be rendered.")
        st.warning("Plotly is not installed in the active environment.")
        return

    scatter_chart = px.scatter(
        scatter_data,
        x="x_value",
        y="y_value",
        color="country_label",
        hover_data=["country_label", "year"],
        labels={
            "x_value": indicator_labels.get(x_indicator, x_indicator),
            "y_value": indicator_labels.get(y_indicator, y_indicator),
            "country_label": "Country",
            "year": "Year",
        },
        color_discrete_sequence=px.colors.qualitative.Safe,
        title=f"{indicator_labels.get(x_indicator, x_indicator)} vs {indicator_labels.get(y_indicator, y_indicator)}",
    )

    trendline_source = scatter_data[["x_value", "y_value"]].dropna()
    if len(trendline_source) >= 2 and trendline_source["x_value"].nunique() > 1:
        regression = linregress(trendline_source["x_value"], trendline_source["y_value"])
        trendline_data = trendline_source[["x_value"]].drop_duplicates().sort_values("x_value").copy()
        trendline_data["y_trend"] = (
            regression.slope * trendline_data["x_value"] + regression.intercept
        )
        scatter_chart.add_scatter(
            x=trendline_data["x_value"],
            y=trendline_data["y_trend"],
            mode="lines",
            showlegend=False,
            line={"color": "rgba(15, 23, 42, 0.75)", "width": 8},
            hoverinfo="skip",
        )
        scatter_chart.add_scatter(
            x=trendline_data["x_value"],
            y=trendline_data["y_trend"],
            mode="lines",
            name="Trend line",
            line={"color": "#38bdf8", "width": 4.5},
            hovertemplate="Trend line<br>X: %{x:.3f}<br>Y: %{y:.3f}<extra></extra>",
        )

    scatter_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=60, b=10),
    )
    st.plotly_chart(scatter_chart, width="stretch")


def render_realtime_legend() -> None:
    with st.expander("Legend", expanded=True):
        st.write("- Each point is one overlap between X and Y values for a specific country and year.")
        st.write("- The trend line is a linear best-fit line computed from the currently visible points.")
        st.write("- Correlation is computed only from rows where both selected indicators are present.")
        st.write("- Pearson is better for linear relationships.")
        st.write("- Spearman is better for rank-order or monotonic relationships.")
        st.write("- P-value helps estimate whether the observed relationship may be random.")
        st.write("- Region template is only a helper for quickly pre-filling the country selection.")


def build_precalculated_sidebar_filters(data: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.subheader("Precalculated correlation filters")

    methods = sorted(data["method"].dropna().unique().tolist())
    selected_methods = st.sidebar.multiselect(
        "Method",
        options=methods,
        default=methods,
        key="corr_methods",
        format_func=lambda value: value.capitalize(),
    )

    strengths = sorted(data["strength_label"].dropna().unique().tolist())
    selected_strengths = st.sidebar.multiselect(
        "Strength",
        options=strengths,
        default=strengths,
        key="corr_strengths",
        format_func=lambda value: value.replace("_", " ").title(),
    )

    directions = sorted(data["direction"].dropna().unique().tolist())
    selected_directions = st.sidebar.multiselect(
        "Direction",
        options=directions,
        default=directions,
        key="corr_directions",
        format_func=lambda value: value.capitalize(),
    )

    indicator_options = sorted(
        pd.unique(pd.concat([data["variable_x"], data["variable_y"]], ignore_index=True).dropna()).tolist()
    )
    selected_indicators = st.sidebar.multiselect(
        "Indicator contains",
        options=indicator_options,
        key="corr_indicators",
    )

    min_observations = st.sidebar.slider(
        "Minimum observations",
        min_value=int(data["observation_count"].min()),
        max_value=int(data["observation_count"].max()),
        value=int(data["observation_count"].min()),
        key="corr_min_observations",
    )

    abs_corr_range = st.sidebar.slider(
        "Absolute correlation range",
        min_value=0.0,
        max_value=1.0,
        value=(0.0, 1.0),
        step=0.01,
        key="corr_abs_range",
    )

    only_significant = st.sidebar.checkbox(
        "Only significant (p <= 0.05)",
        key="corr_only_significant",
    )

    filtered = data.copy()
    if selected_methods:
        filtered = filtered[filtered["method"].isin(selected_methods)]
    if selected_strengths:
        filtered = filtered[filtered["strength_label"].isin(selected_strengths)]
    if selected_directions:
        filtered = filtered[filtered["direction"].isin(selected_directions)]
    if selected_indicators:
        filtered = filtered[
            filtered["variable_x"].isin(selected_indicators) | filtered["variable_y"].isin(selected_indicators)
        ]

    filtered = filtered[
        filtered["abs_correlation_value"].between(abs_corr_range[0], abs_corr_range[1], inclusive="both")
        & (filtered["observation_count"] >= min_observations)
    ]

    if only_significant:
        filtered = filtered[filtered["p_value"].notna() & (filtered["p_value"] <= 0.05)]

    LOG.info("Precalculated filters applied [visible_rows]: %s", len(filtered))
    return filtered.reset_index(drop=True)


def render_precalculated_legend() -> None:
    with st.expander("Legend", expanded=True):
        st.write("- This page analyzes already computed correlations stored in fact_indicator_correlations.")
        st.write("- The heatmap shows how the same pair behaves on three levels: global result, average region behavior, and average country behavior.")
        st.write("- A strong global result combined with weak region/country results may indicate aggregation effects.")
        st.write("- The detail chart shows all stored correlation values for one selected pair across levels.")
        st.write("- Global has one stored result, region has one result per region, and country has one result per country.")
        st.write("- Region and country cells combine average strength with dominant direction, so the color scale still runs from negative to positive values.")


def build_pair_level_summary(filtered: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        filtered.groupby(["pair_label", "method", "scope_type"], as_index=False)
        .agg(
            avg_correlation=("correlation_value", "mean"),
            avg_abs_correlation=("abs_correlation_value", "mean"),
            max_abs_correlation=("abs_correlation_value", "max"),
            scope_count=("scope_value", "nunique"),
        )
    )

    summary = grouped.pivot_table(
        index=["pair_label", "method"],
        columns="scope_type",
        values=["avg_correlation", "avg_abs_correlation", "scope_count"],
    )
    summary.columns = [f"{metric}_{scope}" for metric, scope in summary.columns]
    summary = summary.reset_index()

    for column in [
        "avg_correlation_global",
        "avg_correlation_region",
        "avg_correlation_country",
        "avg_abs_correlation_global",
        "avg_abs_correlation_region",
        "avg_abs_correlation_country",
    ]:
        if column not in summary.columns:
            summary[column] = pd.NA

    summary["interesting_score"] = (
        summary["avg_abs_correlation_global"].fillna(0) * 3
        + summary["avg_abs_correlation_region"].fillna(0) * 2
        + summary["avg_abs_correlation_country"].fillna(0)
    )

    return summary.sort_values(
        ["avg_abs_correlation_global", "interesting_score"],
        ascending=[False, False],
    ).reset_index(drop=True)


def render_pair_level_heatmap(summary: pd.DataFrame) -> None:
    if summary.empty:
        st.info("No pair-level summary is available for the current filters.")
        return

    try:
        import plotly.express as px
    except ImportError:
        LOG.warning("Plotly is not installed. Precalculated heatmap cannot be rendered.")
        st.warning("Plotly is not installed in the active environment.")
        return

    top_summary = summary.head(15).copy()
    top_summary["pair_method"] = top_summary["pair_label"] + " | " + top_summary["method"].str.capitalize()
    top_summary["region_display"] = (
        top_summary["avg_abs_correlation_region"].fillna(0)
        * top_summary["avg_correlation_region"].fillna(0).apply(lambda value: 1 if value >= 0 else -1)
    )
    top_summary["country_display"] = (
        top_summary["avg_abs_correlation_country"].fillna(0)
        * top_summary["avg_correlation_country"].fillna(0).apply(lambda value: 1 if value >= 0 else -1)
    )
    heatmap_frame = (
        top_summary[
            [
                "pair_method",
                "avg_correlation_global",
                "region_display",
                "country_display",
            ]
        ]
        .rename(
            columns={
                "avg_correlation_global": "Global",
                "region_display": "Region avg",
                "country_display": "Country avg",
            }
        )
        .set_index("pair_method")
    )

    heatmap = px.imshow(
        heatmap_frame,
        color_continuous_scale=["#7f1d1d", "#f8f5ef", "#0f766e"],
        zmin=-1,
        zmax=1,
        aspect="auto",
        labels={"x": "Level", "y": "Pair | Method", "color": "Correlation value"},
        title="Interesting pairs across global, region and country levels",
    )
    heatmap.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=60, b=10),
    )
    st.plotly_chart(heatmap, width="stretch")


def render_pair_detail_explorer(filtered: pd.DataFrame, summary: pd.DataFrame) -> None:
    if summary.empty:
        return

    pair_options = summary.head(25).copy()
    pair_options["pair_method"] = pair_options["pair_label"] + " | " + pair_options["method"].str.capitalize()

    selected_pair_method = st.selectbox(
        "Inspect pair across levels",
        options=pair_options["pair_method"].tolist(),
    )

    selected_row = pair_options[pair_options["pair_method"] == selected_pair_method].iloc[0]
    pair_label = selected_row["pair_label"]
    method = selected_row["method"]

    pair_rows = filtered[
        (filtered["pair_label"] == pair_label)
        & (filtered["method"] == method)
    ].copy()
    pair_rows["p_value_display"] = pair_rows["p_value"].map(
        lambda value: f"{value:.5f}" if pd.notna(value) else None
    )

    if pair_rows.empty:
        st.info("No detail rows are available for the selected pair.")
        return

    try:
        import plotly.express as px
    except ImportError:
        LOG.warning("Plotly is not installed. Precalculated detail chart cannot be rendered.")
        st.warning("Plotly is not installed in the active environment.")
        return

    pair_rows["scope_type_label"] = pair_rows["scope_type"].map(
        {"global": "Global", "region": "Region", "country": "Country"}
    ).fillna(pair_rows["scope_type"])
    detail_chart = px.strip(
        pair_rows,
        x="scope_type_label",
        y="correlation_value",
        color="scope_type_label",
        hover_data={
            "scope_value": True,
            "strength_label": True,
            "observation_count": True,
            "p_value_display": True,
            "p_value": False,
        },
        labels={
            "scope_type_label": "Level",
            "correlation_value": "Correlation value",
            "scope_value": "Scope value",
            "p_value_display": "P-value",
        },
        title=f"Detailed behavior of {pair_label} ({method.capitalize()}) across levels",
    )
    detail_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=60, b=10),
        showlegend=False,
    )
    detail_chart.update_traces(marker={"size": 10, "opacity": 0.9})
    detail_chart.add_hline(y=0, line_width=1, line_dash="dash", line_color="#5b6874")
    st.plotly_chart(detail_chart, width="stretch")

    st.dataframe(
        pair_rows[
            [
                "scope_type",
                "scope_value",
                "correlation_value",
                "abs_correlation_value",
                "strength_label",
                "direction",
                "p_value",
                "observation_count",
                "calculated_at",
            ]
        ].rename(
            columns={
                "scope_type": "Scope type",
                "scope_value": "Scope value",
                "correlation_value": "Correlation",
                "abs_correlation_value": "Absolute correlation",
                "strength_label": "Strength",
                "direction": "Direction",
                "p_value": "P-value",
                "observation_count": "Observations",
                "calculated_at": "Calculated at",
            }
        ),
        width="stretch",
        hide_index=True,
        column_config={
            "Correlation": st.column_config.NumberColumn(format="%.4f"),
            "Absolute correlation": st.column_config.NumberColumn(format="%.4f"),
            "P-value": st.column_config.NumberColumn(format="%.5f"),
            "Observations": st.column_config.NumberColumn(format="%d"),
            "Calculated at": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
        },
    )


def render_precalculated_page(data: pd.DataFrame) -> None:
    st.title("Precalculated Correlations")
    st.caption(
        "Explore the stored correlation results directly from public.fact_indicator_correlations without recomputing them."
    )

    filtered = build_precalculated_sidebar_filters(data)

    if filtered.empty:
        st.warning("No precalculated correlations match the current filters.")
        render_precalculated_legend()
        return

    pair_summary = build_pair_level_summary(filtered)

    summary_cols = st.columns(4)
    summary_cols[0].metric("Visible rows", f"{len(filtered):,}")
    summary_cols[1].metric("Unique scopes", f"{filtered['scope_value'].nunique():,}")
    summary_cols[2].metric("Unique pairs", f"{filtered['pair_label'].nunique():,}")
    summary_cols[3].metric("Average |correlation|", f"{filtered['abs_correlation_value'].mean():.3f}")

    render_pair_level_heatmap(pair_summary)
    render_pair_detail_explorer(filtered, pair_summary)

    render_precalculated_legend()
    LOG.info("Precalculated correlation page rendered successfully.")


def render_realtime_page(
    data: pd.DataFrame,
    country_region_lookup: pd.DataFrame,
    indicator_labels: dict[str, str],
    country_names: dict[str, str],
) -> None:
    initialize_state(data, list(indicator_labels.keys()))
    render_header()

    x_indicator, y_indicator, method, year_range, selected_countries = build_sidebar_filters(
        data,
        country_region_lookup,
        indicator_labels,
        country_names,
    )

    scatter_data, corr_value, p_value = compute_realtime_correlation(
        data,
        x_indicator,
        y_indicator,
        method,
        year_range,
        selected_countries,
    )

    if scatter_data.empty or corr_value is None or p_value is None:
        st.warning("At least 3 overlapping non-null rows are required to compute correlation.")
        render_realtime_legend()
        return

    render_metrics(corr_value, p_value, len(scatter_data))
    render_scatter_plot(scatter_data, x_indicator, y_indicator, indicator_labels)

    st.dataframe(
        scatter_data.rename(
            columns={
                "country_label": "Country",
                "year": "Year",
                "x_value": indicator_labels.get(x_indicator, x_indicator),
                "y_value": indicator_labels.get(y_indicator, y_indicator),
            }
        ),
        width="stretch",
        hide_index=True,
    )
    render_realtime_legend()
    LOG.info("Realtime correlation page rendered successfully.")


def run_streamlit_app() -> None:
    configure_page()
    inject_styles()
    render_sidebar_toggle()

    indicator_config = load_indicator_config()
    indicator_labels = build_indicator_labels(indicator_config)
    country_names = load_country_names(tuple(Constants.country_codes))

    st.sidebar.title("Pages")
    selected_page = st.sidebar.radio(
        "Page",
        options=list(CORRELATION_PAGE_LABELS.keys()),
        format_func=lambda value: CORRELATION_PAGE_LABELS[value],
    )

    indicator_data = load_indicator_data(tuple(indicator_labels.keys()), country_names)
    country_region_lookup = load_country_region_lookup()
    precalculated_correlations = load_precalculated_correlations()

    if selected_page == "realtime" and indicator_data.empty:
        LOG.warning("Indicator explorer loaded with no data.")
        st.error("The indicator table returned no data.")
        return

    if selected_page == "precalculated" and precalculated_correlations.empty:
        LOG.warning("Precalculated correlation explorer loaded with no data.")
        st.error("The correlation table returned no data.")
        return

    if selected_page == "realtime":
        render_realtime_page(
            indicator_data,
            country_region_lookup,
            indicator_labels,
            country_names,
        )
    else:
        render_precalculated_page(precalculated_correlations)


def main() -> None:
    if not is_streamlit_runtime():
        message = "Streamlit UI must be started with: streamlit run App.py"
        LOG.warning(message)
        print(message)
        return

    LOG.info("Starting correlation app.")
    try:
        run_streamlit_app()
    except Exception:
        LOG.exception("Unhandled exception while rendering the Streamlit app.")
        st.error("The application failed while rendering. Check app.log for details.")
        raise


if __name__ == "__main__":
    main()
