
import re
import math
import json
import time
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# --- Google OAuth (KW login) + Google Sheets loader -------------------------
# This avoids sharing the sheet to a service account (often blocked in corporate domains).
# Each viewer signs in with their KW Google account and the app reads the sheet using their permissions.

GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

def _get_query_param(name: str):
    """Compatibility helper across Streamlit versions."""
    try:
        # Streamlit >= 1.30-ish
        val = st.query_params.get(name)
        if isinstance(val, list):
            return val[0] if val else None
        return val
    except Exception:
        qp = st.experimental_get_query_params()
        v = qp.get(name)
        return v[0] if isinstance(v, list) and v else (v if isinstance(v, str) else None)

def _clear_query_params():
    try:
        st.query_params.clear()
    except Exception:
        st.experimental_set_query_params()

def _google_oauth_config():
    cfg = dict(st.secrets.get("google_oauth", {}))
    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")
    # IMPORTANT: For Streamlit Community Cloud, set redirect_uri to your deployed app URL:
    #   https://<your-app>.streamlit.app
    # Also add http://localhost:8501 for local dev in Google Cloud OAuth settings.
    redirect_uri = cfg.get("redirect_uri", "http://localhost:8501")
    return client_id, client_secret, redirect_uri

def google_get_credentials():
    """Returns google-auth Credentials in session, or renders a login button."""
    # Lazy imports so Excel-only users don't need these deps.
    try:
        from google_auth_oauthlib.flow import Flow
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
    except Exception as e:
        st.error(
            "Google login dependencies are missing. Add these to requirements.txt and redeploy:\n"
            "- google-auth\n- google-auth-oauthlib\n- gspread\n\n"
            f"Import error: {e}"
        )
        st.stop()

    client_id, client_secret, redirect_uri = _google_oauth_config()
    if not client_id or not client_secret:
        st.error(
            "Missing OAuth settings. Add these in Streamlit Secrets (Settings → Secrets):\n\n"
            "[google_oauth]\n"
            "client_id = YOUR_CLIENT_ID\n"
            "client_secret = YOUR_CLIENT_SECRET\n"
            "redirect_uri = http://localhost:8501"
        )
        st.stop()

    # If we already have creds stored, rebuild and refresh if needed.
    if st.session_state.get("google_creds_json"):
        creds = Credentials.from_authorized_user_info(
            json.loads(st.session_state["google_creds_json"]),
            scopes=GOOGLE_SHEETS_SCOPES,
        )
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                st.session_state["google_creds_json"] = creds.to_json()
            except Exception:
                # If refresh fails, force re-login
                st.session_state.pop("google_creds_json", None)
                st.session_state.pop("google_oauth_state", None)
                _clear_query_params()
                st.rerun()
        return creds

    # Handle callback from Google (code + state in URL)
    code = _get_query_param("code")
    state = _get_query_param("state")

    client_config = {
        "web": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [redirect_uri],
        }
    }
    flow = Flow.from_client_config(
        client_config,
        scopes=GOOGLE_SHEETS_SCOPES,
        redirect_uri=redirect_uri,
    )

    if code:
        # Basic CSRF check: state should match what we generated
        expected_state = st.session_state.get("google_oauth_state")
        if expected_state and state and state != expected_state:
            st.error("Login state mismatch. Please try signing in again.")
            _clear_query_params()
            st.session_state.pop("google_oauth_state", None)
            st.stop()

        try:
            flow.fetch_token(code=code)
            creds = flow.credentials
            st.session_state["google_creds_json"] = creds.to_json()
            st.session_state.pop("google_oauth_state", None)
            _clear_query_params()
            st.rerun()
        except Exception as e:
            st.error(f"Google login failed: {e}")
            _clear_query_params()
            st.stop()

    # Not logged in yet → show sign-in link
    auth_url, new_state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    st.session_state["google_oauth_state"] = new_state

    st.info("Sign in with your KW Google account to load the latest sheet data.")
    try:
        st.link_button("Sign in with Google", auth_url, use_container_width=True)
    except Exception:
        st.markdown(f"[Sign in with Google]({auth_url})")

    st.stop()

def load_raw_google_sheet(spreadsheet_url_or_id: str, worksheet_name: str, creds_json: str) -> pd.DataFrame:
    """Loads a Google Sheet tab into a raw DataFrame similar to pd.read_excel(..., header=None)."""
    try:
        import gspread
        from google.oauth2.credentials import Credentials
    except Exception as e:
        st.error(
            "Google Sheets dependencies are missing. Add these to requirements.txt and redeploy:\n"
            "- gspread\n- google-auth\n\n"
            f"Import error: {e}"
        )
        st.stop()

    creds = Credentials.from_authorized_user_info(json.loads(creds_json), scopes=GOOGLE_SHEETS_SCOPES)
    gc = gspread.authorize(creds)

    try:
        if "docs.google.com" in spreadsheet_url_or_id:
            sh = gc.open_by_url(spreadsheet_url_or_id)
        else:
            sh = gc.open_by_key(spreadsheet_url_or_id)
    except Exception as e:
        raise RuntimeError(f"Could not open spreadsheet. Check the URL/ID and your access. Details: {e}")

    try:
        ws = sh.worksheet(worksheet_name)
    except Exception as e:
        raise RuntimeError(f"Could not find worksheet/tab '{worksheet_name}'. Details: {e}")

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    max_len = max(len(r) for r in values)
    padded = [r + [""] * (max_len - len(r)) for r in values]
    raw = pd.DataFrame(padded).replace("", np.nan)
    return raw
# ---------------------------------------------------------------------------

MONTH_PAT = re.compile(r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}$", re.I)

# Map metric header labels (lowercased) -> internal field names
EXPECTED_METRICS = {
    "leads": "leads",
    "interested (qualifying leads )": "qualified",
    "interested (qualifying leads)": "qualified",
    "qualified/ interested": "qualified",
    "site visits": "site_visits_reported",
    "ff meeting": "ff_meetings",
    "ff meetings": "ff_meetings",
    "sales": "sales",
    "amount spend": "spend",
    "spend": "spend",
    # Optional: Site visits attributable to current month leads (if present in sheet)
    "current month lead vs site visit": "site_visits_current_leads",
    "current month leads vs site visit": "site_visits_current_leads",
}

DEFAULT_EWI_WEIGHTS = {
    "cpsale_up": 0.30,
    "close_down": 0.30,
    "spend_vs_sales": 0.20,
    "visit_down": 0.20
}

DEFAULT_EXCLUDE_PATTERNS = [
    r"\bcp\b", r"dealer", r"referral", r"referrals"
]

def _safe_div(a, b):
    a = np.asarray(a, dtype="float64")
    b = np.asarray(b, dtype="float64")

    # Scalar denominator (Series / total)
    if b.ndim == 0:
        if np.isnan(b) or b == 0:
            return np.full_like(a, np.nan, dtype="float64")
        return a / b

    out = np.full_like(a, np.nan, dtype="float64")
    mask = (b != 0) & ~np.isnan(b)
    out[mask] = a[mask] / b[mask]
    return out

def _month_sort_key(ym: str):
    try:
        y, m = ym.split("-")
        return int(y) * 100 + int(m)
    except Exception:
        return ym

@st.cache_data(show_spinner=False)
def load_raw_excel(file_bytes: bytes, sheet_name: str):
    return pd.read_excel(file_bytes, sheet_name=sheet_name, header=None)

def find_header_rows(raw: pd.DataFrame):
    header_row = None
    for r in range(min(120, raw.shape[0])):
        v0 = str(raw.iat[r, 0]).strip().lower() if not pd.isna(raw.iat[r, 0]) else ""
        v1 = str(raw.iat[r, 1]).strip().lower() if not pd.isna(raw.iat[r, 1]) else ""
        v2 = str(raw.iat[r, 2]).strip().lower() if not pd.isna(raw.iat[r, 2]) else ""
        if v0 == "project name" and v1.startswith("source") and v2.startswith("sub"):
            header_row = r
            break
    if header_row is None:
        raise ValueError("Could not find a header row with 'Project Name', 'Source', 'Sub Source' in the first three columns.")

    best_count = 0
    month_row = None
    for r in range(max(0, header_row - 15), header_row):
        vals = raw.iloc[r, :].tolist()
        count = sum(1 for v in vals if isinstance(v, str) and MONTH_PAT.match(v.strip()))
        if count > best_count:
            best_count = count
            month_row = r
    if month_row is None or best_count == 0:
        raise ValueError("Could not detect a month header row (e.g., 'July 2025', 'November 2025').")

    return header_row, month_row

def parse_media_table(raw: pd.DataFrame):
    """
    Parses the KW media sheet into a normalized long table with optional current-lead site visits.

    Output columns:
      month (YYYY-MM), month_label, project, source, sub_source,
      leads, qualified, site_visits_reported, site_visits_current_leads, ff_meetings, sales, spend
    """
    header_row, month_row = find_header_rows(raw)
    metric_row = header_row

    # Month header positions
    month_headers = []
    for c in range(raw.shape[1]):
        v = raw.iat[month_row, c]
        if isinstance(v, str):
            v2 = v.strip()
            if MONTH_PAT.match(v2):
                month_headers.append((c, v2))
    month_headers.sort(key=lambda x: x[0])
    if not month_headers:
        raise ValueError("No month headers found in the sheet.")

    # Build month blocks: start col -> before next month header col
    blocks = []
    for i, (c0, label) in enumerate(month_headers):
        c1 = (month_headers[i + 1][0] - 1) if i + 1 < len(month_headers) else raw.shape[1] - 1
        blocks.append((label, c0, c1))

    data_start = header_row + 1

    proj_col = raw.iloc[data_start:, 0].copy()
    source_col = raw.iloc[data_start:, 1].copy()
    sub_col = raw.iloc[data_start:, 2].copy()

    # Forward-fill project
    projects = []
    current_proj = None
    for v in proj_col:
        if isinstance(v, str):
            lv = v.lower()
            if ("kw" in lv) and (("delhi" in lv) or ("blue" in lv) or ("pearl" in lv)):
                current_proj = v.strip()
        projects.append(current_proj)

    records = []
    for month_label, c0, c1 in blocks:
        # Locate metric columns inside this month block
        metric_cols = {}
        for col in range(c0, c1 + 1):
            met = raw.iat[metric_row, col]
            if isinstance(met, str):
                key = met.strip().lower()
                if key in EXPECTED_METRICS:
                    metric_cols[EXPECTED_METRICS[key]] = col

        if "leads" not in metric_cols:
            continue

        month_dt = pd.to_datetime("1 " + month_label, errors="coerce")
        if pd.isna(month_dt):
            continue
        month_key = month_dt.strftime("%Y-%m")

        for i_row, row_idx in enumerate(range(data_start, raw.shape[0])):
            proj = projects[i_row]
            if proj is None:
                continue

            src = source_col.iat[i_row]
            sub = sub_col.iat[i_row]
            src_str = str(src).strip() if not pd.isna(src) else ""
            sub_str = str(sub).strip() if not pd.isna(sub) else ""

            # Exclude totals rows (we recompute totals ourselves)
            if "grand total" in src_str.lower() or "both project total" in src_str.lower():
                continue

            def get_num(field, default=0.0):
                col = metric_cols.get(field)
                if col is None:
                    return default
                v = raw.iat[row_idx, col]
                vn = pd.to_numeric(v, errors="coerce")
                return float(vn) if not pd.isna(vn) else default

            leads = get_num("leads", 0.0)
            qualified = get_num("qualified", 0.0)
            site_visits_reported = get_num("site_visits_reported", 0.0)
            site_visits_current_leads = get_num("site_visits_current_leads", np.nan)  # optional
            ff_meetings = get_num("ff_meetings", 0.0)
            sales = get_num("sales", 0.0)
            spend = get_num("spend", 0.0)

            any_num = any([
                leads, qualified, site_visits_reported, ff_meetings, sales, spend,
                (False if pd.isna(site_visits_current_leads) else site_visits_current_leads != 0)
            ])
            if not any_num:
                continue

            primary_source = src_str if src_str else sub_str
            if not primary_source:
                continue

            records.append({
                "month": month_key,
                "month_label": month_label,
                "project": proj.strip(),
                "source": re.sub(r"\s+", " ", primary_source).strip(),
                "sub_source": re.sub(r"\s+", " ", sub_str).strip(),
                "leads": float(leads),
                "qualified": float(qualified),
                "site_visits_reported": float(site_visits_reported),
                "site_visits_current_leads": float(site_visits_current_leads) if not pd.isna(site_visits_current_leads) else np.nan,
                "ff_meetings": float(ff_meetings),
                "sales": float(sales),
                "spend": float(spend),
            })

    out = pd.DataFrame.from_records(records)
    if out.empty:
        raise ValueError("Parsed 0 rows. This usually means the selected sheet isn't the media performance table.")

    out["project"] = out["project"].str.replace(r"\s+", " ", regex=True).str.strip()
    out["source"] = out["source"].str.replace(r"\s+", " ", regex=True).str.strip()

    for c in ["leads", "qualified", "site_visits_reported", "ff_meetings", "sales", "spend"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    out["site_visits_current_leads"] = pd.to_numeric(out["site_visits_current_leads"], errors="coerce")
    return out

def apply_exclusions(df: pd.DataFrame, exclude_sources: list[str]):
    if not exclude_sources:
        return df
    return df[~df["source"].isin(exclude_sources)].copy()

def compute_monthly_totals(df: pd.DataFrame, site_visit_col: str) -> pd.DataFrame:
    base_cols = ["leads","qualified",site_visit_col,"ff_meetings","sales","spend"]
    totals = df.groupby("month")[base_cols].sum().rename(columns={site_visit_col:"site_visits"})
    totals = totals.sort_index(key=lambda idx: [ _month_sort_key(x) for x in idx ])
    return totals

def compute_kpis(totals: pd.DataFrame) -> pd.DataFrame:
    k = totals.copy()
    k["qualified_rate"] = _safe_div(k["qualified"], k["leads"])
    k["visit_rate_qualified"] = _safe_div(k["site_visits"], k["qualified"])
    k["visit_rate_leads"] = _safe_div(k["site_visits"], k["leads"])
    k["meeting_rate"] = _safe_div(k["ff_meetings"], k["site_visits"])
    k["close_rate_ff"] = _safe_div(k["sales"], k["ff_meetings"])
    k["close_rate_visits"] = _safe_div(k["sales"], k["site_visits"])

    k["cpl"] = _safe_div(k["spend"], k["leads"])
    k["cpql"] = _safe_div(k["spend"], k["qualified"])
    k["cpsv"] = _safe_div(k["spend"], k["site_visits"])
    k["cpsale"] = _safe_div(k["spend"], k["sales"])
    return k

def compute_ewi_components(kpi_df: pd.DataFrame, threshold: float, weights):
    wsum = float(sum(weights.values()))

    def pct_change(cur, prev):
        if pd.isna(cur) or pd.isna(prev) or prev == 0:
            return np.nan
        return (cur - prev) / prev

    def clip01(x):
        if pd.isna(x):
            return 0.0
        return float(np.clip(x / threshold, 0, 1))

    idx = list(kpi_df.index)
    rows = []
    for i, m in enumerate(idx):
        if i == 0:
            rows.append({"month": m, "p_cpsale_up": np.nan, "p_close_down": np.nan, "p_spend_vs_sales": np.nan, "p_visit_down": np.nan, "ewi": np.nan})
            continue
        prev = kpi_df.iloc[i - 1]
        cur = kpi_df.iloc[i]

        cpsale_ch = pct_change(cur["cpsale"], prev["cpsale"])
        close_ch = pct_change(cur["close_rate_visits"], prev["close_rate_visits"])
        spend_ch = pct_change(cur["spend"], prev["spend"])
        sales_ch = pct_change(cur["sales"], prev["sales"])
        visit_ch = pct_change(cur["visit_rate_leads"], prev["visit_rate_leads"])

        p1 = clip01(max(0.0, cpsale_ch if not pd.isna(cpsale_ch) else 0.0))
        p2 = clip01(max(0.0, -(close_ch if not pd.isna(close_ch) else 0.0)))

        if pd.isna(spend_ch) or pd.isna(sales_ch):
            diff = 0.0
        else:
            diff = spend_ch - sales_ch
        p3 = clip01(max(0.0, diff))
        p4 = clip01(max(0.0, -(visit_ch if not pd.isna(visit_ch) else 0.0)))

        penalty = (
            weights["cpsale_up"] * p1 +
            weights["close_down"] * p2 +
            weights["spend_vs_sales"] * p3 +
            weights["visit_down"] * p4
        ) / wsum

        ewi = 100.0 * (1.0 - penalty)
        ewi = float(np.clip(ewi, 0, 100))
        rows.append({"month": m, "p_cpsale_up": p1, "p_close_down": p2, "p_spend_vs_sales": p3, "p_visit_down": p4, "ewi": ewi})
    return pd.DataFrame(rows).set_index("month")

def rag_color(value: float, target: float, higher_is_better: bool, amber_band: float = 0.10):
    if target is None or (isinstance(target, float) and np.isnan(target)) or target == 0:
        return ("Grey", "#6b7280")

    if higher_is_better:
        if value >= target:
            return ("Green", "#16a34a")
        elif value >= (1 - amber_band) * target:
            return ("Amber", "#f59e0b")
        else:
            return ("Red", "#dc2626")
    else:
        if value <= target:
            return ("Green", "#16a34a")
        elif value <= (1 + amber_band) * target:
            return ("Amber", "#f59e0b")
        else:
            return ("Red", "#dc2626")

def fmt_num(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    if abs(x) >= 1e7:
        return f"{x/1e7:.2f} Cr"
    if abs(x) >= 1e5:
        return f"{x/1e5:.2f} L"
    if abs(x) >= 1e3:
        return f"{x:,.0f}"
    if float(x).is_integer():
        return f"{int(x)}"
    return f"{x:.2f}"

def fmt_pct(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x*100:.1f}%"

def tile_html(title, value_str, delta_str, target_str, rag_name, rag_hex):
    return f"""
    <div style="
        border: 1px solid #e5e7eb;
        border-left: 6px solid {rag_hex};
        border-radius: 14px;
        padding: 12px 14px;
        background: #ffffff;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        height: 110px;
    ">
      <div style="font-size: 12px; color: #6b7280; margin-bottom: 6px;">{title}</div>
      <div style="display:flex; align-items: baseline; justify-content: space-between;">
        <div style="font-size: 26px; font-weight: 700; color: #111827;">{value_str}</div>
        <div style="font-size: 12px; color: #6b7280;">{rag_name}</div>
      </div>
      <div style="margin-top: 6px; display:flex; justify-content: space-between; font-size: 12px;">
        <div style="color:#111827;">MoM: <b>{delta_str}</b></div>
        <div style="color:#6b7280;">Target: {target_str}</div>
      </div>
    </div>
    """

def build_channel_table(df_month: pd.DataFrame, site_visit_col: str):
    t = df_month.groupby("source")[["leads","qualified",site_visit_col,"ff_meetings","sales","spend"]].sum().rename(columns={site_visit_col:"site_visits"})
    t["cpl"] = _safe_div(t["spend"], t["leads"])
    t["cpql"] = _safe_div(t["spend"], t["qualified"])
    t["cpsv"] = _safe_div(t["spend"], t["site_visits"])
    t["cpsale"] = _safe_div(t["spend"], t["sales"])

    total_spend = float(t["spend"].sum()) if len(t) else 0.0
    total_sales = float(t["sales"].sum()) if len(t) else 0.0

    t["spend_share"] = (t["spend"] / total_spend) if total_spend > 0 else np.nan
    t["sales_share"] = (t["sales"] / total_sales) if total_sales > 0 else np.nan
    # Waste index only makes sense if there are sales this month
    t["waste_index"] = (t["spend_share"] - t["sales_share"]) if (total_spend > 0 and total_sales > 0) else np.nan

    return t.reset_index().sort_values("waste_index", ascending=False, na_position="last")

def trailing_avg(series: pd.Series, window=3):
    s = series.dropna()
    if len(s) == 0:
        return np.nan
    return float(s.tail(min(window, len(s))).mean())

def compute_source_timeseries(df: pd.DataFrame, site_visit_col: str):
    g = df.groupby(["source","month"])[["leads","qualified",site_visit_col,"sales","spend"]].sum().rename(columns={site_visit_col:"site_visits"})
    g["cpql"] = _safe_div(g["spend"], g["qualified"])
    g["cpsale"] = _safe_div(g["spend"], g["sales"])
    return g.reset_index()

def top_sources_by_spend(df: pd.DataFrame, k: int = 8):
    s = df.groupby("source")["spend"].sum().sort_values(ascending=False)
    return s.head(k).index.tolist()

def main():
    st.set_page_config(page_title="KWG Media Dashboard", layout="wide")
    st.title("KWG Media Performance Dashboard")
    st.caption("Management + Strategy view: KPIs, month-over-month trends, funnel leaks, channel waste detection, quality mix, and Early Warning Index.")

    with st.sidebar:
        st.header("1) Data source")
        source = st.radio(
            "Choose how to load data",
            ["Google Sheet (KW login)", "Upload Excel (.xlsx)"],
            index=0,
        )
        
        raw = None
        
        if source == "Google Sheet (KW login)":
            # Optional sign out
            if st.button("Sign out of Google", use_container_width=True):
                st.session_state.pop("google_creds_json", None)
                st.session_state.pop("google_oauth_state", None)
                _clear_query_params()
                st.rerun()
        
            creds = google_get_credentials()  # stops to render login if not authenticated
        
            defaults = dict(st.secrets.get("dashboard", {}))
            default_sheet_url = defaults.get("sheet_url", "")
            default_worksheet = defaults.get("worksheet", "Media Report")
        
            sheet_url = st.text_input(
                "Google Sheet URL (or Spreadsheet ID)",
                value=default_sheet_url,
                placeholder="https://docs.google.com/spreadsheets/d/....",
            )
            if not sheet_url:
                st.info("Paste the Google Sheet URL/ID above.")
                st.stop()
        
            # Fetch worksheet/tab names
            try:
                import gspread
                from google.oauth2.credentials import Credentials
                creds2 = Credentials.from_authorized_user_info(
                    json.loads(st.session_state["google_creds_json"]),
                    scopes=GOOGLE_SHEETS_SCOPES,
                )
                gc = gspread.authorize(creds2)
                sh = gc.open_by_url(sheet_url) if "docs.google.com" in sheet_url else gc.open_by_key(sheet_url)
                tab_names = [w.title for w in sh.worksheets()]
            except Exception as e:
                st.error(f"Could not read the spreadsheet tabs: {e}")
                st.stop()
        
            if not tab_names:
                st.error("No worksheets/tabs found in that spreadsheet.")
                st.stop()
        
            worksheet_name = st.selectbox(
                "Worksheet/tab",
                tab_names,
                index=tab_names.index(default_worksheet) if default_worksheet in tab_names else 0,
            )
        
            # Read sheet (cached per-session for 5 minutes to avoid hammering the API on every widget change)
            refresh = st.button("Refresh data now", use_container_width=True)
            cache_key = (sheet_url, worksheet_name)

            if refresh or st.session_state.get("raw_cache_key") != cache_key or (time.time() - st.session_state.get("raw_cache_ts", 0)) > 300:
                try:
                    raw = load_raw_google_sheet(sheet_url, worksheet_name, st.session_state["google_creds_json"])
                except Exception as e:
                    st.error(str(e))
                    st.stop()
                st.session_state["raw_cache_key"] = cache_key
                st.session_state["raw_cache_ts"] = time.time()
                st.session_state["raw_cache"] = raw
            else:
                raw = st.session_state.get("raw_cache")
        
        else:
            st.header("1) Upload")
            up = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])
            if up is None:
                st.info("Upload your master sheet export to begin.")
                st.stop()
        
            # sheet selector
            try:
                import openpyxl
                wb = openpyxl.load_workbook(up, read_only=True, data_only=True)
                sheet_names = wb.sheetnames
            except Exception:
                sheet_names = None
        
            default_sheet = None
            if sheet_names:
                for s in sheet_names:
                    if str(s).strip().lower() in ["media report", "media report "]:
                        default_sheet = s
                        break
                if default_sheet is None:
                    for s in sheet_names:
                        if "media report" in str(s).lower():
                            default_sheet = s
                            break
            if sheet_names:
                sheet = st.selectbox(
                    "Select sheet",
                    sheet_names,
                    index=sheet_names.index(default_sheet) if default_sheet in sheet_names else 0,
                )
            else:
                sheet = st.text_input("Sheet name", value="Media Report")
        
            raw = load_raw_excel(up, sheet)

        st.header("2) Parse")
        try:
            data = parse_media_table(raw)
        except Exception as e:
            st.error(f"Parsing failed on this sheet: {e}")
            st.info("Try selecting a different sheet (often the correct one is named 'Media Report').")
            st.stop()

        projects = sorted(data["project"].dropna().unique().tolist())
        proj_choice = st.selectbox("Project", projects)

        proj_df0 = data[data["project"] == proj_choice].copy()

        # Exclude sources
        sources_all = sorted(proj_df0["source"].unique().tolist())
        default_exclude = []
        for s in sources_all:
            sl = s.lower()
            if any(re.search(p, sl) for p in DEFAULT_EXCLUDE_PATTERNS):
                default_exclude.append(s)
        exclude_sources = st.multiselect("Exclude sources (not counted in Media/Marketing performance)", sources_all, default=default_exclude)

        proj_df = apply_exclusions(proj_df0, exclude_sources)

        st.header("3) Time Window")
        months_all = sorted(proj_df["month"].unique().tolist(), key=_month_sort_key)
        if not months_all:
            st.warning("No month data found after exclusions. Remove some exclusions.")
            st.stop()

        mode = st.radio("Month selection mode", ["Last N months", "Custom range"], horizontal=True)
        if mode == "Last N months":
            max_n = min(36, len(months_all))
            n = st.slider("How many months of data?", min_value=1, max_value=max_n, value=min(6, max_n))
            sel_months = months_all[-n:]
        else:
            start = st.selectbox("From (month)", months_all, index=max(0, len(months_all)-6))
            end = st.selectbox("To (month)", months_all, index=len(months_all)-1)
            if _month_sort_key(start) > _month_sort_key(end):
                start, end = end, start
            sel_months = [m for m in months_all if _month_sort_key(start) <= _month_sort_key(m) <= _month_sort_key(end)]

        st.header("4) View & Definitions")
        view_mode = st.radio("View Mode", ["Simple", "Advanced"], horizontal=True)

        has_current_lead_visits = proj_df["site_visits_current_leads"].notna().any()
        visit_def = st.radio(
            "Site Visit definition",
            ["Reported Site Visits", "Current-month Leads → Site Visits"] if has_current_lead_visits else ["Reported Site Visits"],
            horizontal=False
        )
        site_visit_col = "site_visits_current_leads" if visit_def == "Current-month Leads → Site Visits" else "site_visits_reported"
        st.caption("If 'Current-month Leads → Site Visits' is not present in the sheet, it won’t appear here.")

        st.divider()
        st.header("5) Targets & Early Warning Index")
        use_auto_targets = st.checkbox("Auto-target = trailing 3-month average (recommended)", value=True)

        filtered = proj_df[proj_df["month"].isin(sel_months)].copy()
        totals = compute_monthly_totals(filtered, site_visit_col=site_visit_col)
        kpi = compute_kpis(totals)

        # Targets
        if use_auto_targets:
            targets = {
                "leads": trailing_avg(totals["leads"]),
                "qualified": trailing_avg(totals["qualified"]),
                "site_visits": trailing_avg(totals["site_visits"]),
                "ff_meetings": trailing_avg(totals["ff_meetings"]),
                "sales": trailing_avg(totals["sales"]),
                "spend": trailing_avg(totals["spend"]),
            }
        else:
            st.caption("Enter monthly targets.")
            targets = {
                "leads": st.number_input("Target: Leads", min_value=0.0, value=float(trailing_avg(totals["leads"])) if not math.isnan(trailing_avg(totals["leads"])) else 0.0, step=100.0),
                "qualified": st.number_input("Target: Qualified", min_value=0.0, value=float(trailing_avg(totals["qualified"])) if not math.isnan(trailing_avg(totals["qualified"])) else 0.0, step=10.0),
                "site_visits": st.number_input("Target: Site Visits", min_value=0.0, value=float(trailing_avg(totals["site_visits"])) if not math.isnan(trailing_avg(totals["site_visits"])) else 0.0, step=10.0),
                "ff_meetings": st.number_input("Target: FF Meetings", min_value=0.0, value=float(trailing_avg(totals["ff_meetings"])) if not math.isnan(trailing_avg(totals["ff_meetings"])) else 0.0, step=1.0),
                "sales": st.number_input("Target: Sales", min_value=0.0, value=float(trailing_avg(totals["sales"])) if not math.isnan(trailing_avg(totals["sales"])) else 0.0, step=1.0),
                "spend": st.number_input("Target: Spend (₹)", min_value=0.0, value=float(trailing_avg(totals["spend"])) if not math.isnan(trailing_avg(totals["spend"])) else 0.0, step=10000.0),
            }

        ewi_threshold = st.slider("EWI sensitivity (penalty saturates at this MoM change)", 0.10, 0.60, 0.30, 0.05)
        with st.expander("EWI weights"):
            w1 = st.slider("Weight: CPSale increase", 0.0, 1.0, DEFAULT_EWI_WEIGHTS["cpsale_up"], 0.05)
            w2 = st.slider("Weight: Close rate fall", 0.0, 1.0, DEFAULT_EWI_WEIGHTS["close_down"], 0.05)
            w3 = st.slider("Weight: Spend↑ without Sales↑", 0.0, 1.0, DEFAULT_EWI_WEIGHTS["spend_vs_sales"], 0.05)
            w4 = st.slider("Weight: Visit rate fall", 0.0, 1.0, DEFAULT_EWI_WEIGHTS["visit_down"], 0.05)
            weights = {"cpsale_up": w1, "close_down": w2, "spend_vs_sales": w3, "visit_down": w4}

    # ---------- Main dashboard ----------
    last_month = totals.index[-1]
    prev_month = totals.index[-2] if len(totals.index) >= 2 else None

    st.subheader(f"Project: {proj_choice}  •  Months: {sel_months[0]} → {sel_months[-1]}  •  Visits: {visit_def}")
    if exclude_sources:
        st.caption(f"Excluded sources: {', '.join(exclude_sources[:8])}" + ("..." if len(exclude_sources) > 8 else ""))

    ewi_comp = compute_ewi_components(kpi, threshold=ewi_threshold, weights=weights)
    ewi = ewi_comp["ewi"]

    st.markdown("### Executive Summary")
    cur = totals.loc[last_month]
    prev = totals.loc[prev_month] if prev_month is not None else None

    def mom_delta(metric):
        if prev is None:
            return np.nan
        return cur[metric] - prev[metric]

    cols = st.columns(6)
    tile_defs = [
        ("Total Leads", "leads", True),
        ("Qualified / Interested", "qualified", True),
        ("Site Visits", "site_visits", True),
        ("FF Meetings", "ff_meetings", True),
        ("Sales (count)", "sales", True),
        ("Spend (₹)", "spend", False),
    ]
    for i, (title, key, higher_is_better) in enumerate(tile_defs):
        val = float(cur[key])
        delta = mom_delta(key)
        target = targets.get(key, np.nan)

        rag_name, rag_hex = rag_color(val, target, higher_is_better=(higher_is_better if key != "spend" else False))
        v_str = fmt_num(val) if key != "spend" else f"₹ {val:,.0f}"
        d_str = "—" if prev is None or np.isnan(delta) else (f"+{fmt_num(delta)}" if delta >= 0 else f"{fmt_num(delta)}")
        t_str = "—" if target is None or (isinstance(target, float) and np.isnan(target)) else (fmt_num(target) if key != "spend" else f"₹ {target:,.0f}")
        with cols[i]:
            st.markdown(tile_html(title, v_str, d_str, t_str, rag_name, rag_hex), unsafe_allow_html=True)

    st.markdown("#### Early Warning Index (EWI)")
    colA, colB = st.columns([1, 2])
    ewi_cur = ewi.loc[last_month]
    ewi_prev = ewi.loc[prev_month] if prev_month is not None else np.nan
    with colA:
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=float(ewi_cur) if not pd.isna(ewi_cur) else 0.0,
            delta={"reference": float(ewi_prev) if not pd.isna(ewi_prev) else 0.0},
            gauge={"axis": {"range": [0, 100]}},
            title={"text": "EWI (0–100)"},
        ))
        fig.update_layout(height=220, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with colB:
        ewi_df = pd.DataFrame({"month": ewi.index, "EWI": ewi.values}).dropna()
        if not ewi_df.empty:
            fig2 = px.line(ewi_df, x="month", y="EWI", markers=True)
            fig2.update_layout(height=220, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig2, use_container_width=True)
        st.caption("Lower EWI indicates rising risk: CPSale↑, Close rate↓, Spend↑ without Sales↑, Visit rate↓.")

    st.markdown("### Health Vitals (Ratios & Efficiency)")
    vcols = st.columns(6)
    vitals = [
        ("Qualified rate (Qualified/Leads)", float(kpi.loc[last_month, "qualified_rate"]), "pct"),
        ("Visit rate (Visits/Leads)", float(kpi.loc[last_month, "visit_rate_leads"]), "pct"),
        ("Meeting rate (FF/Visits)", float(kpi.loc[last_month, "meeting_rate"]), "pct"),
        ("Close rate (Sales/Visits)", float(kpi.loc[last_month, "close_rate_visits"]), "pct"),
        ("CPQL (₹/Qualified)", float(kpi.loc[last_month, "cpql"]), "money"),
        ("CPSale (₹/Sale)", float(kpi.loc[last_month, "cpsale"]), "money"),
    ]
    for i,(name,val,kind) in enumerate(vitals):
        with vcols[i]:
            if kind == "pct":
                st.metric(name, fmt_pct(val))
            else:
                st.metric(name, "—" if np.isnan(val) else f"₹ {val:,.0f}")

    if view_mode == "Simple":
        st.markdown("### Trends (selected months)")
        t1, t2 = st.columns(2)
        with t1:
            plot_df = totals.reset_index()
            fig = px.line(plot_df, x="month", y=["qualified","site_visits","sales"], markers=True)
            fig.update_layout(height=320, legend_title_text="Counts")
            st.plotly_chart(fig, use_container_width=True)
        with t2:
            plot_df2 = kpi.reset_index()
            fig = px.line(plot_df2, x="month", y=["spend","cpql","cpsale"], markers=True)
            fig.update_layout(height=320, legend_title_text="Cost efficiency")
            st.plotly_chart(fig, use_container_width=True)
        st.info("Switch to **Advanced** for Qualified deep-dive, funnel trends over time, channel sparklines, and decomposition graphs.")
        return

    st.markdown("## Deep Dives (Advanced)")
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Qualified Deep Dive",
        "Funnel Over Time",
        "Channel Trends",
        "Channel Efficiency",
        "Quality Mix",
        "Data & Export"
    ])

    with tab1:
        st.subheader("Qualified Leads: month-over-month")

        qdf = totals.reset_index()[["month","qualified","leads","spend"]].copy()
        qdf["qualified_roll3"] = qdf["qualified"].rolling(3).mean()
        target_q = targets.get("qualified", np.nan)
        qdf["target"] = target_q

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=qdf["month"], y=qdf["qualified"], mode="lines+markers", name="Qualified"))
        fig.add_trace(go.Scatter(x=qdf["month"], y=qdf["qualified_roll3"], mode="lines", name="3-mo avg"))
        if not (isinstance(target_q, float) and np.isnan(target_q)):
            fig.add_trace(go.Scatter(x=qdf["month"], y=qdf["target"], mode="lines", name="Target", line=dict(dash="dash")))
        fig.update_layout(height=360, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Qualified rate (quality of targeting)")
        r = kpi.reset_index()[["month","qualified_rate"]].copy()
        fig2 = px.line(r, x="month", y=["qualified_rate"], markers=True)
        fig2.update_layout(height=280, yaxis_tickformat=".0%")
        st.plotly_chart(fig2, use_container_width=True)

        st.markdown("#### Why did Qualified change? (Leads vs Qualified rate)")
        if prev_month is None:
            st.info("Need at least 2 months selected for decomposition.")
        else:
            L0 = float(totals.loc[prev_month, "leads"])
            Q0 = float(totals.loc[prev_month, "qualified"])
            L1 = float(totals.loc[last_month, "leads"])
            Q1 = float(totals.loc[last_month, "qualified"])
            r0 = (Q0 / L0) if L0 else 0.0
            r1 = (Q1 / L1) if L1 else 0.0

            contrib_leads = (L1 - L0) * r0
            contrib_rate = L1 * (r1 - r0)
            base = Q0

            figw = go.Figure(go.Waterfall(
                name="Qualified Δ",
                orientation="v",
                measure=["absolute","relative","relative","total"],
                x=[f"Prev ({prev_month})", "Leads volume effect", "Qualified-rate effect", f"Current ({last_month})"],
                y=[base, contrib_leads, contrib_rate, base + contrib_leads + contrib_rate],
                connector={"line":{"dash":"dot"}}
            ))
            figw.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(figw, use_container_width=True)

        st.markdown("#### Qualified mix by source over time (Top 8 by spend + Others)")
        ts = compute_source_timeseries(filtered, site_visit_col)
        top_sources = top_sources_by_spend(filtered, k=8)
        ts2 = ts.copy()
        ts2["source_group"] = np.where(ts2["source"].isin(top_sources), ts2["source"], "Others")
        mix = ts2.groupby(["month","source_group"])["qualified"].sum().reset_index()
        figm = px.area(mix, x="month", y="qualified", color="source_group")
        figm.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(figm, use_container_width=True)

        st.markdown("#### Spend vs Qualified (are we getting more Qualified for the same spend?)")
        s = totals.reset_index()[["month","spend","qualified"]].copy()
        fig_s = px.scatter(s, x="spend", y="qualified", hover_name="month", size="qualified")
        fig_s.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig_s, use_container_width=True)

    with tab2:
        st.subheader("Funnel conversion trends (month-over-month)")
        f = kpi.reset_index()[["month","qualified_rate","visit_rate_qualified","meeting_rate","close_rate_visits"]].copy()
        f = f.rename(columns={
            "qualified_rate": "Qualified/Leads",
            "visit_rate_qualified": "Visits/Qualified",
            "meeting_rate": "FF/Visits",
            "close_rate_visits": "Sales/Visits"
        })
        fig = px.line(f, x="month", y=["Qualified/Leads","Visits/Qualified","FF/Visits","Sales/Visits"], markers=True)
        fig.update_layout(height=420, yaxis_tickformat=".0%")
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### EWI breakdown (why did risk go up or down?)")
        if prev_month is None:
            st.info("Need at least 2 months for breakdown.")
        else:
            comp = ewi_comp.loc[last_month, ["p_cpsale_up","p_close_down","p_spend_vs_sales","p_visit_down"]].fillna(0.0)
            bdf = pd.DataFrame({
                "component": ["CPSale ↑", "Close rate ↓", "Spend ↑ without Sales ↑", "Visit rate ↓"],
                "penalty (0..1)": comp.values
            })
            figb = px.bar(bdf, x="component", y="penalty (0..1)")
            figb.update_layout(height=320, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(figb, use_container_width=True)

    with tab3:
        st.subheader("Channel trends over time (sparklines)")
        ts = compute_source_timeseries(filtered, site_visit_col)
        months = sorted(ts["month"].unique().tolist(), key=_month_sort_key)

        sources = sorted(ts["source"].unique().tolist())
        top_n = st.slider("Number of sources to show (by spend)", 5, min(30, len(sources)), min(12, len(sources)))
        top_sources = top_sources_by_spend(filtered, k=top_n)

        ts_top = ts[ts["source"].isin(top_sources)].copy()

        def list_per_source(metric):
            piv = ts_top.pivot_table(index="source", columns="month", values=metric, aggfunc="sum").reindex(columns=months).fillna(0.0)
            return piv.apply(lambda row: row.values.tolist(), axis=1)

        leads_list = list_per_source("leads")
        qual_list = list_per_source("qualified")
        spend_list = list_per_source("spend")
        cpql_piv = ts_top.pivot_table(index="source", columns="month", values="cpql", aggfunc="mean").reindex(columns=months)
        cpql_list = cpql_piv.apply(lambda row: [float(x) if not pd.isna(x) else 0.0 for x in row.values.tolist()], axis=1)

        df_month = filtered[filtered["month"] == last_month].copy()
        chan_cur = build_channel_table(df_month, site_visit_col).set_index("source")

        table = pd.DataFrame(index=top_sources)
        table["Spend (₹)"] = [float(chan_cur.loc[s, "spend"]) if s in chan_cur.index else 0.0 for s in top_sources]
        table["Sales"] = [float(chan_cur.loc[s, "sales"]) if s in chan_cur.index else 0.0 for s in top_sources]
        table["Waste Index"] = [float(chan_cur.loc[s, "waste_index"]) if s in chan_cur.index else 0.0 for s in top_sources]
        table["Leads (trend)"] = leads_list.reindex(top_sources)
        table["Qualified (trend)"] = qual_list.reindex(top_sources)
        table["Spend (trend)"] = spend_list.reindex(top_sources)
        table["CPQL (trend)"] = cpql_list.reindex(top_sources)
        table = table.sort_values("Waste Index", ascending=False)

        st.dataframe(
            table.reset_index().rename(columns={"index":"Source"}),
            use_container_width=True,
            column_config={
                "Leads (trend)": st.column_config.LineChartColumn("Leads (trend)"),
                "Qualified (trend)": st.column_config.LineChartColumn("Qualified (trend)"),
                "Spend (trend)": st.column_config.LineChartColumn("Spend (trend)"),
                "CPQL (trend)": st.column_config.LineChartColumn("CPQL (trend)"),
                "Spend (₹)": st.column_config.NumberColumn(format="₹ %d"),
                "Waste Index": st.column_config.NumberColumn(format="%.1f%%"),
            }
        )

    with tab4:
        st.subheader("Channel efficiency (current month)")
        df_month = filtered[filtered["month"] == last_month].copy()
        chan = build_channel_table(df_month, site_visit_col)
        disp = chan.copy()
        for c in ["spend","cpl","cpql","cpsv","cpsale"]:
            disp[c] = disp[c].apply(lambda x: "—" if (x is None or (isinstance(x,float) and np.isnan(x))) else f"₹ {x:,.0f}")
        for c in ["spend_share","sales_share","waste_index"]:
            disp[c] = disp[c].apply(lambda x: "—" if (x is None or (isinstance(x,float) and np.isnan(x))) else f"{x*100:.1f}%")
        st.dataframe(disp[["source","spend","leads","qualified","site_visits","sales","cpl","cpql","cpsale","spend_share","sales_share","waste_index"]], use_container_width=True)

        st.markdown("#### Spend share vs Sales share (waste detector)")
        fig = px.scatter(
            chan,
            x="spend_share",
            y="sales_share",
            size="spend",
            hover_name="source",
            labels={"spend_share":"Spend share", "sales_share":"Sales share"},
        )
        lim = float(max(chan["spend_share"].max(), chan["sales_share"].max(), 0.01))
        fig.add_shape(type="line", x0=0, y0=0, x1=lim, y1=lim)
        fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with tab5:
        st.subheader("Quality mix (by source, current month)")
        df_month = filtered[filtered["month"] == last_month].copy()
        q = df_month.groupby("source")[["leads","qualified",site_visit_col,"sales","spend"]].sum().rename(columns={site_visit_col:"site_visits"})
        q["qualified_rate"] = _safe_div(q["qualified"], q["leads"])
        q["visit_rate"] = _safe_div(q["site_visits"], q["qualified"])
        q["sales_per_visit"] = _safe_div(q["sales"], q["site_visits"])
        q = q.reset_index().sort_values("spend", ascending=False)

        heat = q.set_index("source")[["qualified_rate","visit_rate","sales_per_visit"]].copy()
        heat = heat.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        fig = go.Figure(data=go.Heatmap(
            z=heat.values,
            x=["% Qualified/Leads", "% Visits/Qualified", "% Sales/Visits"],
            y=heat.index.tolist(),
            hovertemplate="Source: %{y}<br>%{x}: %{z:.2f}<extra></extra>"
        ))
        fig.update_layout(height=520, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with tab6:
        st.subheader("Data quality checks")
        dq = []
        if (kpi["qualified_rate"] > 1.05).any():
            dq.append("Qualified rate > 100% in some months (Qualified > Leads). Verify definitions.")
        if (kpi["visit_rate_leads"] > 1.05).any():
            dq.append("Visit rate (Visits/Leads) > 100% in some months. Verify mapping.")
        if (kpi["close_rate_visits"] > 1.05).any():
            dq.append("Close rate (Sales/Visits) > 100% in some months. Verify attribution.")
        if (totals["spend"] == 0).all():
            dq.append("Spend is 0 for all selected months. Check spend column in the sheet.")
        if dq:
            st.warning(" • " + "\n • ".join(dq))
        else:
            st.success("No obvious data-quality red flags detected for the selected range.")

        st.markdown("### Export")
        st.download_button(
            "Download filtered normalized data (CSV)",
            filtered.to_csv(index=False).encode("utf-8"),
            file_name=f"{proj_choice}_media_normalized_{sel_months[0]}_{sel_months[-1]}.csv",
            mime="text/csv",
        )
        snapshot = totals.copy()
        snapshot = snapshot.join(kpi.drop(columns=["leads","qualified","site_visits","ff_meetings","sales","spend"]), how="left", rsuffix="_kpi")
        snapshot["early_warning_index"] = ewi
        st.download_button(
            "Download monthly KPI snapshot (CSV)",
            snapshot.reset_index().to_csv(index=False).encode("utf-8"),
            file_name=f"{proj_choice}_media_kpis_{sel_months[0]}_{sel_months[-1]}.csv",
            mime="text/csv",
        )

if __name__ == "__main__":
    main()