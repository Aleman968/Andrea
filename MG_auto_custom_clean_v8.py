
import streamlit as st
import pandas as pd
import datetime as dt
import requests
import math

st.set_page_config(page_title="MG Auto Dati (football-data.org)", layout="wide")
st.title("MG Auto Dati – stagione corrente (football-data.org)")

st.markdown(
    """
Questa app prende i dati da sola da football-data.org (stagione corrente) e calcola:

- Produzione gol (G0..G4+) della squadra scelta su tutta la stagione (n match usati indicato)
- Gol subiti casa/trasferta (G0..G4+) dell'avversaria nelle ultime 10 partite coerenti
  (solo casa se gioca in casa, solo trasferta se gioca fuori).
  Se sono meno di 10 partite, usa quelle disponibili e indica quante sono.

Serve un token API in Secrets: FOOTBALL_DATA_TOKEN="...".
"""
)

TOKEN = (
    st.secrets.get("FOOTBALL_DATA_TOKEN", "").strip()
    or st.secrets.get("FOOTBALL_DATA_API_KEY", "").strip()
    or st.secrets.get("FOOTBALL_DATA_KEY", "").strip()
    or ""
)
if not TOKEN:
    st.error("Manca la chiave API di football-data.org. Impostala in Streamlit Cloud → Manage app → Settings → Secrets come FOOTBALL_DATA_TOKEN (oppure FOOTBALL_DATA_API_KEY).")
    st.stop()

BASE = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": TOKEN}

COMPETITIONS = {
    "Serie A (SA)": "SA",
    "Serie B (SB)": "SB",
    "Premier League (PL)": "PL",
    "LaLiga (PD)": "PD",
    "Bundesliga (BL1)": "BL1",
    "Ligue 1 (FL1)": "FL1",
    "Eredivisie (DED)": "DED",
    "Primeira Liga – Portogallo (PPL)": "PPL",
}


@st.cache_data(show_spinner=False, ttl=60*30)
def api_get(path: str, params: dict | None = None) -> dict | None:
    """GET verso football-data.org con cache e gestione errori.

    - Ritorna dict se OK
    - Ritorna None se errore (mostra messaggio in UI)
    - Retry/backoff solo su 429 (rate limit)
    """
    import time as _time

    url = f"{BASE}{path}"
    max_tries = 5

    for i in range(max_tries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        except Exception as e:
            st.error(f"Errore di rete durante la chiamata API: {e}")
            return None

        if r.status_code == 200:
            try:
                return r.json()
            except Exception as e:
                st.error(f"Risposta API non valida (JSON): {e}")
                return None

        # Rate limit: retry con backoff
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            try:
                wait = int(retry_after) if retry_after else 10 * (i + 1)
            except Exception:
                wait = 10 * (i + 1)
            wait = min(max(wait, 8), 60)
            if i == 0:
                st.warning("Rate limit API (429): rallento automaticamente e riprovo…")
            _time.sleep(wait)
            continue

        # Altri errori: non crashare l'app, mostra e ritorna None
        if r.status_code == 403:
            st.error("Errore API 403 (accesso negato). Controlla la chiave in Streamlit Cloud → Secrets (FOOTBALL_DATA_TOKEN o FOOTBALL_DATA_API_KEY) e i permessi del piano su football-data.org.")
        elif r.status_code == 401:
            st.error("Errore API 401 (non autorizzato). La chiave API sembra mancante o non valida.")
        else:
            st.error(f"Errore API {r.status_code}: {r.text[:300]}")
        return None

    st.error("Errore API persistente (rate limit o servizio non disponibile). Riprova tra 1–2 minuti.")
    return None

@st.cache_data(show_spinner=False, ttl=60*15)
def get_competition_matches(comp_code: str, date_from: str, date_to: str) -> pd.DataFrame:
    params = {"dateFrom": date_from, "dateTo": date_to}
    data = api_get(f"/competitions/{comp_code}/matches", params=params)
    if data is None:
        return pd.DataFrame()
    matches = data.get("matches", [])
    rows = []
    for m in matches:
        home = m.get("homeTeam", {})
        away = m.get("awayTeam", {})
        score = m.get("score", {})
        full = score.get("fullTime", {}) if isinstance(score, dict) else {}
        rows.append({
            "match_id": m.get("id"),
            "utcDate": m.get("utcDate"),
            "status": m.get("status"),
            "home_id": home.get("id"),
            "home_name": home.get("name"),
            "away_id": away.get("id"),
            "away_name": away.get("name"),
            "home_ft": full.get("home"),
            "away_ft": full.get("away"),
            "matchday": m.get("matchday"),
        })
    df = pd.DataFrame(rows)
    if not df.empty and "utcDate" in df.columns:
        df["utcDate"] = pd.to_datetime(df["utcDate"], errors="coerce", utc=True)
    return df

@st.cache_data(show_spinner=False, ttl=60*60)
def get_team_season_matches(team_id: int, comp_code: str) -> pd.DataFrame:
    data = api_get(f"/teams/{team_id}/matches", params={"competitions": comp_code})
    matches = data.get("matches", [])
    rows = []
    for m in matches:
        home = m.get("homeTeam", {})
        away = m.get("awayTeam", {})
        score = m.get("score", {})
        full = score.get("fullTime", {}) if isinstance(score, dict) else {}
        rows.append({
            "match_id": m.get("id"),
            "utcDate": m.get("utcDate"),
            "status": m.get("status"),
            "home_id": home.get("id"),
            "away_id": away.get("id"),
            "home_name": home.get("name"),
            "away_name": away.get("name"),
            "home_ft": full.get("home"),
            "away_ft": full.get("away"),
        })
    df = pd.DataFrame(rows)
    if not df.empty and "utcDate" in df.columns:
        df["utcDate"] = pd.to_datetime(df["utcDate"], errors="coerce", utc=True)
    return df

def goals_for_in_match(row: pd.Series, team_id: int):
    if row.get("status") != "FINISHED":
        return None
    if row.get("home_id") == team_id:
        return row.get("home_ft")
    if row.get("away_id") == team_id:
        return row.get("away_ft")
    return None

def goals_conceded_in_match(row: pd.Series, team_id: int):
    if row.get("status") != "FINISHED":
        return None
    if row.get("home_id") == team_id:
        return row.get("away_ft")
    if row.get("away_id") == team_id:
        return row.get("home_ft")
    return None

def bucket_0_4p(x: int) -> str:
    return f"G{x}" if x <= 3 else "G4+"

def dist_table(counts: pd.Series, total: int) -> pd.DataFrame:
    order = ["G0","G1","G2","G3","G4+"]
    out = []
    for k in order:
        c = int(counts.get(k, 0))
        p = (c / total) if total > 0 else 0.0
        out.append({"Bucket": k, "Count": c, "Percent": p})
    return pd.DataFrame(out)

def dist_compare_context(total_df: pd.DataFrame, ctx_df: pd.DataFrame, ctx_label: str) -> pd.DataFrame:
    """Tabella comparativa: % gol fatti Totale stagione vs contesto (Casa/Trasferta)."""
    order = ["G0","G1","G2","G3","G4+"]
    tot_counts = total_df["bucket_gf"].value_counts() if total_df is not None and len(total_df) else pd.Series(dtype=int)
    tot_n = int(len(total_df)) if total_df is not None else 0

    ctx_counts = ctx_df["bucket_gf"].value_counts() if ctx_df is not None and len(ctx_df) else pd.Series(dtype=int)
    ctx_n = int(len(ctx_df)) if ctx_df is not None else 0

    rows = []
    for k in order:
        rows.append({
            "Gol": k,
            "Totale": (float(tot_counts.get(k, 0)) / tot_n) if tot_n else 0.0,
            ctx_label: (float(ctx_counts.get(k, 0)) / ctx_n) if ctx_n else None,
        })
    return pd.DataFrame(rows)


col1, col2 = st.columns([1, 2])

with col1:
    comp_label = st.selectbox("Campionato", list(COMPETITIONS.keys()))
    comp = COMPETITIONS[comp_label]
    days_ahead = st.slider("Finestra partite (giorni avanti)", 1, 14, 7)
    today = dt.date.today()
    date_from = today.isoformat()
    date_to = (today + dt.timedelta(days=int(days_ahead))).isoformat()

with st.spinner("Carico partite..."):
    matches_df = get_competition_matches(comp, date_from, date_to)

if matches_df.empty:
    st.warning("Nessuna partita trovata nella finestra scelta. Prova ad aumentare i giorni.")
    st.stop()

upcoming = matches_df[matches_df["status"].isin(["SCHEDULED", "TIMED", "IN_PLAY", "PAUSED"])].copy()
if upcoming.empty:
    upcoming = matches_df.copy()

def match_label(r):
    dt_str = ""
    if pd.notna(r.get("utcDate")):
        try:
            dt_str = r["utcDate"].tz_convert("Europe/Rome").strftime("%d/%m %H:%M")
        except Exception:
            dt_str = str(r.get("utcDate"))
    md = r.get("matchday")
    md_str = f"MD {md} - " if pd.notna(md) else ""
    return f"{md_str}{dt_str} | {r['home_name']} vs {r['away_name']}"

upcoming = upcoming.sort_values("utcDate") if "utcDate" in upcoming.columns else upcoming
labels = [match_label(r) for _, r in upcoming.iterrows()]

with col2:
    st.subheader("Seleziona partita")
    idx = st.selectbox("Partita", list(range(len(labels))), format_func=lambda i: labels[i])

sel = upcoming.iloc[int(idx)]
home_id = int(sel["home_id"])
away_id = int(sel["away_id"])
home_name = sel["home_name"]
away_name = sel["away_name"]


# Toggle per mostrare dettagli dei calcoli Poisson (medie e λ)
show_poisson_debug = st.checkbox("Mostra dettagli Poisson (medie & λ usati nel calcolo)", value=False)

st.divider()
st.subheader("Calcolo distribuzioni")

N_LAST = 10

with st.spinner("Scarico match stagione e calcolo..."):
    home_season = get_team_season_matches(home_id, comp)
    away_season = get_team_season_matches(away_id, comp)

    hs = home_season[home_season["status"] == "FINISHED"].copy()
    aw = away_season[away_season["status"] == "FINISHED"].copy()

    hs["gf"] = hs.apply(lambda r: goals_for_in_match(r, home_id), axis=1)
    aw["gf"] = aw.apply(lambda r: goals_for_in_match(r, away_id), axis=1)

    # Gol SUBITI (serve per Poisson / BTTS / Under match)
    hs["ga"] = hs.apply(lambda r: goals_conceded_in_match(r, home_id), axis=1)
    aw["ga"] = aw.apply(lambda r: goals_conceded_in_match(r, away_id), axis=1)

    hs = hs.dropna(subset=["gf"])
    aw = aw.dropna(subset=["gf"])

    hs["bucket_gf"] = hs["gf"].astype(int).apply(bucket_0_4p)
    aw["bucket_gf"] = aw["gf"].astype(int).apply(bucket_0_4p)

    
    # --- SPLIT CASA/TRASFERTA: gol FATTI (stagione) ---
    hs_home_gf = hs[hs["home_id"] == home_id].copy()
    hs_away_gf = hs[hs["away_id"] == home_id].copy()
    aw_home_gf = aw[aw["home_id"] == away_id].copy()
    aw_away_gf = aw[aw["away_id"] == away_id].copy()

    # --- SPLIT CASA/TRASFERTA: gol SUBITI (stagione) ---
    hs_home_ga = hs[hs["home_id"] == home_id].copy()
    hs_away_ga = hs[hs["away_id"] == home_id].copy()
    aw_home_ga = aw[aw["home_id"] == away_id].copy()
    aw_away_ga = aw[aw["away_id"] == away_id].copy()

    for _df in [hs_home_gf, hs_away_gf, aw_home_gf, aw_away_gf]:
        if not _df.empty:
            _df["bucket_gf"] = _df["gf"].astype(int).apply(bucket_0_4p)

# --- Indicatori trend (ultime 6 vs stagione) sui gol FATTI ---
    def _trend_metrics(team_df: pd.DataFrame, team_label: str) -> dict:
        out = {
            "Squadra": team_label,
            "Match stagione (FINISHED)": int(len(team_df)),
            "Media gol stagione": float(team_df["gf"].mean()) if len(team_df) else 0.0,
            "Match usati ultime 6": int(min(6, len(team_df))),
            "Media gol ultime 6": 0.0,
            "Delta (ult6 - stag)": 0.0,
            "Evento estremo (ult6)": "",
            "Estremi (ult6)": 0,
            "Stato": "DATI INSUFFICIENTI",
        }
        if len(team_df) < 3:
            return out

        recent = team_df.sort_values("utcDate", ascending=False).head(6).copy()
        m6 = float(recent["gf"].mean()) if len(recent) else 0.0
        delta = m6 - float(out["Media gol stagione"])
        out["Media gol ultime 6"] = m6
        out["Delta (ult6 - stag)"] = delta

        if delta >= 0:
            out["Evento estremo (ult6)"] = "3+"
            out["Estremi (ult6)"] = int((recent["gf"] >= 3).sum())
        else:
            out["Evento estremo (ult6)"] = "0"
            out["Estremi (ult6)"] = int((recent["gf"] == 0).sum())

        if len(recent) < 6:
            out["Stato"] = "DATI INSUFFICIENTI"
            return out

        abs_delta = abs(delta)
        extremes = out["Estremi (ult6)"]
        if abs_delta >= 0.7 and extremes >= 3:
            out["Stato"] = "CAMBIO CONFERMATO"
        elif abs_delta >= 0.4:
            out["Stato"] = "WARNING"
        else:
            out["Stato"] = "NORMAL"
        return out

    home_tr = _trend_metrics(hs, home_name)
    away_tr = _trend_metrics(aw, away_name)

    # Gol subiti coerenti (ultime 10)
    home_home = hs[hs["home_id"] == home_id].copy()
    home_home["ga"] = home_home.apply(lambda r: goals_conceded_in_match(r, home_id), axis=1)
    home_home = home_home.dropna(subset=["ga"]).sort_values("utcDate", ascending=False).head(N_LAST)
    home_home["bucket_ga"] = home_home["ga"].astype(int).apply(bucket_0_4p)

    away_away = aw[aw["away_id"] == away_id].copy()
    away_away["ga"] = away_away.apply(lambda r: goals_conceded_in_match(r, away_id), axis=1)
    away_away = away_away.dropna(subset=["ga"]).sort_values("utcDate", ascending=False).head(N_LAST)
    away_away["bucket_ga"] = away_away["ga"].astype(int).apply(bucket_0_4p)


st.subheader("Indicatori trend (automatici) – gol fatti: ultime 6 vs stagione")

def _badge(s: str) -> str:
    if s == "CAMBIO CONFERMATO":
        return "🔴 CAMBIO CONFERMATO"
    if s == "WARNING":
        return "🟡 WARNING"
    if s == "NORMAL":
        return "🟢 NORMAL"
    return "⚪ DATI INSUFFICIENTI"

trend_df = pd.DataFrame([home_tr, away_tr])
trend_df["Stato"] = trend_df["Stato"].apply(_badge)

st.dataframe(
    trend_df[[
        "Squadra",
        "Match stagione (FINISHED)",
        "Media gol stagione",
        "Match usati ultime 6",
        "Media gol ultime 6",
        "Delta (ult6 - stag)",
        "Evento estremo (ult6)",
        "Estremi (ult6)",
        "Stato",
    ]].style.format({
        "Media gol stagione": "{:.2f}",
        "Media gol ultime 6": "{:.2f}",
        "Delta (ult6 - stag)": "{:+.2f}",
    }),
    use_container_width=True,
    hide_index=True
)
st.caption("Regole: 🔴 CAMBIO CONFERMATO se |Δ| ≥ 0.7 e l’evento estremo (3+ se Δ≥0, altrimenti 0) esce ≥ 3 volte nelle ultime 6. 🟡 WARNING se |Δ| ≥ 0.4.")
st.divider()

c1, c2 = st.columns(2)
with c1:
    st.markdown(f"### {home_name} – Gol fatti (Totale vs Casa)")
    df_h_cmp = dist_compare_context(hs, hs_home_gf, "Casa")
    st.dataframe(
        df_h_cmp.style.format({"Totale":"{:.1%}", "Casa":"{:.1%}"}),
        use_container_width=True,
        hide_index=True
    )
    st.caption(f"Match usati: Totale={len(hs)} | Casa={len(hs_home_gf)}")

with c2:
    st.markdown(f"### {away_name} – Gol subiti in trasferta (ultime {min(N_LAST, len(away_away))} partite)")
    st.dataframe(dist_table(away_away["bucket_ga"].value_counts(), len(away_away)).style.format({"Percent":"{:.1%}"}),
                 use_container_width=True, hide_index=True)

st.divider()

c3, c4 = st.columns(2)
with c3:
    st.markdown(f"### {away_name} – Gol fatti (Totale vs Trasferta)")
    df_a_cmp = dist_compare_context(aw, aw_away_gf, "Trasferta")
    st.dataframe(
        df_a_cmp.style.format({"Totale":"{:.1%}", "Trasferta":"{:.1%}"}),
        use_container_width=True,
        hide_index=True
    )
    st.caption(f"Match usati: Totale={len(aw)} | Trasferta={len(aw_away_gf)}")

with c4:
    st.markdown(f"### {home_name} – Gol subiti in casa (ultime {min(N_LAST, len(home_home))} partite)")
    st.dataframe(dist_table(home_home["bucket_ga"].value_counts(), len(home_home)).style.format({"Percent":"{:.1%}"}),
                 use_container_width=True, hide_index=True)



st.divider()

# ===========================
# CHECKLIST WIREFRAME (NO H2H)
# ===========================


st.divider()
st.subheader("Riepilogo numerico — medie GF/GA + Segna/Non segna (solo contesto casa/trasferta)")

def _safe_mean_series(s: pd.Series) -> float:
    try:
        s = pd.to_numeric(s, errors="coerce").dropna()
        return float(s.mean()) if len(s) else 0.0
    except Exception:
        return 0.0

def _safe_mean_df(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return _safe_mean_series(df[col])

def _count_scored(df: pd.DataFrame, col_gf: str = "gf"):
    if df is None or df.empty or col_gf not in df.columns:
        return 0, 0, 0
    s = pd.to_numeric(df[col_gf], errors="coerce").dropna()
    n = int(len(s))
    scored = int((s >= 1).sum())
    not_scored = int((s == 0).sum())
    return n, scored, not_scored

# Tabella riassuntiva: SOLO contesto coerente con la partita (casa/trasferta)
rows = []

# Home team: CASA (coerente col match)
n_hc, h_sc, h_ns = _count_scored(hs_home_gf, "gf")
rows.append({
    "Squadra": home_name,
    "Contesto": "CASA (coerente)",
    "Match": n_hc,
    "Media GF": _safe_mean_df(hs_home_gf, "gf"),
    "Media GA": _safe_mean_df(hs_home_ga, "ga"),
    "Segna (GF≥1)": h_sc,
    "Non segna (GF=0)": h_ns,
})

# Away team: TRASFERTA (coerente col match)
n_at, a_sc, a_ns = _count_scored(aw_away_gf, "gf")
rows.append({
    "Squadra": away_name,
    "Contesto": "TRASFERTA (coerente)",
    "Match": n_at,
    "Media GF": _safe_mean_df(aw_away_gf, "gf"),
    "Media GA": _safe_mean_df(aw_away_ga, "ga"),
    "Segna (GF≥1)": a_sc,
    "Non segna (GF=0)": a_ns,
})

summary_df = pd.DataFrame(rows)
st.dataframe(
    summary_df.style.format({
        "Media GF": "{:.2f}",
        "Media GA": "{:.2f}",
    }),
    use_container_width=True,
    hide_index=True
)

st.caption("Usa solo match casa per la squadra di casa e solo match fuori per la squadra in trasferta (coerente con la partita selezionata).")

st.divider()
st.subheader("Multigol (per squadra) — mostra tutti i range con p ≥ 70% (altrimenti NO BET)")

# --- Helpers: distribuzioni e Poisson ---
def _pct_dict_from_buckets(series_counts: pd.Series) -> dict:
    order = ["G0", "G1", "G2", "G3", "G4+"]
    total = int(series_counts.sum()) if series_counts is not None else 0
    out = {k: 0.0 for k in order}
    if total <= 0:
        return out
    for k in order:
        out[k] = float(series_counts.get(k, 0)) / total
    return out

def _pois_pmf(lam: float, k: int) -> float:
    lam = max(float(lam), 0.0)
    if k < 0:
        return 0.0
    p0 = math.exp(-lam)
    if k == 0:
        return p0
    p = p0
    for i in range(1, k + 1):
        p *= lam / i
    return p

def _pois_cdf(lam: float, k: int) -> float:
    if k < 0:
        return 0.0
    s = 0.0
    for i in range(0, k + 1):
        s += _pois_pmf(lam, i)
    return min(max(s, 0.0), 1.0)

def _pois_range_prob(lam: float, lo: int, hi: int, hi_is_4plus: bool = False) -> float:
    lam = max(float(lam), 0.0)
    lo = int(lo)
    hi = int(hi)
    if lo > hi:
        return 0.0
    if hi_is_4plus and hi == 4:
        # include tail (>=4)
        p_le_3 = _pois_cdf(lam, 3)
        p_lo_3 = _pois_cdf(lam, 3) - _pois_cdf(lam, lo - 1)
        return p_lo_3 + (1.0 - p_le_3)
    return _pois_cdf(lam, hi) - _pois_cdf(lam, lo - 1)

def range_includes(range_str: str, k: str) -> bool:
    lo, hi = range_str.split("–")
    lo_i = int(lo)
    hi_i = int(hi)
    v = 4 if k == "G4+" else int(k[1])
    return lo_i <= v <= hi_i

def mg_cover(range_str: str, distd: dict) -> float:
    return sum(float(distd.get(k, 0.0)) for k in ["G0", "G1", "G2", "G3", "G4+"] if range_includes(range_str, k))

def _ctx_or_total(split_df: pd.DataFrame, total_df: pd.DataFrame, min_matches: int = 6) -> pd.DataFrame:
    return split_df if (split_df is not None and len(split_df) >= min_matches) else total_df

# --- Calcolo λ (Poisson) su contesto casa/trasferta con fallback ---
MIN_LAMBDA_MATCHES = 4

# medie contestuali (se campione troppo piccolo, fallback su totale stagione)
gf_home = _safe_mean_df(hs_home_gf, "gf") if len(hs_home_gf) >= MIN_LAMBDA_MATCHES else _safe_mean_df(hs, "gf")
ga_home = _safe_mean_df(hs_home_ga, "ga") if len(hs_home_ga) >= MIN_LAMBDA_MATCHES else _safe_mean_df(hs, "ga")

gf_away = _safe_mean_df(aw_away_gf, "gf") if len(aw_away_gf) >= MIN_LAMBDA_MATCHES else _safe_mean_df(aw, "gf")
ga_away = _safe_mean_df(aw_away_ga, "ga") if len(aw_away_ga) >= MIN_LAMBDA_MATCHES else _safe_mean_df(aw, "ga")

lambda_home_raw = (gf_home + ga_away) / 2.0
lambda_away_raw = (gf_away + ga_home) / 2.0

# --- Shrinkage su λ (fisso) per ridurre sovrastime ---
LEAGUE_LAMBDA = 1.30  # media gol per squadra (prior)
K_SHRINK = 8          # forza prior
n_home = int(min(len(hs_home_gf), len(aw_away_ga))) if (len(hs_home_gf) and len(aw_away_ga)) else int(min(len(hs), len(aw)))
n_away = int(min(len(aw_away_gf), len(hs_home_ga))) if (len(aw_away_gf) and len(hs_home_ga)) else int(min(len(aw), len(hs)))
w_home = (n_home / (n_home + K_SHRINK)) if n_home > 0 else 0.0
w_away = (n_away / (n_away + K_SHRINK)) if n_away > 0 else 0.0
lambda_home = (w_home * lambda_home_raw) + ((1.0 - w_home) * LEAGUE_LAMBDA)
lambda_away = (w_away * lambda_away_raw) + ((1.0 - w_away) * LEAGUE_LAMBDA)

# Debug Poisson (se checkbox attivo)
if show_poisson_debug:
    with st.expander("Dettagli Poisson (medie e λ)", expanded=True):
        st.write({
            "GF_home (casa)": round(gf_home, 3),
            "GA_home (casa)": round(ga_home, 3),
            "GF_away (trasferta)": round(gf_away, 3),
            "GA_away (trasferta)": round(ga_away, 3),
            "lambda_home": round(lambda_home, 3),
            "lambda_away": round(lambda_away, 3),
        })


st.divider()
st.subheader("Probabilità che la squadra segni almeno 1 gol (Poisson)")

p_home_scores = 1.0 - math.exp(-lambda_home)
p_away_scores = 1.0 - math.exp(-lambda_away)

score_df = pd.DataFrame([
    {"Squadra": home_name, "Contesto": "CASA", "Prob segna ≥1": p_home_scores},
    {"Squadra": away_name, "Contesto": "TRASFERTA", "Prob segna ≥1": p_away_scores},
])

st.dataframe(
    score_df.style.format({"Prob segna ≥1": "{:.0%}"}),
    use_container_width=True,
    hide_index=True
)

# --- 3 risultati esatti più probabili (Dixon–Coles, λ shrinkati) ---
st.subheader("3 risultati esatti più probabili (più realistici)")

# Poisson indipendente tende a sovrastimare alcuni punteggi bassi (0-0 / 1-1).
# Applichiamo una correzione Dixon–Coles (tau) per i punteggi {0,1} che introduce
# una piccola correlazione tra le squadre. Parametro fisso e prudente.
DC_RHO = -0.10  # negativo = meno 0-0/1-1 rispetto al Poisson puro

def _pois_pmf(lam: float, k: int) -> float:
    lam = max(float(lam), 0.0)
    k = int(k)
    return math.exp(-lam) * (lam ** k) / math.factorial(k)

def _dc_tau(x: int, y: int, lam_h: float, lam_a: float, rho: float) -> float:
    # Dixon–Coles tau correction (solo per punteggi bassi)
    if x == 0 and y == 0:
        return 1.0 - rho * lam_h * lam_a
    if x == 0 and y == 1:
        return 1.0 + rho * lam_h
    if x == 1 and y == 0:
        return 1.0 + rho * lam_a
    if x == 1 and y == 1:
        return 1.0 - rho
    return 1.0

scores = []
total = 0.0
for h in range(0, 7):
    ph = _pois_pmf(lambda_home, h)
    for a in range(0, 7):
        pa = _pois_pmf(lambda_away, a)
        tau = _dc_tau(h, a, lambda_home, lambda_away, DC_RHO)
        # Evitiamo valori negativi in casi estremi
        tau = max(tau, 0.001)
        p = ph * pa * tau
        total += p
        scores.append({"Risultato": f"{h}-{a}", "Prob": p})

# normalizza (la correzione tau altera la somma totale)
if total > 0:
    for r in scores:
        r["Prob"] = r["Prob"] / total

top3 = sorted(scores, key=lambda x: x["Prob"], reverse=True)[:3]
top3_df = pd.DataFrame(top3)
st.dataframe(top3_df.style.format({"Prob": "{:.1%}"}), use_container_width=True, hide_index=True)
st.caption("Risultati esatti calcolati con Poisson + correzione Dixon–Coles (più realistica sui punteggi bassi), usando λ shrinkati (quindi già match-specific contro l’avversario).")



# --- Scelta MG per ciascuna squadra (range fissi) ---
# Regola: Score calibrato = 60% Poisson + 40% Storico
# Mostra MG solo se Score >= 70% e se il Trend NON è in WARNING/CAMBIO CONFERMATO

# --- Scelta MG per ciascuna squadra (range fissi) ---
# Regola: Score calibrato = 60% Poisson + 40% Storico
# Filtro HARD: Prob(segna >=1) (Poisson) deve essere >= 72%, altrimenti NO BET.
# Trend: 🔴 CAMBIO CONFERMATO -> NO BET; 🟡 WARNING -> soglia Score 72%; 🟢 NORMAL -> soglia Score 70%.

RANGES = ["0–1", "1–2", "1–3", "2–3", "2–4"]
MIN_SCORE_NORMAL = 0.70
MIN_SCORE_WARNING = 0.72
MIN_SCORE_NO_BET = None
MIN_SCORE_TEAM_SCORES = 0.72  # filtro fisso

def _range_lo_hi(r: str):
    lo, hi = r.split("–")
    return int(lo), int(hi)

def _trend_min_score(trend_row: dict) -> tuple[float | None, str]:
    """Ritorna (min_score, motivo_blocco). min_score=None significa NO BET."""
    stt = str(trend_row.get("Stato", "")).upper()
    if "CAMBIO CONFERMATO" in stt:
        return MIN_SCORE_NO_BET, "Trend 🔴 CAMBIO CONFERMATO (ultime 6 molto diverse dalla stagione)"
    if "WARNING" in stt:
        return MIN_SCORE_WARNING, "Trend 🟡 WARNING (soglia Score più severa)"
    return MIN_SCORE_NORMAL, ""

def list_mg_candidates(team_name: str, team_df_ctx: pd.DataFrame, team_df_total: pd.DataFrame, lam_team: float, min_score: float) -> tuple[pd.DataFrame, str]:
    """Ritorna (df_candidati, label_dati_usati). Usa contesto casa/trasferta se ha abbastanza match, altrimenti fallback sul totale."""
    df_used = _ctx_or_total(team_df_ctx, team_df_total, min_matches=6)
    dist = _pct_dict_from_buckets(df_used["bucket_gf"].value_counts()) if (df_used is not None and len(df_used)) else {k: 0.0 for k in ["G0","G1","G2","G3","G4+"]}
    used_label = "CONTESTO" if df_used is team_df_ctx else "TOTALE (fallback)"

    rows = []
    for r in RANGES:
        lo, hi = _range_lo_hi(r)

        # p_poisson: match-specific (λ già shrinkato)
        p_poisson = float(_pois_range_prob(lam_team, lo, hi, hi_is_4plus=True))

        # p_storico: copertura osservata nei match usati (contesto se possibile)
        p_storico = float(mg_cover(r, dist))

        # score calibrato
        score = 0.60 * p_poisson + 0.40 * p_storico

        if score < min_score:
            continue

        rows.append({
            "MG": f"MG {r} {team_name}",
            "Range": r,
            "Score (calibrato)": score,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["Score (calibrato)"], ascending=[False]).reset_index(drop=True)
    return df, used_label

# --- Filtri per squadra: Trend + Segna >=72% ---
home_min_score, home_trend_note = _trend_min_score(home_tr)
away_min_score, away_trend_note = _trend_min_score(away_tr)

home_block_reason = ""
away_block_reason = ""

# Filtro segna (HARD)
if p_home_scores < MIN_SCORE_TEAM_SCORES:
    home_min_score = MIN_SCORE_NO_BET
    home_block_reason = f"Prob segna (Poisson) {p_home_scores:.0%} < 72%"

if p_away_scores < MIN_SCORE_TEAM_SCORES:
    away_min_score = MIN_SCORE_NO_BET
    away_block_reason = f"Prob segna (Poisson) {p_away_scores:.0%} < 72%"

# Se trend rosso blocca, motivo trend prevale (ma mostriamo anche segna se presente)
if home_min_score is None and not home_block_reason:
    home_block_reason = home_trend_note or "Condizioni non soddisfatte"

if away_min_score is None and not away_block_reason:
    away_block_reason = away_trend_note or "Condizioni non soddisfatte"

home_df = pd.DataFrame()
away_df = pd.DataFrame()
home_used = "—"
away_used = "—"

if home_min_score is not None:
    home_df, home_used = list_mg_candidates(home_name, hs_home_gf, hs, lambda_home, home_min_score)

if away_min_score is not None:
    away_df, away_used = list_mg_candidates(away_name, aw_away_gf, aw, lambda_away, away_min_score)
st.divider()
st.subheader("Multigol consigliati (Score = 60% Poisson + 40% Storico)")

c1, c2 = st.columns(2)

with c1:
    st.markdown(f"### {home_name} (squadra Casa)")
    if home_min_score is None:
        st.error(f"NO BET — {home_block_reason}")
    elif home_df.empty:
        st.error("NO BET (nessun MG valido con le soglie impostate)")
    else:
        st.caption(f"Dati usati per lo storico gol fatti: {home_used}")
        st.dataframe(
            home_df.style.format({
                "Score (calibrato)": "{:.0%}",
            }),
            use_container_width=True,
            hide_index=True
        )

with c2:
    st.markdown(f"### {away_name} (squadra Trasferta)")
    if away_min_score is None:
        st.error(f"NO BET — {away_block_reason}")
    elif away_df.empty:
        st.error("NO BET (nessun MG valido con le soglie impostate)")
    else:
        st.caption(f"Dati usati per lo storico gol fatti: {away_used}")
        st.dataframe(
            away_df.style.format({
                "Score (calibrato)": "{:.0%}",
            }),
            use_container_width=True,
            hide_index=True
        )
