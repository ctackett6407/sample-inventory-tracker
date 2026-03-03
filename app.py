# app.py
# Sample Inventory + Content Tracker (CSV-backed) with:
# - Contacts system (contacts.csv) including WhatsApp
# - Controlled dropdowns for concentration/type and size (avoid human error)
# - Posting updates UI (mark posted + URLs) by UPC
# - Inventory update + safe delete tools
# - Catalog overrides (catalog_overrides.csv) to add/correct catalog entries without editing fra_cleaned.csv

import os
import re
import time
from datetime import datetime, date
from typing import Optional, Dict, List

import pandas as pd
import streamlit as st


# ----------------------------
# Configuration
# ----------------------------
DEFAULT_SAMPLES_CSV = "samples.csv"
DEFAULT_CATALOG_CSV = "data/fra_cleaned.csv"
DEFAULT_CATALOG_OVERRIDES_CSV = "catalog_overrides.csv"
DEFAULT_ACTIVITY_LOG_CSV = "activity_log.csv"
DEFAULT_CONTACTS_CSV = "contacts.csv"

STATUS_NEW = "NEW"
STATUS_FILMED = "FILMED"
STATUS_POSTED = "POSTED"
STATUS_COMPLETE = "COMPLETE"
ALL_STATUSES = [STATUS_NEW, STATUS_FILMED, STATUS_POSTED, STATUS_COMPLETE]

CONCENTRATION_OPTIONS = [
    "Unknown",
    "EDP",
    "EDT",
    "Extrait",
    "Parfum",
    "Cologne",
    "Oil",
    "Body Spray",
    "Aftershave",
    "Other",
]

SIZE_OPTIONS = [
    "Unknown",
    "0.7 ml",
    "1 ml",
    "1.5 ml",
    "2 ml",
    "3 ml",
    "5 ml",
    "8 ml",
    "10 ml",
    "15 ml",
    "20 ml",
    "30 ml",
    "50 ml",
    "75 ml",
    "100 ml",
    "125 ml",
    "150 ml",
    "200 ml",
    "Other",
]

CONTACT_TYPE_OPTIONS = [
    "Brand",
    "Brand Contact (Person)",
    "Warehouse",
    "Retailer",
    "Distributor",
    "PR Agency",
    "Creator",
    "Other",
]

SAMPLES_FIELDS = [
    "upc",
    "brand",
    "product_name",

    "concentration",
    "size",
    "variant",

    "source_contact_id",
    "source_shipper",
    "brand_contact_id",
    "contact_handle",

    "received_date",
    "batch_id",

    "status",
    "tiktok_posted",
    "tiktok_url",
    "instagram_posted",
    "instagram_url",
    "amazon_posted",
    "amazon_url",

    "fragrance_url",
    "country",
    "gender",
    "year",
    "top_notes",
    "middle_notes",
    "base_notes",
    "main_accord_1",
    "main_accord_2",
    "main_accord_3",
    "main_accord_4",
    "main_accord_5",
    "rating_value",
    "rating_count",
    "perfumer1",
    "perfumer2",

    "notes",
    "last_updated",
]

# Contacts schema (NOW includes whatsapp)
CONTACT_FIELDS = [
    "contact_id",
    "name",
    "contact_type",
    "platform",
    "handle",
    "email",
    "phone",
    "whatsapp",
    "notes",
    "last_updated",
]

LOG_FIELDS = ["timestamp", "action", "upc_raw", "upc_normalized", "message"]

# Normalized catalog schema used inside the app
CATALOG_FIELDS = [
    "url",
    "perfume",
    "brand",
    "country",
    "gender",
    "rating_value",
    "rating_count",
    "year",
    "top_notes",
    "middle_notes",
    "base_notes",
    "perfumer1",
    "perfumer2",
    "main_accord_1",
    "main_accord_2",
    "main_accord_3",
    "main_accord_4",
    "main_accord_5",
]


# ----------------------------
# Helpers
# ----------------------------
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def pretty_slug(s: str) -> str:
    return str(s).replace("-", " ").strip().title()


def normalize_bool01(val: str) -> str:
    v = str(val).strip()
    return "1" if v == "1" else "0"


def normalize_upc(upc_raw: str) -> str:
    s = "" if upc_raw is None else str(upc_raw)
    s = s.strip()
    digits = re.sub(r"\D", "", s)
    return digits


def safe_str(x) -> str:
    return "" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x)


def build_variant(concentration: str, size: str, conc_other: str = "", size_other: str = "") -> str:
    c = concentration.strip()
    s = size.strip()
    if c == "Other":
        c = conc_other.strip() or "Other"
    if s == "Other":
        s = size_other.strip() or "Other"
    if c == "Unknown" and s == "Unknown":
        return ""
    if c == "Unknown":
        return s
    if s == "Unknown":
        return c
    return f"{c} | {s}"


def ensure_csv(path: str, columns: List[str]) -> None:
    if not os.path.exists(path):
        pd.DataFrame(columns=columns).to_csv(path, index=False)
        return

    # Backfill missing columns for existing files (migration-safe)
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
        changed = False
        for c in columns:
            if c not in df.columns:
                df[c] = ""
                changed = True
        if changed:
            df = df[columns].copy()
            df.to_csv(path, index=False)
    except Exception:
        # If file exists but can't be read, don't overwrite
        pass


def log_event(activity_log_path: str, action: str, upc_raw: str, upc_norm: str, message: str) -> None:
    event = {
        "timestamp": now_str(),
        "action": action,
        "upc_raw": upc_raw or "",
        "upc_normalized": upc_norm or "",
        "message": message,
    }
    st.session_state.setdefault("activity_log", [])
    st.session_state["activity_log"].append(event)

    try:
        ensure_csv(activity_log_path, LOG_FIELDS)
        df = pd.read_csv(activity_log_path, dtype=str).fillna("")
        df = pd.concat([df, pd.DataFrame([event])], ignore_index=True)
        df.to_csv(activity_log_path, index=False)
    except Exception:
        pass


def load_samples(path: str) -> pd.DataFrame:
    ensure_csv(path, SAMPLES_FIELDS)
    df = pd.read_csv(path, dtype=str).fillna("")

    for c in SAMPLES_FIELDS:
        if c not in df.columns:
            df[c] = ""

    for col in ["tiktok_posted", "instagram_posted", "amazon_posted"]:
        df[col] = df[col].apply(normalize_bool01)

    df["upc"] = df["upc"].astype(str).apply(normalize_upc)

    df["concentration"] = df["concentration"].replace("", "Unknown")
    df["size"] = df["size"].replace("", "Unknown")

    def _rebuild_variant(r):
        v = safe_str(r.get("variant", "")).strip()
        c = safe_str(r.get("concentration", "")).strip() or "Unknown"
        s = safe_str(r.get("size", "")).strip() or "Unknown"
        if not v and (c != "Unknown" or s != "Unknown"):
            return build_variant(c, s)
        return v

    df["variant"] = df.apply(_rebuild_variant, axis=1)
    df = df[SAMPLES_FIELDS].copy()
    return df


def save_samples(df: pd.DataFrame, path: str) -> None:
    for c in SAMPLES_FIELDS:
        if c not in df.columns:
            df[c] = ""
    df = df[SAMPLES_FIELDS].copy()
    df["upc"] = df["upc"].astype(str).apply(normalize_upc)
    df.to_csv(path, index=False)


def load_contacts(path: str) -> pd.DataFrame:
    ensure_csv(path, CONTACT_FIELDS)
    df = pd.read_csv(path, dtype=str).fillna("")
    for c in CONTACT_FIELDS:
        if c not in df.columns:
            df[c] = ""
    df = df[CONTACT_FIELDS].copy()
    return df


def save_contacts(df: pd.DataFrame, path: str) -> None:
    for c in CONTACT_FIELDS:
        if c not in df.columns:
            df[c] = ""
    df = df[CONTACT_FIELDS].copy()
    df.to_csv(path, index=False)


def compute_status(row: pd.Series, track_amazon: bool) -> str:
    tt = str(row.get("tiktok_posted", "0")) == "1"
    ig = str(row.get("instagram_posted", "0")) == "1"
    am = (str(row.get("amazon_posted", "0")) == "1") if track_amazon else True

    all_posted = tt and ig and am
    any_posted = tt or ig or (str(row.get("amazon_posted", "0")) == "1" if track_amazon else False)

    if all_posted:
        return STATUS_COMPLETE
    if any_posted:
        return STATUS_POSTED

    current = str(row.get("status", "")).strip().upper()
    if current == STATUS_FILMED:
        return STATUS_FILMED
    return STATUS_NEW


def get_upc_row_indexes(df: pd.DataFrame, upc_norm: str) -> List[int]:
    if not upc_norm:
        return []
    return df.index[df["upc"] == upc_norm].tolist()


def apply_catalog_to_session(crow: Dict[str, str]) -> None:
    st.session_state["af_brand"] = pretty_slug(crow.get("brand", ""))
    st.session_state["af_product_name"] = pretty_slug(crow.get("perfume", "")) or crow.get("perfume", "")

    st.session_state["af_fragrance_url"] = crow.get("url", "")
    st.session_state["af_country"] = crow.get("country", "")
    st.session_state["af_gender"] = crow.get("gender", "")
    st.session_state["af_year"] = safe_str(crow.get("year", "")).strip()

    st.session_state["af_top_notes"] = crow.get("top_notes", "")
    st.session_state["af_middle_notes"] = crow.get("middle_notes", "")
    st.session_state["af_base_notes"] = crow.get("base_notes", "")

    st.session_state["af_main_accord_1"] = crow.get("main_accord_1", "")
    st.session_state["af_main_accord_2"] = crow.get("main_accord_2", "")
    st.session_state["af_main_accord_3"] = crow.get("main_accord_3", "")
    st.session_state["af_main_accord_4"] = crow.get("main_accord_4", "")
    st.session_state["af_main_accord_5"] = crow.get("main_accord_5", "")

    st.session_state["af_rating_value"] = crow.get("rating_value", "")
    st.session_state["af_rating_count"] = crow.get("rating_count", "")

    st.session_state["af_perfumer1"] = crow.get("perfumer1", "")
    st.session_state["af_perfumer2"] = crow.get("perfumer2", "")


def clear_autofill() -> None:
    keys = [k for k in list(st.session_state.keys()) if k.startswith("af_")]
    for k in keys:
        del st.session_state[k]


def contact_display_row(r: pd.Series) -> str:
    name = safe_str(r.get("name", "")).strip()
    ctype = safe_str(r.get("contact_type", "")).strip()
    handle = safe_str(r.get("handle", "")).strip()
    bits = [name]
    if ctype:
        bits.append(ctype)
    if handle:
        bits.append(handle)
    return " • ".join([b for b in bits if b])


def normalize_catalog_df(df: pd.DataFrame) -> pd.DataFrame:
    # ensure all catalog fields exist
    out = df.copy()
    for c in CATALOG_FIELDS:
        if c not in out.columns:
            out[c] = ""
    out = out[CATALOG_FIELDS].fillna("").copy()

    out["brand"] = out["brand"].astype(str).str.strip()
    out["perfume"] = out["perfume"].astype(str).str.strip()
    out["url"] = out["url"].astype(str).str.strip()
    out["rating_value"] = out["rating_value"].astype(str).str.replace(",", ".", regex=False).str.strip()
    return out[(out["brand"].str.len() > 0) & (out["perfume"].str.len() > 0)].copy()


@st.cache_data
def load_base_catalog_semicolon(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None

    df = pd.read_csv(
        path,
        sep=";",
        encoding="utf-8",
        encoding_errors="replace",
        dtype=str,
    ).fillna("")

    # map likely column names
    def find_col(*candidates):
        existing = {c.lower(): c for c in df.columns}
        for cand in candidates:
            key = cand.lower()
            if key in existing:
                return existing[key]
        return None

    col_url = find_col("URL", "url")
    col_perfume = find_col("Perfume", "perfume")
    col_brand = find_col("Brand", "brand")
    col_country = find_col("Country", "country")
    col_gender = find_col("Gender", "gender")
    col_rating_value = find_col("Rating Value", "rating value", "rating_value")
    col_rating_count = find_col("Rating Count", "rating count", "rating_count")
    col_year = find_col("Year", "year")

    col_top = find_col("Top Notes", "Top", "top")
    col_middle = find_col("Middle Notes", "Middle", "middle")
    col_base = find_col("Base Notes", "Base", "base")

    col_perfumer1 = find_col("Perfumer1", "perfumer1", "Perfumer 1")
    col_perfumer2 = find_col("Perfumer2", "perfumer2", "Perfumer 2")

    col_a1 = find_col("Main Accord 1", "mainaccord1", "main_accord_1")
    col_a2 = find_col("Main Accord 2", "mainaccord2", "main_accord_2")
    col_a3 = find_col("Main Accord 3", "mainaccord3", "main_accord_3")
    col_a4 = find_col("Main Accord 4", "mainaccord4", "main_accord_4")
    col_a5 = find_col("Main Accord 5", "mainaccord5", "main_accord_5")

    out = pd.DataFrame()
    out["url"] = df[col_url] if col_url else ""
    out["perfume"] = df[col_perfume] if col_perfume else ""
    out["brand"] = df[col_brand] if col_brand else ""
    out["country"] = df[col_country] if col_country else ""
    out["gender"] = df[col_gender] if col_gender else ""
    out["rating_value"] = df[col_rating_value] if col_rating_value else ""
    out["rating_count"] = df[col_rating_count] if col_rating_count else ""
    out["year"] = df[col_year] if col_year else ""

    out["top_notes"] = df[col_top] if col_top else ""
    out["middle_notes"] = df[col_middle] if col_middle else ""
    out["base_notes"] = df[col_base] if col_base else ""

    out["perfumer1"] = df[col_perfumer1] if col_perfumer1 else ""
    out["perfumer2"] = df[col_perfumer2] if col_perfumer2 else ""

    out["main_accord_1"] = df[col_a1] if col_a1 else ""
    out["main_accord_2"] = df[col_a2] if col_a2 else ""
    out["main_accord_3"] = df[col_a3] if col_a3 else ""
    out["main_accord_4"] = df[col_a4] if col_a4 else ""
    out["main_accord_5"] = df[col_a5] if col_a5 else ""

    return normalize_catalog_df(out)


def load_catalog_overrides(path: str) -> pd.DataFrame:
    # overrides are regular comma CSV
    ensure_csv(path, CATALOG_FIELDS)
    df = pd.read_csv(path, dtype=str).fillna("")
    return normalize_catalog_df(df)


def save_catalog_overrides(df: pd.DataFrame, path: str) -> None:
    for c in CATALOG_FIELDS:
        if c not in df.columns:
            df[c] = ""
    df = normalize_catalog_df(df)
    df.to_csv(path, index=False)


def merge_catalog(base_df: Optional[pd.DataFrame], overrides_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if base_df is None and overrides_df is None:
        return None
    if base_df is None:
        return overrides_df.copy()

    merged = pd.concat([base_df.copy(), overrides_df.copy()], ignore_index=True)
    # overrides win: keep last by (brand, perfume)
    merged["_key"] = (merged["brand"].str.lower().str.strip() + "||" + merged["perfume"].str.lower().str.strip())
    merged = merged.drop_duplicates(subset=["_key"], keep="last").drop(columns=["_key"])
    return merged


def update_item_by_upc(
    df: pd.DataFrame,
    samples_path: str,
    activity_log_path: str,
    upc_raw: str,
    updates: Dict[str, str],
    track_amazon: bool
) -> pd.DataFrame:
    upc_norm = normalize_upc(upc_raw)
    hits = get_upc_row_indexes(df, upc_norm)
    if not upc_norm:
        raise ValueError("UPC is required.")
    if not hits:
        raise KeyError("UPC not found in inventory.")

    idx = hits[0]
    for k, v in updates.items():
        if k in df.columns:
            df.at[idx, k] = "" if v is None else str(v)

    for col in ["tiktok_posted", "instagram_posted", "amazon_posted"]:
        df.at[idx, col] = normalize_bool01(df.at[idx, col])

    df.at[idx, "status"] = compute_status(df.loc[idx], track_amazon)
    df.at[idx, "last_updated"] = now_str()

    save_samples(df, samples_path)
    log_event(activity_log_path, "UPDATE_BY_UPC", upc_raw, upc_norm, "Updated item by UPC.")
    return df


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Sample Inventory + Content Tracker", layout="wide")
st.title("Sample Inventory + Content Tracker")

with st.sidebar:
    st.subheader("Storage")
    samples_path = st.text_input("Samples CSV path", value=DEFAULT_SAMPLES_CSV)
    contacts_path = st.text_input("Contacts CSV path", value=DEFAULT_CONTACTS_CSV)
    activity_log_path = st.text_input("Activity log CSV path", value=DEFAULT_ACTIVITY_LOG_CSV)

    st.subheader("Catalog")
    catalog_path = st.text_input("Base catalog path (read-only)", value=DEFAULT_CATALOG_CSV)
    catalog_overrides_path = st.text_input("Catalog overrides path (editable)", value=DEFAULT_CATALOG_OVERRIDES_CSV)

    st.subheader("Platforms")
    track_amazon = st.toggle("Track Amazon postings", value=True)

samples_df = load_samples(samples_path)
contacts_df = load_contacts(contacts_path)
ensure_csv(activity_log_path, LOG_FIELDS)

base_catalog_df = load_base_catalog_semicolon(catalog_path)
overrides_df = load_catalog_overrides(catalog_overrides_path)
catalog_df = merge_catalog(base_catalog_df, overrides_df)

if "activity_log" not in st.session_state:
    try:
        st.session_state["activity_log"] = pd.read_csv(activity_log_path, dtype=str).fillna("").tail(200).to_dict("records")
    except Exception:
        st.session_state["activity_log"] = []

contacts_df_sorted = contacts_df.sort_values(by=["name", "contact_type"], ascending=[True, True]).copy()
contact_id_to_name = {safe_str(r["contact_id"]): safe_str(r["name"]) for _, r in contacts_df_sorted.iterrows()}
contact_options = ["(None)"] + contacts_df_sorted["contact_id"].tolist()
contact_label_map = {"(None)": "(None)"}
for _, r in contacts_df_sorted.iterrows():
    contact_label_map[safe_str(r["contact_id"])] = contact_display_row(r)


# ----------------------------
# Quick Scan / Lookup
# ----------------------------
st.subheader("Quick Scan / Lookup")

c1, c2, c3 = st.columns([2.2, 2.2, 5.6])
with c1:
    scan_upc_raw = st.text_input("Scan or paste UPC", value=st.session_state.get("scan_upc_raw", ""), placeholder="Click here, scan barcode")
    st.session_state["scan_upc_raw"] = scan_upc_raw
    scan_upc_norm = normalize_upc(scan_upc_raw)

with c2:
    search_text = st.text_input("Search inventory", value=st.session_state.get("search_text", ""), placeholder="Brand, product, shipper, notes…").strip()
    st.session_state["search_text"] = search_text

with c3:
    st.caption("Use Add / Receive to add new UPCs. Use Posting Updates to mark posted + add URLs.")

scan_hits = get_upc_row_indexes(samples_df, scan_upc_norm)
if scan_upc_raw and not scan_upc_norm:
    st.warning("Your scan contained no digits. Try scanning again.")
elif scan_upc_norm and not scan_hits:
    st.info("UPC not found. Go to **Add / Receive** to add it.")
elif scan_upc_norm and scan_hits:
    st.success(f"UPC found ({len(scan_hits)} match). You can update it from **Posting Updates** or **Inventory Admin**.")


filtered_df = samples_df.copy()
if search_text:
    q = search_text.lower()
    hay = (
        filtered_df["upc"] + " " +
        filtered_df["brand"] + " " +
        filtered_df["product_name"] + " " +
        filtered_df["variant"] + " " +
        filtered_df["source_shipper"] + " " +
        filtered_df["contact_handle"] + " " +
        filtered_df["notes"]
    ).str.lower()
    filtered_df = filtered_df[hay.str.contains(q, na=False)].copy()


tab_dash, tab_add, tab_post, tab_inventory, tab_contacts, tab_catalog, tab_log = st.tabs(
    ["Dashboard", "Add / Receive", "Posting Updates", "Inventory Admin", "Contacts", "Catalog", "Activity Log"]
)


# ----------------------------
# Dashboard
# ----------------------------
with tab_dash:
    st.subheader("Dashboard")
    total = len(samples_df)
    open_items = int((samples_df["status"] != STATUS_COMPLETE).sum()) if total else 0
    needs_filming = int((samples_df["status"] == STATUS_NEW).sum()) if total else 0

    def needs_posting(row: pd.Series) -> bool:
        if row.get("status", "") not in [STATUS_FILMED, STATUS_POSTED]:
            return False
        tt = row.get("tiktok_posted", "0") == "1"
        ig = row.get("instagram_posted", "0") == "1"
        am = (row.get("amazon_posted", "0") == "1") if track_amazon else True
        return not (tt and ig and am)

    needs_post = int(samples_df.apply(needs_posting, axis=1).sum()) if total else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total items", total)
    m2.metric("Open (not complete)", open_items)
    m3.metric("Needs filming", needs_filming)
    m4.metric("Needs posting", needs_post)

    st.divider()
    focus = samples_df[samples_df.apply(needs_posting, axis=1)].copy()
    focus = focus.sort_values(by=["received_date", "brand", "product_name"], ascending=[False, True, True])
    if focus.empty:
        st.write("No items are waiting on posting.")
    else:
        st.dataframe(
            focus[["upc", "brand", "product_name", "variant", "status", "tiktok_posted", "instagram_posted", "amazon_posted", "received_date", "source_shipper"]],
            use_container_width=True,
            hide_index=True,
        )


# ----------------------------
# Add / Receive
# ----------------------------
with tab_add:
    st.subheader("Add / Receive")
    left, right = st.columns([1.05, 1])

    with left:
        st.markdown("### Step 1: Pick fragrance from catalog (autofill)")
        if catalog_df is None or catalog_df.empty:
            st.warning("Catalog not loaded. Confirm data/fra_cleaned.csv exists and/or add entries in Catalog overrides.")
        else:
            brand_filter = st.text_input("Brand filter (optional)", value=st.session_state.get("brand_filter", ""), placeholder="Type to narrow brands…").strip().lower()
            st.session_state["brand_filter"] = brand_filter

            brands = sorted(catalog_df["brand"].unique().tolist())
            if brand_filter:
                brands = [b for b in brands if brand_filter in b.lower()]

            if not brands:
                st.info("No brands match your filter. Clear it or add to Catalog overrides.")
                st.stop()

            selected_brand = st.selectbox("Brand", options=brands, format_func=pretty_slug)
            brand_df = catalog_df[catalog_df["brand"] == selected_brand].copy()
            if brand_df.empty:
                st.info("No perfumes for this brand in catalog. Add via Catalog overrides.")
                st.stop()

            perfume_filter = st.text_input("Perfume filter (optional)", value=st.session_state.get("perfume_filter", ""), placeholder="Type to narrow perfumes…").strip().lower()
            st.session_state["perfume_filter"] = perfume_filter

            perfumes = sorted(brand_df["perfume"].unique().tolist())
            if perfume_filter:
                perfumes = [p for p in perfumes if perfume_filter in p.lower()]

            if not perfumes:
                st.info("No perfumes match. Clear perfume filter or add to overrides.")
                st.stop()

            selected_perfume = st.selectbox("Perfume", options=perfumes, format_func=pretty_slug)
            picked_df = brand_df[brand_df["perfume"] == selected_perfume].copy()
            if picked_df.empty:
                st.info("Selection not found. Reselect.")
                st.stop()

            picked_row = picked_df.iloc[0].to_dict()

            a1, a2 = st.columns([1, 1])
            with a1:
                if st.button("Use this fragrance to autofill", type="primary"):
                    apply_catalog_to_session(picked_row)
                    log_event(activity_log_path, "AUTOFILL", "", "", f"Selected {pretty_slug(picked_row.get('brand',''))} - {pretty_slug(picked_row.get('perfume',''))}")
                    st.success("Autofill applied.")
            with a2:
                if st.button("Clear autofill"):
                    clear_autofill()
                    st.info("Autofill cleared.")

            with st.expander("Preview details", expanded=False):
                st.write({
                    "Brand": pretty_slug(picked_row.get("brand", "")),
                    "Perfume": pretty_slug(picked_row.get("perfume", "")),
                    "Year": picked_row.get("year", ""),
                    "Gender": picked_row.get("gender", ""),
                    "Top notes": picked_row.get("top_notes", ""),
                    "Middle notes": picked_row.get("middle_notes", ""),
                    "Base notes": picked_row.get("base_notes", ""),
                    "Accords": [
                        picked_row.get("main_accord_1", ""),
                        picked_row.get("main_accord_2", ""),
                        picked_row.get("main_accord_3", ""),
                        picked_row.get("main_accord_4", ""),
                        picked_row.get("main_accord_5", ""),
                    ],
                    "Rating": f'{picked_row.get("rating_value","")} ({picked_row.get("rating_count","")} ratings)',
                    "URL": picked_row.get("url", ""),
                })

    with right:
        st.markdown("### Step 2: Scan UPC, confirm, and save")
        st.caption("If you hit a UPC conflict, you can inspect it and choose to update the existing record instead.")

        default_upc_raw = st.session_state.get("add_upc_raw", "")
        if scan_upc_raw and not get_upc_row_indexes(samples_df, scan_upc_norm):
            default_upc_raw = scan_upc_raw

        st.session_state.setdefault("add_upc_raw", default_upc_raw)
        st.session_state.setdefault("add_brand", st.session_state.get("af_brand", ""))
        st.session_state.setdefault("add_product_name", st.session_state.get("af_product_name", ""))

        st.session_state.setdefault("add_concentration", "Unknown")
        st.session_state.setdefault("add_conc_other", "")
        st.session_state.setdefault("add_size", "Unknown")
        st.session_state.setdefault("add_size_other", "")

        st.session_state.setdefault("add_batch_id", "")
        st.session_state.setdefault("add_source_contact_id", "(None)")
        st.session_state.setdefault("add_source_shipper_override", "")
        st.session_state.setdefault("add_brand_contact_id", "(None)")
        st.session_state.setdefault("add_brand_contact_text", "")
        st.session_state.setdefault("add_notes", "")

        with st.form("add_item_form", clear_on_submit=False):
            r1, r2 = st.columns(2)
            add_upc_raw = r1.text_input("UPC (scan here)", value=st.session_state["add_upc_raw"])
            st.session_state["add_upc_raw"] = add_upc_raw
            add_upc_norm = normalize_upc(add_upc_raw)

            received = r2.date_input("Received date", value=date.today())

            r3, r4 = st.columns(2)
            if not st.session_state.get("add_brand"):
                st.session_state["add_brand"] = st.session_state.get("af_brand", "")
            if not st.session_state.get("add_product_name"):
                st.session_state["add_product_name"] = st.session_state.get("af_product_name", "")

            add_brand = r3.text_input("Brand", value=st.session_state["add_brand"])
            st.session_state["add_brand"] = add_brand

            add_name = r4.text_input("Product name", value=st.session_state["add_product_name"])
            st.session_state["add_product_name"] = add_name

            d1, d2 = st.columns(2)
            add_conc = d1.selectbox("Type / Concentration", options=CONCENTRATION_OPTIONS,
                                   index=CONCENTRATION_OPTIONS.index(st.session_state["add_concentration"]) if st.session_state["add_concentration"] in CONCENTRATION_OPTIONS else 0)
            st.session_state["add_concentration"] = add_conc
            conc_other = ""
            if add_conc == "Other":
                conc_other = d1.text_input("If Other, specify", value=st.session_state["add_conc_other"])
                st.session_state["add_conc_other"] = conc_other

            add_size = d2.selectbox("Size", options=SIZE_OPTIONS,
                                    index=SIZE_OPTIONS.index(st.session_state["add_size"]) if st.session_state["add_size"] in SIZE_OPTIONS else 0)
            st.session_state["add_size"] = add_size
            size_other = ""
            if add_size == "Other":
                size_other = d2.text_input("If Other, specify", value=st.session_state["add_size_other"])
                st.session_state["add_size_other"] = size_other

            variant_str = build_variant(add_conc, add_size, st.session_state.get("add_conc_other", ""), st.session_state.get("add_size_other", ""))

            st.markdown("**Contacts**")
            c1, c2 = st.columns(2)

            source_contact_id = c1.selectbox(
                "Sent by (Contact)",
                options=contact_options,
                index=contact_options.index(st.session_state["add_source_contact_id"]) if st.session_state["add_source_contact_id"] in contact_options else 0,
                format_func=lambda cid: contact_label_map.get(cid, cid),
            )
            st.session_state["add_source_contact_id"] = source_contact_id

            shipper_override = c1.text_input(
                "If not in contacts, type sender name (optional)",
                value=st.session_state["add_source_shipper_override"],
                placeholder="Leave blank if you selected a contact above"
            )
            st.session_state["add_source_shipper_override"] = shipper_override

            brand_contact_id = c2.selectbox(
                "Brand Contact (optional)",
                options=contact_options,
                index=contact_options.index(st.session_state["add_brand_contact_id"]) if st.session_state["add_brand_contact_id"] in contact_options else 0,
                format_func=lambda cid: contact_label_map.get(cid, cid),
            )
            st.session_state["add_brand_contact_id"] = brand_contact_id

            brand_contact_text = c2.text_input(
                "Brand Contact handle (optional)",
                value=st.session_state["add_brand_contact_text"],
                placeholder="@brandhandle or person name"
            )
            st.session_state["add_brand_contact_text"] = brand_contact_text

            r7, r8 = st.columns(2)
            add_batch = r7.text_input("Batch ID (optional)", value=st.session_state["add_batch_id"])
            st.session_state["add_batch_id"] = add_batch

            add_notes = st.text_area("Notes (optional)", value=st.session_state["add_notes"], height=90)
            st.session_state["add_notes"] = add_notes

            submitted = st.form_submit_button("Save to inventory", type="primary")

        if submitted:
            if not add_upc_norm:
                log_event(activity_log_path, "ADD_FAIL", add_upc_raw, add_upc_norm, "UPC missing or contained no digits.")
                st.error("UPC is required (must include digits).")
            else:
                hits = get_upc_row_indexes(samples_df, add_upc_norm)

                selected_source_name = ""
                if source_contact_id != "(None)":
                    selected_source_name = contact_id_to_name.get(source_contact_id, "")
                if shipper_override.strip():
                    selected_source_name = shipper_override.strip()
                    source_contact_id_final = "(None)"
                else:
                    source_contact_id_final = source_contact_id

                if hits:
                    log_event(activity_log_path, "UPC_CONFLICT", add_upc_raw, add_upc_norm, f"UPC matched {len(hits)} existing row(s).")
                    st.error("That UPC already exists in your inventory.")
                    with st.expander("Show matching record(s)", expanded=True):
                        st.dataframe(
                            samples_df.iloc[hits][["upc", "brand", "product_name", "variant", "received_date", "source_shipper", "status", "last_updated"]],
                            use_container_width=True,
                            hide_index=True,
                        )
                else:
                    row = {k: "" for k in SAMPLES_FIELDS}
                    row["upc"] = add_upc_norm
                    row["brand"] = add_brand.strip()
                    row["product_name"] = add_name.strip()
                    row["concentration"] = add_conc
                    row["size"] = add_size
                    row["variant"] = variant_str

                    row["source_contact_id"] = "" if source_contact_id_final == "(None)" else source_contact_id_final
                    row["source_shipper"] = selected_source_name.strip()
                    row["brand_contact_id"] = "" if brand_contact_id == "(None)" else brand_contact_id
                    row["contact_handle"] = brand_contact_text.strip()

                    row["received_date"] = str(received)
                    row["batch_id"] = add_batch.strip()

                    row["status"] = STATUS_NEW
                    row["tiktok_posted"] = "0"
                    row["instagram_posted"] = "0"
                    row["amazon_posted"] = "0"

                    row["fragrance_url"] = st.session_state.get("af_fragrance_url", "")
                    row["country"] = st.session_state.get("af_country", "")
                    row["gender"] = st.session_state.get("af_gender", "")
                    row["year"] = st.session_state.get("af_year", "")
                    row["top_notes"] = st.session_state.get("af_top_notes", "")
                    row["middle_notes"] = st.session_state.get("af_middle_notes", "")
                    row["base_notes"] = st.session_state.get("af_base_notes", "")
                    row["main_accord_1"] = st.session_state.get("af_main_accord_1", "")
                    row["main_accord_2"] = st.session_state.get("af_main_accord_2", "")
                    row["main_accord_3"] = st.session_state.get("af_main_accord_3", "")
                    row["main_accord_4"] = st.session_state.get("af_main_accord_4", "")
                    row["main_accord_5"] = st.session_state.get("af_main_accord_5", "")
                    row["rating_value"] = st.session_state.get("af_rating_value", "")
                    row["rating_count"] = st.session_state.get("af_rating_count", "")
                    row["perfumer1"] = st.session_state.get("af_perfumer1", "")
                    row["perfumer2"] = st.session_state.get("af_perfumer2", "")

                    row["notes"] = add_notes.strip()
                    row["last_updated"] = now_str()
                    row["status"] = compute_status(pd.Series(row), track_amazon)

                    samples_df = pd.concat([samples_df, pd.DataFrame([row])], ignore_index=True)
                    save_samples(samples_df, samples_path)
                    log_event(activity_log_path, "ADD_SUCCESS", add_upc_raw, add_upc_norm, "Added new inventory row.")
                    st.success("Saved.")

                    # Clear on success
                    st.session_state["add_upc_raw"] = ""
                    st.session_state["add_brand"] = st.session_state.get("af_brand", "")
                    st.session_state["add_product_name"] = st.session_state.get("af_product_name", "")
                    st.session_state["add_concentration"] = "Unknown"
                    st.session_state["add_conc_other"] = ""
                    st.session_state["add_size"] = "Unknown"
                    st.session_state["add_size_other"] = ""
                    st.session_state["add_batch_id"] = ""
                    st.session_state["add_source_contact_id"] = "(None)"
                    st.session_state["add_source_shipper_override"] = ""
                    st.session_state["add_brand_contact_id"] = "(None)"
                    st.session_state["add_brand_contact_text"] = ""
                    st.session_state["add_notes"] = ""
                    st.rerun()


# ----------------------------
# Posting Updates (By UPC)
# ----------------------------
with tab_post:
    st.subheader("Posting Updates")
    st.caption("Scan/paste a UPC, then mark platforms as posted and add URLs. This saves immediately.")

    pu1, pu2 = st.columns([2.2, 7.8])
    with pu1:
        pu_upc_raw = st.text_input("UPC to update", value=scan_upc_raw, placeholder="Scan barcode")
        pu_upc_norm = normalize_upc(pu_upc_raw)

    hits = get_upc_row_indexes(samples_df, pu_upc_norm) if pu_upc_norm else []
    if not pu_upc_norm:
        st.info("Scan/paste a UPC to begin.")
    elif not hits:
        st.error("UPC not found. Add it first in Add / Receive.")
    else:
        idx = hits[0]
        row = samples_df.loc[idx].copy()

        st.write(f"**{row.get('brand','')}** — {row.get('product_name','')}  \nUPC: `{row.get('upc','')}`  \nCurrent status: **{row.get('status','')}**")

        with st.form("posting_update_form", clear_on_submit=False):
            p1, p2, p3 = st.columns(3)

            tt_posted = p1.checkbox("TikTok posted", value=(row.get("tiktok_posted","0") == "1"))
            tt_url = p1.text_input("TikTok URL", value=row.get("tiktok_url",""))

            ig_posted = p2.checkbox("Instagram posted", value=(row.get("instagram_posted","0") == "1"))
            ig_url = p2.text_input("Instagram URL", value=row.get("instagram_url",""))

            if track_amazon:
                am_posted = p3.checkbox("Amazon posted", value=(row.get("amazon_posted","0") == "1"))
                am_url = p3.text_input("Amazon URL", value=row.get("amazon_url",""))
            else:
                am_posted = True
                am_url = row.get("amazon_url","")
                p3.caption("Amazon tracking is turned off in the sidebar.")

            save_posting = st.form_submit_button("Save posting updates", type="primary")

        if save_posting:
            updates = {
                "tiktok_posted": "1" if tt_posted else "0",
                "tiktok_url": tt_url.strip(),
                "instagram_posted": "1" if ig_posted else "0",
                "instagram_url": ig_url.strip(),
            }
            if track_amazon:
                updates.update({
                    "amazon_posted": "1" if am_posted else "0",
                    "amazon_url": am_url.strip(),
                })

            try:
                samples_df = update_item_by_upc(samples_df, samples_path, activity_log_path, pu_upc_raw, updates, track_amazon)
                st.success("Posting updates saved.")
                st.rerun()
            except Exception as e:
                st.error(str(e))


# ----------------------------
# Inventory Admin (Update + Delete)
# ----------------------------
with tab_inventory:
    st.subheader("Inventory Admin")
    st.caption("Update an item by UPC, or delete items safely. Also includes a full table editor for advanced fixes.")

    st.markdown("### Update item by UPC")
    iu1, iu2 = st.columns([2.2, 7.8])
    with iu1:
        iu_upc_raw = st.text_input("UPC to edit", value=scan_upc_raw, placeholder="Scan barcode")
        iu_upc_norm = normalize_upc(iu_upc_raw)

    hits = get_upc_row_indexes(samples_df, iu_upc_norm) if iu_upc_norm else []
    if iu_upc_norm and hits:
        idx = hits[0]
        row = samples_df.loc[idx].copy()

        with st.form("inventory_update_form", clear_on_submit=False):
            a1, a2 = st.columns(2)
            brand = a1.text_input("Brand", value=row.get("brand",""))
            name = a2.text_input("Product name", value=row.get("product_name",""))

            b1, b2 = st.columns(2)
            conc = b1.selectbox("Type / Concentration", options=CONCENTRATION_OPTIONS,
                                index=CONCENTRATION_OPTIONS.index(row.get("concentration","Unknown")) if row.get("concentration","Unknown") in CONCENTRATION_OPTIONS else 0)
            size = b2.selectbox("Size", options=SIZE_OPTIONS,
                                index=SIZE_OPTIONS.index(row.get("size","Unknown")) if row.get("size","Unknown") in SIZE_OPTIONS else 0)

            c1, c2 = st.columns(2)
            status = c1.selectbox("Status", options=ALL_STATUSES, index=ALL_STATUSES.index(row.get("status", STATUS_NEW)) if row.get("status", STATUS_NEW) in ALL_STATUSES else 0)
            received_date = c2.text_input("Received date (YYYY-MM-DD)", value=row.get("received_date",""))

            notes = st.text_area("Notes", value=row.get("notes",""), height=90)

            save_edit = st.form_submit_button("Save item changes", type="primary")

        if save_edit:
            updates = {
                "brand": brand.strip(),
                "product_name": name.strip(),
                "concentration": conc,
                "size": size,
                "variant": build_variant(conc, size),
                "status": status,
                "received_date": received_date.strip(),
                "notes": notes.strip(),
            }
            try:
                samples_df = update_item_by_upc(samples_df, samples_path, activity_log_path, iu_upc_raw, updates, track_amazon)
                st.success("Item updated.")
                st.rerun()
            except Exception as e:
                st.error(str(e))
    elif iu_upc_norm and not hits:
        st.info("UPC not found. Add it in Add / Receive.")
    else:
        st.info("Scan/paste a UPC to edit an item.")

    st.divider()
    st.markdown("### Delete inventory items (safe)")
    st.caption("Select UPCs to delete, then confirm. This is permanent in the CSV.")

    del_upcs = st.multiselect(
        "Select UPC(s) to delete",
        options=sorted([u for u in samples_df["upc"].unique().tolist() if u]),
        default=[],
    )

    if del_upcs:
        preview = samples_df[samples_df["upc"].isin(del_upcs)][["upc", "brand", "product_name", "variant", "received_date", "status"]].copy()
        st.dataframe(preview, use_container_width=True, hide_index=True)

        confirm = st.checkbox("I understand this will permanently delete the selected UPC rows.")
        if st.button("Delete selected", type="primary", disabled=not confirm):
            before = len(samples_df)
            samples_df = samples_df[~samples_df["upc"].isin(del_upcs)].copy()
            after = len(samples_df)
            save_samples(samples_df, samples_path)
            log_event(activity_log_path, "DELETE_ITEMS", "", "", f"Deleted {before-after} row(s).")
            st.success(f"Deleted {before-after} row(s).")
            st.rerun()

    st.divider()
    st.markdown("### Full table editor (advanced)")
    st.caption("Use this for rare fixes. Posting changes are easier in Posting Updates tab.")

    edited = st.data_editor(
        filtered_df,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
    )

    if st.button("Save table edits", type="primary"):
        updated = samples_df.copy()

        edited_map = {}
        for _, r in edited.iterrows():
            u = normalize_upc(r.get("upc", ""))
            if u:
                edited_map[u] = r

        for i in range(len(updated)):
            u = updated.at[i, "upc"]
            if u in edited_map:
                for col in SAMPLES_FIELDS:
                    val = edited_map[u].get(col, "")
                    updated.at[i, col] = "" if pd.isna(val) else str(val)

        updated["upc"] = updated["upc"].astype(str).apply(normalize_upc)
        for i in range(len(updated)):
            updated.at[i, "tiktok_posted"] = normalize_bool01(updated.at[i, "tiktok_posted"])
            updated.at[i, "instagram_posted"] = normalize_bool01(updated.at[i, "instagram_posted"])
            updated.at[i, "amazon_posted"] = normalize_bool01(updated.at[i, "amazon_posted"])
            updated.at[i, "status"] = compute_status(updated.loc[i], track_amazon)
            updated.at[i, "last_updated"] = now_str()

        save_samples(updated, samples_path)
        log_event(activity_log_path, "INVENTORY_TABLE_SAVE", "", "", "Saved edits from table editor.")
        st.success("Saved.")
        st.rerun()


# ----------------------------
# Contacts
# ----------------------------
with tab_contacts:
    st.subheader("Contacts")
    st.caption("WhatsApp is supported. Create contacts once, then select them in Add/Receive.")

    st.markdown("### Add new contact")
    with st.form("add_contact_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        name = c1.text_input("Name", placeholder="Maison Alhambra, VV Fragrances Wholesale, John Doe…")
        ctype = c2.selectbox("Contact type", options=CONTACT_TYPE_OPTIONS, index=0)
        platform = c3.selectbox("Primary platform", options=["", "Instagram", "TikTok", "Email", "Phone", "Website", "Other"], index=0)

        d1, d2, d3, d4 = st.columns(4)
        handle = d1.text_input("Handle (optional)", placeholder="@brandhandle")
        email = d2.text_input("Email (optional)")
        phone = d3.text_input("Phone (optional)")
        whatsapp = d4.text_input("WhatsApp number (optional)", placeholder="+1 555 555 5555")

        notes = st.text_area("Notes (optional)", height=70)
        add_contact = st.form_submit_button("Add contact", type="primary")

    if add_contact:
        if not name.strip():
            st.error("Name is required.")
        else:
            new_id = f"c_{int(time.time() * 1000)}"
            new_row = {
                "contact_id": new_id,
                "name": name.strip(),
                "contact_type": ctype.strip(),
                "platform": platform.strip(),
                "handle": handle.strip(),
                "email": email.strip(),
                "phone": phone.strip(),
                "whatsapp": whatsapp.strip(),
                "notes": notes.strip(),
                "last_updated": now_str(),
            }
            contacts_df = pd.concat([contacts_df, pd.DataFrame([new_row])], ignore_index=True)
            save_contacts(contacts_df, contacts_path)
            st.success("Contact added.")
            st.rerun()

    st.divider()
    st.markdown("### Manage contacts")
    contacts_view = contacts_df.sort_values(by=["name", "contact_type"], ascending=[True, True]).copy()
    edited_contacts = st.data_editor(
        contacts_view,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
    )

    if st.button("Save contacts", type="primary"):
        dfc = edited_contacts.copy()
        if "last_updated" in dfc.columns:
            dfc["last_updated"] = now_str()
        save_contacts(dfc, contacts_path)
        st.success("Contacts saved.")
        st.rerun()

    st.divider()
    st.markdown("### Inventory by contact")
    contacts_df2 = load_contacts(contacts_path).sort_values(by=["name", "contact_type"], ascending=[True, True]).copy()
    if contacts_df2.empty:
        st.info("Add at least one contact to use this view.")
    else:
        cid_to_row = {safe_str(r["contact_id"]): r for _, r in contacts_df2.iterrows()}
        contact_ids = contacts_df2["contact_id"].tolist()
        selected_contact_id = st.selectbox(
            "Select a contact",
            options=contact_ids,
            format_func=lambda cid: contact_display_row(cid_to_row[cid]),
        )
        selected = cid_to_row[selected_contact_id]
        selected_name = safe_str(selected.get("name", "")).strip()

        inv = samples_df.copy()
        inv["source_contact_id"] = inv["source_contact_id"].fillna("")
        inv["source_shipper"] = inv["source_shipper"].fillna("")

        mask = (inv["source_contact_id"] == selected_contact_id) | (inv["source_shipper"].str.lower() == selected_name.lower())
        inv_hits = inv[mask].copy()

        if safe_str(selected.get("contact_type", "")) == "Brand" and selected_name:
            inv_hits = pd.concat([inv_hits, inv[inv["brand"].str.lower() == selected_name.lower()]], ignore_index=True).drop_duplicates()

        if inv_hits.empty:
            st.info("No inventory items linked to this contact yet.")
        else:
            st.dataframe(
                inv_hits[["upc", "brand", "product_name", "variant", "received_date", "status", "source_shipper", "contact_handle"]],
                use_container_width=True,
                hide_index=True,
            )


# ----------------------------
# Catalog
# ----------------------------
with tab_catalog:
    st.subheader("Catalog")
    st.caption("Base catalog is read-only (fra_cleaned.csv). Use overrides to add missing items or correct bad entries safely.")

    browse, admin = st.tabs(["Browse merged catalog", "Catalog overrides admin"])

    with browse:
        if catalog_df is None or catalog_df.empty:
            st.warning("Merged catalog is empty. Add items in overrides admin.")
        else:
            f1, f2 = st.columns([2.5, 2.5])
            bfilter = f1.text_input("Brand filter", value="").strip().lower()
            pfilter = f2.text_input("Perfume filter", value="").strip().lower()

            view = catalog_df.copy()
            if bfilter:
                view = view[view["brand"].str.lower().str.contains(bfilter, na=False)]
            if pfilter:
                view = view[view["perfume"].str.lower().str.contains(pfilter, na=False)]

            show_cols = ["brand", "perfume", "year", "gender", "country", "main_accord_1", "main_accord_2", "main_accord_3", "rating_value", "rating_count", "url"]
            st.dataframe(view[show_cols], use_container_width=True, hide_index=True)

    with admin:
        st.markdown("### Edit catalog overrides (adds/repairs entries)")
        st.caption("This writes to catalog_overrides.csv. Overrides win when merged with the base catalog.")

        ov = load_catalog_overrides(catalog_overrides_path)
        edited_ov = st.data_editor(ov, use_container_width=True, hide_index=True, num_rows="dynamic")

        col1, col2 = st.columns([1.2, 8.8])
        with col1:
            if st.button("Save overrides", type="primary"):
                try:
                    save_catalog_overrides(edited_ov, catalog_overrides_path)
                    log_event(activity_log_path, "SAVE_CATALOG_OVERRIDES", "", "", "Saved catalog overrides.")
                    st.success("Overrides saved.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

        with col2:
            st.download_button(
                label="Download catalog_overrides.csv",
                data=ov.to_csv(index=False).encode("utf-8"),
                file_name=os.path.basename(catalog_overrides_path),
                mime="text/csv",
            )


# ----------------------------
# Activity Log
# ----------------------------
with tab_log:
    st.subheader("Activity Log")
    log_rows = st.session_state.get("activity_log", [])
    if not log_rows:
        st.write("No activity logged yet.")
    else:
        df_log = pd.DataFrame(log_rows).tail(200).iloc[::-1]
        st.dataframe(df_log, use_container_width=True, hide_index=True)

    st.download_button(
        label="Download activity_log.csv",
        data=pd.DataFrame(log_rows).to_csv(index=False).encode("utf-8"),
        file_name=os.path.basename(activity_log_path),
        mime="text/csv",
    )

st.caption(
    "Note: On Streamlit Community Cloud, file writes can be ephemeral after restarts. "
    "If you want true persistence, the next step is saving CSV changes back to GitHub automatically."
)