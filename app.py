# app.py
# Sample Inventory + Content Tracker (CSV-backed) with Fragrance Catalog Autofill (fra_cleaned.csv)
# Improvements:
# - Consistent UPC normalization
# - Add/Receive does NOT lose entered data on validation errors
# - When UPC "already exists", show matching rows + tools to fix
# - Activity log (UI + activity_log.csv best-effort)

import os
import re
from datetime import datetime, date
from typing import Optional, Dict, List

import pandas as pd
import streamlit as st


# ----------------------------
# Configuration
# ----------------------------
DEFAULT_SAMPLES_CSV = "samples.csv"
DEFAULT_CATALOG_CSV = "data/fra_cleaned.csv"
DEFAULT_ACTIVITY_LOG_CSV = "activity_log.csv"

STATUS_NEW = "NEW"
STATUS_FILMED = "FILMED"
STATUS_POSTED = "POSTED"
STATUS_COMPLETE = "COMPLETE"
ALL_STATUSES = [STATUS_NEW, STATUS_FILMED, STATUS_POSTED, STATUS_COMPLETE]

SAMPLES_FIELDS = [
    # identity + basics
    "upc",
    "brand",
    "product_name",
    "variant",

    # shipping / relationship
    "source_shipper",
    "contact_handle",
    "received_date",
    "batch_id",

    # workflow
    "status",
    "tiktok_posted",
    "tiktok_url",
    "instagram_posted",
    "instagram_url",
    "amazon_posted",
    "amazon_url",

    # fragrance metadata (from catalog)
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

    # misc
    "notes",
    "last_updated",
]


LOG_FIELDS = ["timestamp", "action", "upc_raw", "upc_normalized", "message"]


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
    """
    Normalize UPC scans to avoid false mismatches:
    - strip whitespace
    - keep digits only (scanners sometimes add CR/LF or other characters)
    """
    s = "" if upc_raw is None else str(upc_raw)
    s = s.strip()
    digits = re.sub(r"\D", "", s)
    return digits


def ensure_samples_csv(path: str) -> None:
    if not os.path.exists(path):
        df = pd.DataFrame(columns=SAMPLES_FIELDS)
        df.to_csv(path, index=False)


def ensure_activity_log(path: str) -> None:
    if not os.path.exists(path):
        pd.DataFrame(columns=LOG_FIELDS).to_csv(path, index=False)


def log_event(activity_log_path: str, action: str, upc_raw: str, upc_norm: str, message: str) -> None:
    """
    Best-effort logging to:
    - session_state for immediate UI
    - activity_log.csv on disk (persists locally; cloud persistence depends on hosting)
    """
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
        ensure_activity_log(activity_log_path)
        df = pd.read_csv(activity_log_path, dtype=str).fillna("")
        df = pd.concat([df, pd.DataFrame([event])], ignore_index=True)
        df.to_csv(activity_log_path, index=False)
    except Exception:
        # Do not break UX if file write fails on cloud
        pass


def load_samples(path: str) -> pd.DataFrame:
    ensure_samples_csv(path)
    df = pd.read_csv(path, dtype=str).fillna("")

    for c in SAMPLES_FIELDS:
        if c not in df.columns:
            df[c] = ""

    for col in ["tiktok_posted", "instagram_posted", "amazon_posted"]:
        df[col] = df[col].apply(normalize_bool01)

    # Normalize stored UPCs for consistency
    df["upc"] = df["upc"].astype(str).apply(normalize_upc)

    df = df[SAMPLES_FIELDS].copy()
    return df


def save_samples(df: pd.DataFrame, path: str) -> None:
    for c in SAMPLES_FIELDS:
        if c not in df.columns:
            df[c] = ""
    df = df[SAMPLES_FIELDS].copy()

    # Normalize UPCs at write-time as well
    df["upc"] = df["upc"].astype(str).apply(normalize_upc)

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


@st.cache_data
def load_catalog(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None

    df = pd.read_csv(
        path,
        sep=";",
        encoding="utf-8",
        encoding_errors="replace",
        dtype=str,
    ).fillna("")

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

    out["brand"] = out["brand"].astype(str).str.strip()
    out["perfume"] = out["perfume"].astype(str).str.strip()
    out["url"] = out["url"].astype(str).str.strip()

    out["rating_value"] = out["rating_value"].astype(str).str.replace(",", ".", regex=False).str.strip()

    out["brand_display"] = out["brand"].apply(pretty_slug)
    out["perfume_display"] = out["perfume"].apply(pretty_slug)

    out = out[(out["brand"].str.len() > 0) & (out["perfume"].str.len() > 0)].copy()
    return out


def get_upc_row_indexes(df: pd.DataFrame, upc_norm: str) -> List[int]:
    if not upc_norm:
        return []
    hits = df.index[df["upc"] == upc_norm].tolist()
    return hits


def apply_catalog_to_session(crow: Dict[str, str]) -> None:
    st.session_state["af_brand"] = pretty_slug(crow.get("brand", ""))
    st.session_state["af_product_name"] = crow.get("perfume_display", "")

    st.session_state["af_fragrance_url"] = crow.get("url", "")
    st.session_state["af_country"] = crow.get("country", "")
    st.session_state["af_gender"] = crow.get("gender", "")
    st.session_state["af_year"] = str(crow.get("year", "")).strip()

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


def set_add_form_from_existing(row: pd.Series) -> None:
    """
    Load an existing inventory record into the Add/Receive form state
    so the user can correct/update without retyping.
    """
    st.session_state["add_upc_raw"] = row.get("upc", "")
    st.session_state["add_brand"] = row.get("brand", "")
    st.session_state["add_product_name"] = row.get("product_name", "")
    st.session_state["add_variant"] = row.get("variant", "")
    st.session_state["add_batch_id"] = row.get("batch_id", "")
    st.session_state["add_shipper"] = row.get("source_shipper", "")
    st.session_state["add_handle"] = row.get("contact_handle", "")
    st.session_state["add_notes"] = row.get("notes", "")

    # Keep existing fragrance metadata in autofill state too
    st.session_state["af_fragrance_url"] = row.get("fragrance_url", "")
    st.session_state["af_country"] = row.get("country", "")
    st.session_state["af_gender"] = row.get("gender", "")
    st.session_state["af_year"] = row.get("year", "")
    st.session_state["af_top_notes"] = row.get("top_notes", "")
    st.session_state["af_middle_notes"] = row.get("middle_notes", "")
    st.session_state["af_base_notes"] = row.get("base_notes", "")
    st.session_state["af_main_accord_1"] = row.get("main_accord_1", "")
    st.session_state["af_main_accord_2"] = row.get("main_accord_2", "")
    st.session_state["af_main_accord_3"] = row.get("main_accord_3", "")
    st.session_state["af_main_accord_4"] = row.get("main_accord_4", "")
    st.session_state["af_main_accord_5"] = row.get("main_accord_5", "")
    st.session_state["af_rating_value"] = row.get("rating_value", "")
    st.session_state["af_rating_count"] = row.get("rating_count", "")
    st.session_state["af_perfumer1"] = row.get("perfumer1", "")
    st.session_state["af_perfumer2"] = row.get("perfumer2", "")


def update_existing_by_upc(
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

    # Normalize booleans + recompute status
    for col in ["tiktok_posted", "instagram_posted", "amazon_posted"]:
        df.at[idx, col] = normalize_bool01(df.at[idx, col])

    df.at[idx, "status"] = compute_status(df.loc[idx], track_amazon)
    df.at[idx, "last_updated"] = now_str()

    save_samples(df, samples_path)
    log_event(activity_log_path, "UPDATE_EXISTING", upc_raw, upc_norm, "Updated existing UPC row from Add/Receive.")
    return df


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Sample Inventory + Content Tracker", layout="wide")
st.title("Sample Inventory + Content Tracker")

with st.sidebar:
    st.subheader("Storage")
    samples_path = st.text_input("Samples CSV path", value=DEFAULT_SAMPLES_CSV)
    activity_log_path = st.text_input("Activity log CSV path", value=DEFAULT_ACTIVITY_LOG_CSV)
    st.caption("This app reads and writes to your CSV.")

    st.subheader("Platforms")
    track_amazon = st.toggle("Track Amazon postings", value=True)

    st.subheader("Catalog (Brand → Perfume Autofill)")
    catalog_path = st.text_input("Catalog CSV path", value=DEFAULT_CATALOG_CSV)
    st.caption("Uses the cleaned dataset file stored in your repo.")


samples_df = load_samples(samples_path)
catalog_df = load_catalog(catalog_path)
ensure_activity_log(activity_log_path)

# Seed activity log state from file if empty
if "activity_log" not in st.session_state:
    try:
        st.session_state["activity_log"] = pd.read_csv(activity_log_path, dtype=str).fillna("").tail(200).to_dict("records")
    except Exception:
        st.session_state["activity_log"] = []

# ----------------------------
# Quick Scan / Search
# ----------------------------
st.subheader("Quick Scan / Search")

q1, q2, q3 = st.columns([2.2, 2.2, 5.6])
with q1:
    scan_upc_raw = st.text_input(
        "Scan or paste UPC",
        value=st.session_state.get("scan_upc_raw", ""),
        placeholder="Click here, scan barcode",
        help="Your scanner types like a keyboard. Click the box then scan.",
    )
    st.session_state["scan_upc_raw"] = scan_upc_raw
    scan_upc_norm = normalize_upc(scan_upc_raw)

with q2:
    search_text = st.text_input(
        "Search inventory",
        value=st.session_state.get("search_text", ""),
        placeholder="Brand, product, shipper, handle, notes…",
    ).strip()
    st.session_state["search_text"] = search_text

with q3:
    st.caption(
        "If a scanned UPC is new, go to **Add / Receive** and it will prefill the UPC. "
        "If it exists, you will see exactly which record matched."
    )

# Find exact matches
scan_hits = get_upc_row_indexes(samples_df, scan_upc_norm)

if scan_upc_raw and not scan_upc_norm:
    st.warning("Your scan contained no digits. Try scanning again.")
elif scan_upc_norm and not scan_hits:
    st.info("UPC not found. Use **Add / Receive** to add it and associate it to a fragrance.")
elif scan_upc_norm and scan_hits:
    st.success(f"UPC found ({len(scan_hits)} match). You can edit it in **Inventory** or update it in **Content Queue**.")

# Filter view
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


tab_dash, tab_add, tab_queue, tab_inventory, tab_catalog, tab_log = st.tabs(
    ["Dashboard", "Add / Receive", "Content Queue", "Inventory", "Catalog Browser", "Activity Log"]
)

# ----------------------------
# Dashboard
# ----------------------------
with tab_dash:
    st.subheader("Dashboard")

    total = len(samples_df)
    complete = int((samples_df["status"] == STATUS_COMPLETE).sum()) if total else 0
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
    st.subheader("Recommended next actions")

    focus = samples_df[samples_df.apply(needs_posting, axis=1)].copy()
    focus = focus.sort_values(by=["received_date", "brand", "product_name"], ascending=[False, True, True])

    if len(focus) == 0:
        st.write("No items are waiting on posting.")
    else:
        st.dataframe(
            focus[["upc", "brand", "product_name", "variant", "status", "tiktok_posted", "instagram_posted", "amazon_posted", "received_date"]],
            use_container_width=True,
            hide_index=True,
        )

# ----------------------------
# Add / Receive
# ----------------------------
with tab_add:
    st.subheader("Add / Receive")
    st.caption("This is your guided flow. Pick a fragrance (autofill), scan UPC, save. If a UPC conflict happens, you will see why and can fix it without losing what you entered.")

    left, right = st.columns([1.05, 1])

    with left:
        st.markdown("### Step 1: Pick the fragrance (autofill)")

        if catalog_df is None:
            st.warning("Catalog not loaded. Confirm the file exists at: data/fra_cleaned.csv")
        else:
            brand_filter = st.text_input(
                "Brand filter (optional)",
                value=st.session_state.get("brand_filter", ""),
                placeholder="Type to narrow brands…"
            ).strip().lower()
            st.session_state["brand_filter"] = brand_filter

            brands = sorted(catalog_df["brand"].unique().tolist())
            if brand_filter:
                brands = [b for b in brands if brand_filter in b.lower()]

            selected_brand = st.selectbox(
                "Brand",
                options=brands,
                index=0 if brands else 0,
                format_func=pretty_slug,
                key="catalog_brand_select"
            )

            brand_df = catalog_df[catalog_df["brand"] == selected_brand].copy()

            perfume_filter = st.text_input(
                "Perfume filter (optional)",
                value=st.session_state.get("perfume_filter", ""),
                placeholder="Type to narrow perfumes…"
            ).strip().lower()
            st.session_state["perfume_filter"] = perfume_filter

            perfumes = brand_df["perfume_display"].tolist()
            if perfume_filter:
                perfumes = [p for p in perfumes if perfume_filter in p.lower()]

            selected_perfume_display = st.selectbox(
                "Perfume",
                options=perfumes,
                index=0 if perfumes else 0,
                key="catalog_perfume_select"
            )

            picked_row = brand_df[brand_df["perfume_display"] == selected_perfume_display].iloc[0].to_dict()

            cta1, cta2 = st.columns([1, 1])
            with cta1:
                if st.button("Use this fragrance to autofill", type="primary"):
                    apply_catalog_to_session(picked_row)
                    log_event(activity_log_path, "AUTOFILL", "", "", f"Selected {pretty_slug(picked_row.get('brand',''))} - {picked_row.get('perfume_display','')}")
                    st.success("Autofill applied. Complete Step 2 on the right.")
            with cta2:
                if st.button("Clear autofill"):
                    clear_autofill()
                    st.info("Autofill cleared.")

            with st.expander("Preview details", expanded=False):
                st.write({
                    "Brand": pretty_slug(picked_row.get("brand", "")),
                    "Perfume": picked_row.get("perfume_display", ""),
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
        st.caption("This writes to samples.csv. If the UPC conflicts, you will see the exact existing record and can update it.")

        # Prefill UPC from top scan if not found
        default_upc_raw = st.session_state.get("add_upc_raw", "")
        if scan_upc_raw and not get_upc_row_indexes(samples_df, scan_upc_norm):
            default_upc_raw = scan_upc_raw

        # Persist form entries in session_state so errors do not wipe them
        st.session_state.setdefault("add_upc_raw", default_upc_raw)
        st.session_state.setdefault("add_brand", st.session_state.get("af_brand", ""))
        st.session_state.setdefault("add_product_name", st.session_state.get("af_product_name", ""))
        st.session_state.setdefault("add_variant", "")
        st.session_state.setdefault("add_batch_id", "")
        st.session_state.setdefault("add_shipper", "")
        st.session_state.setdefault("add_handle", "")
        st.session_state.setdefault("add_notes", "")

        with st.form("add_item_form", clear_on_submit=False):
            r1, r2 = st.columns(2)

            add_upc_raw = r1.text_input("UPC (scan here)", value=st.session_state["add_upc_raw"], placeholder="Scan barcode")
            st.session_state["add_upc_raw"] = add_upc_raw
            add_upc_norm = normalize_upc(add_upc_raw)

            received = r2.date_input("Received date", value=date.today())

            r3, r4 = st.columns(2)

            # If autofill exists and user hasn't typed, keep it helpful
            if not st.session_state.get("add_brand"):
                st.session_state["add_brand"] = st.session_state.get("af_brand", "")
            if not st.session_state.get("add_product_name"):
                st.session_state["add_product_name"] = st.session_state.get("af_product_name", "")

            add_brand = r3.text_input("Brand", value=st.session_state["add_brand"])
            st.session_state["add_brand"] = add_brand

            add_name = r4.text_input("Product name", value=st.session_state["add_product_name"])
            st.session_state["add_product_name"] = add_name

            r5, r6 = st.columns(2)
            add_variant = r5.text_input("Variant (size, concentration, etc.)", value=st.session_state["add_variant"], placeholder="e.g., 2ml sample, EDP 10ml, Extrait…")
            st.session_state["add_variant"] = add_variant

            add_batch = r6.text_input("Batch ID (optional)", value=st.session_state["add_batch_id"], placeholder="e.g., 2026-03-03-A")
            st.session_state["add_batch_id"] = add_batch

            r7, r8 = st.columns(2)
            add_shipper = r7.text_input("Who shipped it (company/person)", value=st.session_state["add_shipper"], placeholder="e.g., Brand PR, VV Fragrances Wholesale")
            st.session_state["add_shipper"] = add_shipper

            add_handle = r8.text_input("Contact handle (@)", value=st.session_state["add_handle"], placeholder="@brandhandle (optional)")
            st.session_state["add_handle"] = add_handle

            add_notes = st.text_area("Notes (optional)", value=st.session_state["add_notes"], height=90, placeholder="Anything you want to remember…")
            st.session_state["add_notes"] = add_notes

            submitted = st.form_submit_button("Save to inventory", type="primary")

        # After form block: validate + handle conflicts without losing form state
        if submitted:
            if not add_upc_norm:
                log_event(activity_log_path, "ADD_FAIL", add_upc_raw, add_upc_norm, "UPC missing or contained no digits.")
                st.error("UPC is required (must include digits).")
            else:
                hits = get_upc_row_indexes(samples_df, add_upc_norm)
                if hits:
                    # Conflict: show why, show matching records, allow fix/update
                    log_event(activity_log_path, "UPC_CONFLICT", add_upc_raw, add_upc_norm, f"UPC matched {len(hits)} existing row(s).")
                    st.error("That UPC already exists in your inventory.")

                    with st.expander("Show matching record(s) and debug info", expanded=True):
                        st.write({
                            "Your scan (raw)": add_upc_raw,
                            "Normalized UPC (digits only)": add_upc_norm,
                            "Matches found": len(hits),
                        })

                        match_df = samples_df.iloc[hits][[
                            "upc", "brand", "product_name", "variant", "received_date", "source_shipper", "status", "last_updated"
                        ]].copy()
                        st.dataframe(match_df, use_container_width=True, hide_index=True)

                        # Near matches can explain "I swear it doesn't exist"
                        near = samples_df[samples_df["upc"].str.contains(add_upc_norm[:6], na=False)].head(20)
                        if len(near) > 0:
                            st.caption("Possible near matches (shares first 6 digits):")
                            st.dataframe(
                                near[["upc", "brand", "product_name", "variant", "received_date"]].head(10),
                                use_container_width=True,
                                hide_index=True
                            )

                    fix1, fix2, fix3 = st.columns([1.3, 1.7, 2.0])

                    with fix1:
                        if st.button("Load existing into this form"):
                            # Load first matching record into the form without losing it
                            set_add_form_from_existing(samples_df.iloc[hits[0]])
                            st.info("Loaded the existing record into the form. Adjust fields and then use Inventory to save edits, or use Update Existing below.")
                            st.rerun()

                    with fix2:
                        if st.button("Update existing with current form values", type="primary"):
                            # Update only the 'safe' fields + fragrance metadata association
                            updates = {
                                "brand": add_brand.strip(),
                                "product_name": add_name.strip(),
                                "variant": add_variant.strip(),
                                "source_shipper": add_shipper.strip(),
                                "contact_handle": add_handle.strip(),
                                "batch_id": add_batch.strip(),
                                "received_date": str(received),
                                "notes": add_notes.strip(),

                                # (re)associate fragrance metadata from current autofill
                                "fragrance_url": st.session_state.get("af_fragrance_url", ""),
                                "country": st.session_state.get("af_country", ""),
                                "gender": st.session_state.get("af_gender", ""),
                                "year": st.session_state.get("af_year", ""),
                                "top_notes": st.session_state.get("af_top_notes", ""),
                                "middle_notes": st.session_state.get("af_middle_notes", ""),
                                "base_notes": st.session_state.get("af_base_notes", ""),
                                "main_accord_1": st.session_state.get("af_main_accord_1", ""),
                                "main_accord_2": st.session_state.get("af_main_accord_2", ""),
                                "main_accord_3": st.session_state.get("af_main_accord_3", ""),
                                "main_accord_4": st.session_state.get("af_main_accord_4", ""),
                                "main_accord_5": st.session_state.get("af_main_accord_5", ""),
                                "rating_value": st.session_state.get("af_rating_value", ""),
                                "rating_count": st.session_state.get("af_rating_count", ""),
                                "perfumer1": st.session_state.get("af_perfumer1", ""),
                                "perfumer2": st.session_state.get("af_perfumer2", ""),
                            }

                            try:
                                samples_df = update_existing_by_upc(
                                    samples_df, samples_path, activity_log_path,
                                    add_upc_raw, updates, track_amazon
                                )
                                st.success("Updated the existing UPC record (and kept your form data).")
                            except Exception as e:
                                st.error(str(e))

                    with fix3:
                        st.caption("If your scanner is adding extra characters, the debug section shows what the app normalized to. Only digits are used for matching.")

                else:
                    # Create new record
                    row = {k: "" for k in SAMPLES_FIELDS}

                    row["upc"] = add_upc_norm
                    row["brand"] = add_brand.strip()
                    row["product_name"] = add_name.strip()
                    row["variant"] = add_variant.strip()

                    row["source_shipper"] = add_shipper.strip()
                    row["contact_handle"] = add_handle.strip()
                    row["received_date"] = str(received)
                    row["batch_id"] = add_batch.strip()

                    row["status"] = STATUS_NEW
                    row["tiktok_posted"] = "0"
                    row["instagram_posted"] = "0"
                    row["amazon_posted"] = "0"

                    # catalog autofill association
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

                    st.success("Saved. Your inventory CSV has been updated.")

                    # Clear the form only on success (not on errors)
                    st.session_state["add_upc_raw"] = ""
                    st.session_state["add_brand"] = st.session_state.get("af_brand", "")
                    st.session_state["add_product_name"] = st.session_state.get("af_product_name", "")
                    st.session_state["add_variant"] = ""
                    st.session_state["add_batch_id"] = ""
                    st.session_state["add_shipper"] = ""
                    st.session_state["add_handle"] = ""
                    st.session_state["add_notes"] = ""

                    st.rerun()

        # Manual reset button (safe)
        if st.button("Reset form (does not delete inventory)"):
            st.session_state["add_upc_raw"] = ""
            st.session_state["add_brand"] = st.session_state.get("af_brand", "")
            st.session_state["add_product_name"] = st.session_state.get("af_product_name", "")
            st.session_state["add_variant"] = ""
            st.session_state["add_batch_id"] = ""
            st.session_state["add_shipper"] = ""
            st.session_state["add_handle"] = ""
            st.session_state["add_notes"] = ""
            st.info("Form reset.")
            st.rerun()

# ----------------------------
# Content Queue
# ----------------------------
with tab_queue:
    st.subheader("Content Queue")

    def missing_platforms(row: pd.Series) -> str:
        missing = []
        if row.get("tiktok_posted", "0") != "1":
            missing.append("TikTok")
        if row.get("instagram_posted", "0") != "1":
            missing.append("Instagram")
        if track_amazon and row.get("amazon_posted", "0") != "1":
            missing.append("Amazon")
        return ", ".join(missing)

    queue_df = samples_df[samples_df["status"] != STATUS_COMPLETE].copy()
    queue_df["missing"] = queue_df.apply(missing_platforms, axis=1)

    f1, f2, f3 = st.columns([2, 2, 6])
    with f1:
        status_filter = st.multiselect("Status", options=ALL_STATUSES, default=[STATUS_NEW, STATUS_FILMED, STATUS_POSTED])
    with f2:
        brand_contains = st.text_input("Brand contains", value="")
    with f3:
        st.caption("Quick Actions update your CSV immediately.")

    if status_filter:
        queue_df = queue_df[queue_df["status"].isin(status_filter)]
    if brand_contains.strip():
        queue_df = queue_df[queue_df["brand"].str.lower().str.contains(brand_contains.strip().lower(), na=False)]

    st.dataframe(
        queue_df[["upc", "brand", "product_name", "variant", "status", "missing", "received_date", "source_shipper"]],
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.subheader("Quick Actions")

    qa_upc_raw = st.text_input("UPC for quick actions", value=scan_upc_raw, placeholder="Scan barcode")
    qa_upc_norm = normalize_upc(qa_upc_raw)

    def quick_update(updates: Dict[str, str], action_name: str):
        nonlocal_df = None  # just to make intent obvious; not used
        try:
            samples_df_local = samples_df.copy()
            hits = get_upc_row_indexes(samples_df_local, qa_upc_norm)
            if not qa_upc_norm:
                raise ValueError("UPC is required.")
            if not hits:
                raise KeyError("UPC not found.")
            idx = hits[0]
            for k, v in updates.items():
                if k in samples_df_local.columns:
                    samples_df_local.at[idx, k] = v

            for col in ["tiktok_posted", "instagram_posted", "amazon_posted"]:
                samples_df_local.at[idx, col] = normalize_bool01(samples_df_local.at[idx, col])

            samples_df_local.at[idx, "status"] = compute_status(samples_df_local.loc[idx], track_amazon)
            samples_df_local.at[idx, "last_updated"] = now_str()

            save_samples(samples_df_local, samples_path)
            log_event(activity_log_path, action_name, qa_upc_raw, qa_upc_norm, "Quick action applied.")
            st.success("Updated and saved.")
            st.rerun()
        except Exception as e:
            log_event(activity_log_path, "QUICK_ACTION_FAIL", qa_upc_raw, qa_upc_norm, str(e))
            st.error(str(e))

    b1, b2, b3, b4, b5 = st.columns([1.2, 1.0, 1.4, 1.6, 3.0])
    with b1:
        if st.button("Mark FILMED", type="primary"):
            quick_update({"status": STATUS_FILMED}, "MARK_FILMED")
    with b2:
        if st.button("Mark NEW"):
            quick_update({"status": STATUS_NEW}, "MARK_NEW")
    with b3:
        if st.button("TikTok POSTED"):
            quick_update({"tiktok_posted": "1"}, "TIKTOK_POSTED")
    with b4:
        if st.button("Instagram POSTED"):
            quick_update({"instagram_posted": "1"}, "INSTAGRAM_POSTED")
    with b5:
        if track_amazon:
            if st.button("Amazon POSTED"):
                quick_update({"amazon_posted": "1"}, "AMAZON_POSTED")
        else:
            st.caption("Amazon tracking is turned off in the sidebar.")

# ----------------------------
# Inventory
# ----------------------------
with tab_inventory:
    st.subheader("Inventory")
    st.caption("Edit directly, then save. UPCs are normalized to digits-only.")

    view_df = filtered_df.copy()

    if scan_upc_norm and scan_hits:
        view_df = samples_df.iloc[scan_hits].copy()

    edited = st.data_editor(
        view_df,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
    )

    c1, c2 = st.columns([1.2, 8.8])
    with c1:
        if st.button("Save changes", type="primary"):
            updated = samples_df.copy()

            # Build map by normalized UPC
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

            # Normalize and recompute
            updated["upc"] = updated["upc"].astype(str).apply(normalize_upc)
            for i in range(len(updated)):
                updated.at[i, "tiktok_posted"] = normalize_bool01(updated.at[i, "tiktok_posted"])
                updated.at[i, "instagram_posted"] = normalize_bool01(updated.at[i, "instagram_posted"])
                updated.at[i, "amazon_posted"] = normalize_bool01(updated.at[i, "amazon_posted"])
                updated.at[i, "status"] = compute_status(updated.loc[i], track_amazon)
                updated.at[i, "last_updated"] = now_str()

            samples_df = updated
            save_samples(samples_df, samples_path)
            log_event(activity_log_path, "INVENTORY_SAVE", "", "", "Saved edits from Inventory.")
            st.success("Saved to CSV.")
            st.rerun()

    with c2:
        st.download_button(
            label="Download current samples.csv",
            data=samples_df.to_csv(index=False).encode("utf-8"),
            file_name=os.path.basename(samples_path),
            mime="text/csv",
        )

# ----------------------------
# Catalog Browser
# ----------------------------
with tab_catalog:
    st.subheader("Catalog Browser")
    st.caption("Browse the catalog and confirm details. Use Add / Receive to apply autofill and save an item.")

    if catalog_df is None:
        st.warning("Catalog not loaded.")
    else:
        c1, c2, c3 = st.columns([2.5, 2.5, 5.0])
        with c1:
            bfilter = st.text_input("Brand filter", value="").strip().lower()
        with c2:
            pfilter = st.text_input("Perfume filter", value="").strip().lower()
        with c3:
            st.caption("Catalog is loaded from your dataset file, not from a live website connection.")

        cat_view = catalog_df.copy()
        if bfilter:
            cat_view = cat_view[cat_view["brand"].str.lower().str.contains(bfilter, na=False)]
        if pfilter:
            cat_view = cat_view[cat_view["perfume_display"].str.lower().str.contains(pfilter, na=False)]

        st.dataframe(
            cat_view[[
                "brand_display",
                "perfume_display",
                "year",
                "gender",
                "country",
                "main_accord_1",
                "main_accord_2",
                "main_accord_3",
                "main_accord_4",
                "main_accord_5",
                "rating_value",
                "rating_count",
                "perfumer1",
                "perfumer2",
                "url",
            ]].rename(columns={
                "brand_display": "Brand",
                "perfume_display": "Perfume",
                "main_accord_1": "Accord 1",
                "main_accord_2": "Accord 2",
                "main_accord_3": "Accord 3",
                "main_accord_4": "Accord 4",
                "main_accord_5": "Accord 5",
                "rating_value": "Rating",
                "rating_count": "Ratings",
                "perfumer1": "Perfumer 1",
                "perfumer2": "Perfumer 2",
                "url": "URL",
            }),
            use_container_width=True,
            hide_index=True,
        )

# ----------------------------
# Activity Log
# ----------------------------
with tab_log:
    st.subheader("Activity Log")
    st.caption("This is a simple log of actions and conflicts. It also attempts to write to activity_log.csv.")

    log_rows = st.session_state.get("activity_log", [])
    if not log_rows:
        st.write("No activity logged yet.")
    else:
        df_log = pd.DataFrame(log_rows)
        df_log = df_log.tail(200).iloc[::-1]  # newest first
        st.dataframe(df_log, use_container_width=True, hide_index=True)

    st.download_button(
        label="Download activity_log.csv",
        data=pd.DataFrame(log_rows).to_csv(index=False).encode("utf-8"),
        file_name=os.path.basename(activity_log_path),
        mime="text/csv",
    )

st.caption(
    "Note: On Streamlit Community Cloud, file writes may not persist after restarts. "
    "If you want permanent cloud persistence, the best practice is saving samples.csv and activity_log.csv back to GitHub on each save."
)