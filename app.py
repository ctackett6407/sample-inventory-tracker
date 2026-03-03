# app.py
# Sample Inventory + Content Tracker (CSV-backed) with Fragrance Catalog Autofill (fra_cleaned.csv)

import os
from datetime import datetime, date
from typing import Optional, Dict

import pandas as pd
import streamlit as st


# ----------------------------
# Configuration
# ----------------------------
DEFAULT_SAMPLES_CSV = "samples.csv"
DEFAULT_CATALOG_CSV = "data/fra_cleaned.csv"

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


# ----------------------------
# Helpers
# ----------------------------
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def pretty_slug(s: str) -> str:
    # Dataset often uses hyphenated slugs; make it human-friendly.
    return str(s).replace("-", " ").strip().title()


def ensure_samples_csv(path: str) -> None:
    if not os.path.exists(path):
        df = pd.DataFrame(columns=SAMPLES_FIELDS)
        df.to_csv(path, index=False)


def normalize_bool01(val: str) -> str:
    v = str(val).strip()
    return "1" if v == "1" else "0"


def load_samples(path: str) -> pd.DataFrame:
    ensure_samples_csv(path)
    df = pd.read_csv(path, dtype=str).fillna("")

    # Ensure all columns exist
    for c in SAMPLES_FIELDS:
        if c not in df.columns:
            df[c] = ""

    # Normalize boolean fields
    for col in ["tiktok_posted", "instagram_posted", "amazon_posted"]:
        df[col] = df[col].apply(normalize_bool01)

    # Clean UPC whitespace
    df["upc"] = df["upc"].astype(str).str.strip()

    # Keep consistent column order
    df = df[SAMPLES_FIELDS].copy()
    return df


def save_samples(df: pd.DataFrame, path: str) -> None:
    for c in SAMPLES_FIELDS:
        if c not in df.columns:
            df[c] = ""
    df = df[SAMPLES_FIELDS].copy()
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
    """
    Loads the cleaned Fragrantica-derived dataset (semicolon-delimited).
    We normalize column names to a predictable internal schema.
    """
    if not os.path.exists(path):
        return None

    df = pd.read_csv(
        path,
        sep=";",
        encoding="utf-8",
        encoding_errors="replace",
        dtype=str,
    ).fillna("")

    # Normalize column names (case-insensitive and space-tolerant)
    cols = {c: c.strip() for c in df.columns}
    df.rename(columns=cols, inplace=True)

    # Build a mapping from known dataset columns to internal standardized columns
    # Dataset columns as described by you:
    # URL, Perfume, Brand, Country, Gender, Rating Value, Rating Count, Year,
    # Top Notes, Middle Notes, Base Notes, Perfumer1, Perfumer2, Main Accord 1-5
    # Some cleaned files use: Top, Middle, Base, mainaccord1..5, url
    colmap = {}

    def find_col(*candidates):
        existing = {c.lower(): c for c in df.columns}
        for cand in candidates:
            key = cand.lower()
            if key in existing:
                return existing[key]
        return None

    colmap["url"] = find_col("URL", "url")
    colmap["perfume"] = find_col("Perfume", "perfume")
    colmap["brand"] = find_col("Brand", "brand")
    colmap["country"] = find_col("Country", "country")
    colmap["gender"] = find_col("Gender", "gender")
    colmap["rating_value"] = find_col("Rating Value", "rating value", "rating_value")
    colmap["rating_count"] = find_col("Rating Count", "rating count", "rating_count")
    colmap["year"] = find_col("Year", "year")

    # Notes
    colmap["top_notes"] = find_col("Top Notes", "Top", "top")
    colmap["middle_notes"] = find_col("Middle Notes", "Middle", "middle")
    colmap["base_notes"] = find_col("Base Notes", "Base", "base")

    # Perfumers
    colmap["perfumer1"] = find_col("Perfumer1", "perfumer1", "Perfumer 1")
    colmap["perfumer2"] = find_col("Perfumer2", "perfumer2", "Perfumer 2")

    # Accords
    colmap["main_accord_1"] = find_col("Main Accord 1", "mainaccord1", "main_accord_1")
    colmap["main_accord_2"] = find_col("Main Accord 2", "mainaccord2", "main_accord_2")
    colmap["main_accord_3"] = find_col("Main Accord 3", "mainaccord3", "main_accord_3")
    colmap["main_accord_4"] = find_col("Main Accord 4", "mainaccord4", "main_accord_4")
    colmap["main_accord_5"] = find_col("Main Accord 5", "mainaccord5", "main_accord_5")

    # Create standardized columns in a clean catalog dataframe
    out = pd.DataFrame()
    for k, src in colmap.items():
        out[k] = df[src] if src else ""

    # Clean key text columns
    out["brand"] = out["brand"].astype(str).str.strip()
    out["perfume"] = out["perfume"].astype(str).str.strip()
    out["url"] = out["url"].astype(str).str.strip()

    # Normalize rating decimals like "4,21" -> "4.21"
    out["rating_value"] = out["rating_value"].astype(str).str.replace(",", ".", regex=False).str.strip()

    # Add display-friendly names
    out["brand_display"] = out["brand"].apply(pretty_slug)
    out["perfume_display"] = out["perfume"].apply(pretty_slug)

    # Drop rows without a brand or perfume
    out = out[(out["brand"].str.len() > 0) & (out["perfume"].str.len() > 0)].copy()

    return out


def get_upc_row_index(df: pd.DataFrame, upc: str) -> Optional[int]:
    upc = (upc or "").strip()
    if not upc:
        return None
    hits = df.index[df["upc"] == upc].tolist()
    return hits[0] if hits else None


def apply_catalog_to_session(crow: Dict[str, str]) -> None:
    """
    Stores catalog-selected fields in session_state so the Add form can autofill.
    """
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
    keys = [k for k in st.session_state.keys() if k.startswith("af_")]
    for k in keys:
        del st.session_state[k]


# ----------------------------
# UI
# ----------------------------
st.set_page_config(
    page_title="Sample Inventory + Content Tracker",
    layout="wide",
)

st.title("Sample Inventory + Content Tracker")

with st.sidebar:
    st.subheader("Storage")
    samples_path = st.text_input("Samples CSV path", value=DEFAULT_SAMPLES_CSV)
    st.caption("This app reads and writes to your CSV.")

    st.subheader("Platforms")
    track_amazon = st.toggle("Track Amazon postings", value=True)

    st.subheader("Catalog (Brand → Perfume Autofill)")
    catalog_path = st.text_input("Catalog CSV path", value=DEFAULT_CATALOG_CSV)
    st.caption("Uses the cleaned dataset file you added to your repo.")

samples_df = load_samples(samples_path)
catalog_df = load_catalog(catalog_path)

# Top quick access: scan/search
st.subheader("Quick Scan / Search")

q1, q2, q3 = st.columns([2.2, 2.2, 5.6])
with q1:
    scan_upc = st.text_input(
        "Scan or paste UPC",
        value="",
        placeholder="Click here, scan barcode",
        help="Your scanner types like a keyboard. Click the box then scan.",
    ).strip()

with q2:
    search_text = st.text_input(
        "Search inventory",
        value="",
        placeholder="Brand, product, shipper, handle, notes…",
    ).strip()

with q3:
    st.caption(
        "Tip: If a scanned UPC is new, go to **Add / Receive** and it will prefill the UPC. "
        "If it already exists, the Inventory tab will show it immediately."
    )

# Build a filtered view
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

selected_index = get_upc_row_index(samples_df, scan_upc)
if scan_upc and selected_index is None:
    st.info("UPC not found yet. Use **Add / Receive** to create it and associate it to a catalog fragrance.")
elif scan_upc and selected_index is not None:
    st.success("UPC found. You can edit it in **Inventory** or update status in **Content Queue**.")


# Tabs for customer-friendly layout
tab_dash, tab_add, tab_queue, tab_inventory, tab_catalog = st.tabs(
    ["Dashboard", "Add / Receive", "Content Queue", "Inventory", "Catalog Browser"]
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

    st.subheader("What should you work on next?")
    focus = samples_df[samples_df.apply(needs_posting, axis=1)].copy()
    focus = focus.sort_values(by=["received_date", "brand", "product_name"], ascending=[False, True, True])
    if len(focus) == 0:
        st.write("No items are currently waiting on posting. Nice.")
    else:
        st.dataframe(
            focus[["upc", "brand", "product_name", "variant", "status", "tiktok_posted", "instagram_posted", "amazon_posted", "received_date"]],
            use_container_width=True,
            hide_index=True,
        )


# ----------------------------
# Add / Receive (catalog autofill + UPC association)
# ----------------------------
with tab_add:
    st.subheader("Receive a new sample and associate it to a fragrance")

    left, right = st.columns([1.05, 1])

    with left:
        st.markdown("### 1) Pick the fragrance (autofill)")

        if catalog_df is None:
            st.warning("Catalog not loaded. Confirm the file exists at: data/fra_cleaned.csv")
        else:
            brand_filter = st.text_input("Brand filter (optional)", value="", placeholder="Type to narrow brands…").strip().lower()

            brands = sorted(catalog_df["brand"].unique().tolist())
            if brand_filter:
                brands = [b for b in brands if brand_filter in b.lower()]

            selected_brand = st.selectbox("Brand", options=brands, format_func=pretty_slug)

            brand_df = catalog_df[catalog_df["brand"] == selected_brand].copy()

            perfume_filter = st.text_input("Perfume filter (optional)", value="", placeholder="Type to narrow perfumes…").strip().lower()
            perfumes = brand_df["perfume_display"].tolist()
            if perfume_filter:
                perfumes = [p for p in perfumes if perfume_filter in p.lower()]

            selected_perfume_display = st.selectbox("Perfume", options=perfumes)

            picked_row = brand_df[brand_df["perfume_display"] == selected_perfume_display].iloc[0].to_dict()

            cta1, cta2 = st.columns([1, 1])
            with cta1:
                if st.button("Use this fragrance to autofill the form", type="primary"):
                    apply_catalog_to_session(picked_row)
                    st.success("Autofill applied. Complete Step 2 on the right.")
            with cta2:
                if st.button("Clear autofill"):
                    clear_autofill()
                    st.info("Autofill cleared.")

            with st.expander("Preview autofill details", expanded=False):
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
        st.markdown("### 2) Scan UPC and save it to your inventory")
        st.caption("This writes a new row into samples.csv and ties the UPC to the selected fragrance details.")

        # Prefill UPC if user scanned something at the top and it wasn't found
        upc_prefill = ""
        if scan_upc and get_upc_row_index(samples_df, scan_upc) is None:
            upc_prefill = scan_upc

        with st.form("add_item_form", clear_on_submit=True):
            r1, r2 = st.columns(2)

            new_upc = r1.text_input("UPC (scan here)", value=upc_prefill, placeholder="Scan barcode").strip()
            received = r2.date_input("Received date", value=date.today())

            r3, r4 = st.columns(2)
            brand_val = st.session_state.get("af_brand", "")
            name_val = st.session_state.get("af_product_name", "")

            new_brand = r3.text_input("Brand", value=brand_val)
            new_name = r4.text_input("Product name", value=name_val)

            r5, r6 = st.columns(2)
            new_variant = r5.text_input("Variant (size, concentration, etc.)", value="", placeholder="e.g., 2ml sample, EDP 10ml, Extrait…")
            new_batch = r6.text_input("Batch ID (optional)", value="", placeholder="e.g., 2026-03-03-A")

            r7, r8 = st.columns(2)
            new_shipper = r7.text_input("Who shipped it (company/person)", value="", placeholder="e.g., Brand PR, VV Fragrances Wholesale")
            new_handle = r8.text_input("Contact handle (@)", value="", placeholder="@brandhandle (optional)")

            new_notes = st.text_area("Notes (optional)", value="", height=90, placeholder="Anything you want to remember…")

            submitted = st.form_submit_button("Save to inventory", type="primary")

            if submitted:
                if not new_upc:
                    st.error("UPC is required.")
                elif get_upc_row_index(samples_df, new_upc) is not None:
                    st.error("That UPC already exists. Edit it in the Inventory tab.")
                else:
                    row = {k: "" for k in SAMPLES_FIELDS}

                    # basics
                    row["upc"] = new_upc
                    row["brand"] = new_brand.strip()
                    row["product_name"] = new_name.strip()
                    row["variant"] = new_variant.strip()

                    # relationship/shipping
                    row["source_shipper"] = new_shipper.strip()
                    row["contact_handle"] = new_handle.strip()
                    row["received_date"] = str(received)
                    row["batch_id"] = new_batch.strip()

                    # workflow defaults
                    row["status"] = STATUS_NEW
                    row["tiktok_posted"] = "0"
                    row["instagram_posted"] = "0"
                    row["amazon_posted"] = "0"

                    # catalog autofill (if available)
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

                    # misc
                    row["notes"] = new_notes.strip()
                    row["last_updated"] = now_str()

                    # compute status
                    row["status"] = compute_status(pd.Series(row), track_amazon)

                    samples_df = pd.concat([samples_df, pd.DataFrame([row])], ignore_index=True)
                    save_samples(samples_df, samples_path)
                    st.success("Saved. Your inventory CSV has been updated.")


# ----------------------------
# Content Queue (best-practice: work list + quick actions)
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
        st.caption("Use the quick actions below to update filming and posting without digging through the table.")

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
    st.caption("Scan/paste a UPC, then click actions. This updates the CSV immediately.")

    qa1, qa2 = st.columns([2, 8])
    with qa1:
        qa_upc = st.text_input("UPC for quick actions", value=scan_upc.strip(), placeholder="Scan barcode").strip()

    def update_by_upc(upc: str, updates: Dict[str, str]) -> None:
        nonlocal samples_df
        idx = get_upc_row_index(samples_df, upc)
        if idx is None:
            st.error("UPC not found in inventory.")
            return

        for k, v in updates.items():
            samples_df.at[idx, k] = v

        samples_df.at[idx, "status"] = compute_status(samples_df.loc[idx], track_amazon)
        samples_df.at[idx, "last_updated"] = now_str()
        save_samples(samples_df, samples_path)
        st.success("Updated and saved to CSV.")

    b1, b2, b3, b4, b5 = st.columns([1.2, 1.2, 1.4, 1.4, 3.0])

    with b1:
        if st.button("Mark FILMED", type="primary"):
            update_by_upc(qa_upc, {"status": STATUS_FILMED})

    with b2:
        if st.button("Mark NEW"):
            update_by_upc(qa_upc, {"status": STATUS_NEW})

    with b3:
        if st.button("TikTok POSTED"):
            update_by_upc(qa_upc, {"tiktok_posted": "1"})

    with b4:
        if st.button("Instagram POSTED"):
            update_by_upc(qa_upc, {"instagram_posted": "1"})

    with b5:
        if track_amazon:
            if st.button("Amazon POSTED"):
                update_by_upc(qa_upc, {"amazon_posted": "1"})
        else:
            st.caption("Amazon tracking is turned off in the sidebar.")

    st.divider()
    st.subheader("Add links (optional)")
    st.caption("Paste URLs after posting so you can reference them later.")
    link_upc = st.text_input("UPC to add links", value=qa_upc).strip()
    l1, l2, l3 = st.columns(3)
    with l1:
        tt_url = st.text_input("TikTok URL", value="")
    with l2:
        ig_url = st.text_input("Instagram URL", value="")
    with l3:
        amz_url = st.text_input("Amazon URL", value="") if track_amazon else ""

    if st.button("Save links"):
        idx = get_upc_row_index(samples_df, link_upc)
        if idx is None:
            st.error("UPC not found.")
        else:
            updates = {}
            if tt_url.strip():
                updates["tiktok_url"] = tt_url.strip()
            if ig_url.strip():
                updates["instagram_url"] = ig_url.strip()
            if track_amazon and amz_url.strip():
                updates["amazon_url"] = amz_url.strip()
            if updates:
                update_by_upc(link_upc, updates)
            else:
                st.info("Nothing to save.")


# ----------------------------
# Inventory (edit in place)
# ----------------------------
with tab_inventory:
    st.subheader("Inventory")
    st.caption("Edit directly, then save. Use search/scan at the top to narrow what you see.")

    view_df = filtered_df.copy()

    # If a UPC was scanned and exists, show only that row for focused editing
    if scan_upc and selected_index is not None:
        view_df = samples_df.iloc[[selected_index]].copy()

    edited = st.data_editor(
        view_df,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
    )

    c1, c2 = st.columns([1.2, 8.8])
    with c1:
        if st.button("Save changes", type="primary"):
            # Merge edits back by UPC
            updated = samples_df.copy()
            edited_map = {r["upc"]: r for _, r in edited.iterrows() if str(r.get("upc", "")).strip()}

            for i in range(len(updated)):
                u = updated.at[i, "upc"]
                if u in edited_map:
                    for col in SAMPLES_FIELDS:
                        val = edited_map[u].get(col, "")
                        updated.at[i, col] = "" if pd.isna(val) else str(val)

            # Normalize + recompute status
            for i in range(len(updated)):
                updated.at[i, "tiktok_posted"] = normalize_bool01(updated.at[i, "tiktok_posted"])
                updated.at[i, "instagram_posted"] = normalize_bool01(updated.at[i, "instagram_posted"])
                updated.at[i, "amazon_posted"] = normalize_bool01(updated.at[i, "amazon_posted"])
                updated.at[i, "status"] = compute_status(updated.loc[i], track_amazon)
                updated.at[i, "last_updated"] = now_str()

            samples_df = updated
            save_samples(samples_df, samples_path)
            st.success("Saved to CSV.")

    with c2:
        st.download_button(
            label="Download current samples.csv",
            data=samples_df.to_csv(index=False).encode("utf-8"),
            file_name=os.path.basename(samples_path),
            mime="text/csv",
        )


# ----------------------------
# Catalog Browser (optional but helpful for discovery)
# ----------------------------
with tab_catalog:
    st.subheader("Catalog Browser")
    st.caption("Browse the catalog and copy details, or use the Add / Receive tab to apply autofill directly.")

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

st.caption(
    "Note: Your app is deployed on Streamlit Community Cloud. If you want your CSV changes to persist permanently in the cloud, "
    "the best practice is to save the updated CSV back to GitHub on each save. Tell me and I’ll implement that next."
)