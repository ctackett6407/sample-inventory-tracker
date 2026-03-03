import os
from datetime import datetime, date
import pandas as pd
import streamlit as st

DEFAULT_CSV = "samples.csv"

FIELDS = [
    "upc",
    "brand",
    "product_name",
    "variant",
    "source_shipper",
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
    "notes",
    "last_updated",
]

STATUS_NEW = "NEW"
STATUS_FILMED = "FILMED"
STATUS_POSTED = "POSTED"
STATUS_COMPLETE = "COMPLETE"
ALL_STATUSES = [STATUS_NEW, STATUS_FILMED, STATUS_POSTED, STATUS_COMPLETE]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_csv(path: str) -> None:
    if not os.path.exists(path):
        df = pd.DataFrame(columns=FIELDS)
        df.to_csv(path, index=False)


def load_df(path: str) -> pd.DataFrame:
    ensure_csv(path)
    df = pd.read_csv(path, dtype=str).fillna("")
    # normalize yes/no fields
    for col in ["tiktok_posted", "instagram_posted", "amazon_posted"]:
        if col not in df.columns:
            df[col] = "0"
        df[col] = df[col].replace({"": "0"}).apply(lambda x: "1" if str(x).strip() == "1" else "0")

    # ensure all columns exist
    for c in FIELDS:
        if c not in df.columns:
            df[c] = ""

    # basic cleanup
    df["upc"] = df["upc"].astype(str).str.strip()
    df = df[FIELDS]
    return df


def save_df(df: pd.DataFrame, path: str) -> None:
    # keep columns consistent
    for c in FIELDS:
        if c not in df.columns:
            df[c] = ""
    df = df[FIELDS].copy()
    df.to_csv(path, index=False)


def compute_status(row: pd.Series, track_amazon: bool) -> str:
    tt = row.get("tiktok_posted", "0") == "1"
    ig = row.get("instagram_posted", "0") == "1"
    am = row.get("amazon_posted", "0") == "1" if track_amazon else True

    any_posted = tt or ig or (am if track_amazon else False)
    all_posted = tt and ig and am

    if all_posted:
        return STATUS_COMPLETE
    if any_posted:
        return STATUS_POSTED

    current = row.get("status", "").strip()
    if current == STATUS_FILMED:
        return STATUS_FILMED
    return STATUS_NEW


def upc_index(df: pd.DataFrame) -> dict:
    return {u: i for i, u in enumerate(df["upc"].tolist()) if u}


st.set_page_config(page_title="Sample Inventory + Content Tracker", layout="wide")

st.title("Sample Inventory + Content Tracker")

with st.sidebar:
    st.subheader("Settings")
    csv_path = st.text_input("CSV path", value=DEFAULT_CSV)
    track_amazon = st.toggle("Track Amazon postings", value=True)
    st.caption("All changes are written back to the CSV you select here.")

df = load_df(csv_path)

# --- Top: Scan / Search ---
st.subheader("Scan or Search")
colA, colB, colC = st.columns([2, 2, 3])

with colA:
    scan_upc = st.text_input("Scan/paste UPC here", value="", placeholder="Click here, then scan barcode")
with colB:
    search_text = st.text_input("Search (brand/product/shipper/handle)", value="")
with colC:
    st.write("")
    st.write("")
    st.caption("Tip: your scanner types like a keyboard. Click the UPC box and scan.")

# --- Filters ---
idx_map = upc_index(df)

filtered = df.copy()
if search_text.strip():
    q = search_text.strip().lower()
    hay = (
        filtered["upc"].astype(str)
        + " " + filtered["brand"].astype(str)
        + " " + filtered["product_name"].astype(str)
        + " " + filtered["variant"].astype(str)
        + " " + filtered["source_shipper"].astype(str)
        + " " + filtered["contact_handle"].astype(str)
        + " " + filtered["notes"].astype(str)
    ).str.lower()
    filtered = filtered[hay.str.contains(q, na=False)]

# If UPC scanned, narrow hard to that item (if exists)
selected_row = None
if scan_upc.strip():
    s = scan_upc.strip()
    if s in idx_map:
        selected_row = idx_map[s]
        filtered = df.iloc[[selected_row]].copy()
    else:
        st.warning("UPC not found. Use the 'Add New Sample' form below to create it.")

# --- Dashboard metrics ---
st.subheader("Dashboard")
m1, m2, m3, m4 = st.columns(4)

def is_complete(r):
    return r.get("status","") == STATUS_COMPLETE

def needs_film(r):
    return r.get("status","") in [STATUS_NEW]

def needs_post(r):
    # FILMED or POSTED but missing any required platforms
    if r.get("status","") not in [STATUS_FILMED, STATUS_POSTED]:
        return False
    tt = r.get("tiktok_posted","0") == "1"
    ig = r.get("instagram_posted","0") == "1"
    am = (r.get("amazon_posted","0") == "1") if track_amazon else True
    return not (tt and ig and am)

total = len(df)
open_items = (df["status"] != STATUS_COMPLETE).sum() if total else 0
to_film = (df["status"] == STATUS_NEW).sum() if total else 0

if track_amazon:
    missing_any = df.apply(lambda r: needs_post(r), axis=1).sum() if total else 0
else:
    missing_any = df.apply(lambda r: needs_post(r), axis=1).sum() if total else 0

complete = (df["status"] == STATUS_COMPLETE).sum() if total else 0

m1.metric("Total items", total)
m2.metric("Open (not complete)", int(open_items))
m3.metric("Needs filming", int(to_film))
m4.metric("Needs posting", int(missing_any))

st.divider()

# --- Add new sample ---
st.subheader("Add New Sample (100% through the interface)")
with st.form("add_form", clear_on_submit=True):
    c1, c2, c3, c4 = st.columns(4)
    new_upc = c1.text_input("UPC (scan here)", value="")
    new_brand = c2.text_input("Brand", value="")
    new_name = c3.text_input("Product name", value="")
    new_variant = c4.text_input("Variant", value="")

    c5, c6, c7, c8 = st.columns(4)
    new_shipper = c5.text_input("Who shipped it (company/person)", value="")
    new_handle = c6.text_input("Contact handle (@)", value="")
    new_received = c7.date_input("Received date", value=date.today())
    new_batch = c8.text_input("Batch ID (optional)", value="")

    new_notes = st.text_area("Notes (optional)", value="", height=80)

    submitted = st.form_submit_button("Add sample")
    if submitted:
        u = new_upc.strip()
        if not u:
            st.error("UPC is required.")
        elif u in idx_map:
            st.error("That UPC already exists. Use the editor below to update it.")
        else:
            row = {k: "" for k in FIELDS}
            row.update({
                "upc": u,
                "brand": new_brand.strip(),
                "product_name": new_name.strip(),
                "variant": new_variant.strip(),
                "source_shipper": new_shipper.strip(),
                "contact_handle": new_handle.strip(),
                "received_date": str(new_received),
                "batch_id": new_batch.strip(),
                "status": STATUS_NEW,
                "tiktok_posted": "0",
                "instagram_posted": "0",
                "amazon_posted": "0",
                "notes": new_notes.strip(),
                "last_updated": now_str(),
            })
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            save_df(df, csv_path)
            st.success("Added and saved to CSV.")

st.divider()

# --- Content Queue ---
st.subheader("Content Queue (what’s left)")
queue = df.copy()
# show only items not complete
queue = queue[queue["status"] != STATUS_COMPLETE].copy()

# Determine if missing required postings
def missing_platforms(r):
    missing = []
    if r.get("tiktok_posted","0") != "1":
        missing.append("TikTok")
    if r.get("instagram_posted","0") != "1":
        missing.append("Instagram")
    if track_amazon and r.get("amazon_posted","0") != "1":
        missing.append("Amazon")
    return ", ".join(missing)

queue["missing"] = queue.apply(missing_platforms, axis=1)

cq1, cq2, cq3 = st.columns([2, 2, 6])
status_filter = cq1.multiselect("Filter by status", options=ALL_STATUSES, default=[STATUS_NEW, STATUS_FILMED, STATUS_POSTED])
brand_filter = cq2.text_input("Filter by brand contains", value="")

if status_filter:
    queue = queue[queue["status"].isin(status_filter)]
if brand_filter.strip():
    queue = queue[queue["brand"].str.lower().str.contains(brand_filter.strip().lower(), na=False)]

st.dataframe(
    queue[["upc", "brand", "product_name", "variant", "status", "missing", "source_shipper", "received_date", "batch_id"]],
    use_container_width=True,
    hide_index=True
)

st.divider()

# --- Quick Actions (scan UPC then click buttons) ---
st.subheader("Quick Actions (fast status updates)")
qa_upc = st.text_input("UPC for quick actions (scan/paste)", value=scan_upc.strip())
qa_cols = st.columns([2, 2, 2, 2, 4])

def update_row_by_upc(df_local, u, updates: dict):
    u = (u or "").strip()
    if not u:
        st.error("Enter a UPC.")
        return df_local
    hit = df_local.index[df_local["upc"] == u].tolist()
    if not hit:
        st.error("UPC not found.")
        return df_local
    i = hit[0]
    for k, v in updates.items():
        df_local.at[i, k] = v
    # recompute status
    df_local.at[i, "status"] = compute_status(df_local.loc[i], track_amazon)
    df_local.at[i, "last_updated"] = now_str()
    save_df(df_local, csv_path)
    st.success("Saved to CSV.")
    return df_local

with qa_cols[0]:
    if st.button("Mark FILMED"):
        df = update_row_by_upc(df, qa_upc, {"status": STATUS_FILMED})
with qa_cols[1]:
    if st.button("Mark TikTok POSTED"):
        df = update_row_by_upc(df, qa_upc, {"tiktok_posted": "1"})
with qa_cols[2]:
    if st.button("Mark Instagram POSTED"):
        df = update_row_by_upc(df, qa_upc, {"instagram_posted": "1"})
with qa_cols[3]:
    if st.button("Mark Amazon POSTED", disabled=not track_amazon):
        df = update_row_by_upc(df, qa_upc, {"amazon_posted": "1"})
with qa_cols[4]:
    st.caption("Optional: paste URLs in the editor below after marking posted.")

st.divider()

# --- Inventory Editor ---
st.subheader("Inventory (edit in-place, then Save)")
st.caption("Edit cells directly. Then click **Save to CSV**. This is your full inventory table.")

edited = st.data_editor(
    filtered,
    use_container_width=True,
    hide_index=True,
    num_rows="dynamic",
    key="editor",
)

save_cols = st.columns([2, 8])
with save_cols[0]:
    if st.button("Save to CSV"):
        # Merge edits back into full df (handles filtered view)
        edited_full = df.copy()

        # If filtered equals full, overwrite directly
        if len(edited) == len(df) and set(edited["upc"]) == set(df["upc"]):
            edited_full = edited.copy()
        else:
            # Apply updates row-by-row by UPC
            edited_map = {r["upc"]: r for _, r in edited.iterrows()}
            for i in range(len(edited_full)):
                u = edited_full.at[i, "upc"]
                if u in edited_map:
                    for c in FIELDS:
                        edited_full.at[i, c] = str(edited_map[u].get(c, "") if edited_map[u].get(c, "") is not None else "")

        # Recompute status + timestamps
        for i in range(len(edited_full)):
            edited_full.at[i, "status"] = compute_status(edited_full.loc[i], track_amazon)
            if not edited_full.at[i, "last_updated"]:
                edited_full.at[i, "last_updated"] = now_str()

        save_df(edited_full, csv_path)
        st.success("Saved edits to CSV.")
with save_cols[1]:
    st.caption("If you scan a UPC at the top, the editor will focus to that item.")

st.divider()

# --- Export / backup ---
st.subheader("Export / Backup")
st.download_button(
    label="Download current samples.csv",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name=os.path.basename(csv_path),
    mime="text/csv",
)