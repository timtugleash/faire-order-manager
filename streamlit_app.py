"""
Faire Order Manager — Streamlit App
=====================================
Displays NEW and PROCESSING orders from Faire.
Allows downloading the order data as Excel and packing slips as PDFs.

SETUP:
  pip install streamlit requests openpyxl

RUN LOCALLY:
  streamlit run streamlit_app.py

DEPLOY:
  Push to GitHub, then connect to share.streamlit.io
  Add FAIRE_API_KEY to Streamlit Cloud secrets.
"""

import io
import os
import requests
import openpyxl
import streamlit as st
from datetime import datetime
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ─────────────────────────────────────────────
# CONFIGURATION
# Get API key from Streamlit secrets (when deployed) or environment variable
# ─────────────────────────────────────────────
FAIRE_API_KEY = st.secrets.get("FAIRE_API_KEY", os.environ.get("FAIRE_API_KEY", ""))

ALL_SKUS = [
    "T008-SBLK", "T008-MBLK", "T008-LBLK",
    "T008-SG",   "T008-MG",   "T008-LG",
    "T008-SC",   "T008-MC",   "T008-LC",
    "GRAB-H-SWT",  "GRAB-H-MWT",  "GRAB-H-LWT",  "GRAB-H-XLWT",
    "GRAB-H-SBLK", "GRAB-H-MBLK", "GRAB-H-LBLK", "GRAB-H-XLBLK",
    "GRAB-H-SG",   "GRAB-H-MG",   "GRAB-H-LG",   "GRAB-H-XLG",
    "GRAB-C-MWT",  "GRAB-C-LWT",  "GRAB-C-XLWT",
    "GRAB-C-MBLK", "GRAB-C-LBLK", "GRAB-C-XLBLK",
    "ROPE-OVL-BLK", "ROPE-OVL-BLU", "ROPE-OVL-GRN",
    "TUG-WM-MNY", "TUG-FL-02",
]

INCLUDE_STATES = {"NEW", "PROCESSING"}

# ─────────────────────────────────────────────
# API FUNCTIONS
# ─────────────────────────────────────────────

def parse_order(data: dict) -> dict:
    items = []
    for item in data.get("items", []):
        sku = item.get("sku") or item.get("product_option", {}).get("sku", "UNKNOWN")
        items.append({"sku": sku, "quantity": item.get("quantity", 0)})

    created_raw = data.get("created_at", "")
    if isinstance(created_raw, (int, float)):
        created = datetime.utcfromtimestamp(created_raw / 1000).strftime("%Y-%m-%d")
    else:
        created = str(created_raw)[:10]

    return {
        "order_number": data.get("display_id", ""),
        "raw_id":       data.get("id", ""),
        "created_at":   created,
        "state":        data.get("state", ""),
        "customer":     data.get("address", {}).get("company_name", ""),
        "items":        items,
    }


@st.cache_data(ttl=300, show_spinner=False)
def fetch_orders() -> list:
    """Fetch all NEW and PROCESSING orders. Cached for 5 minutes."""
    headers = {"X-FAIRE-ACCESS-TOKEN": FAIRE_API_KEY}
    orders  = []
    cursor  = None

    while True:
        params = {"limit": 50, "sort_by": "CREATED_AT"}
        if cursor:
            params["cursor"] = cursor

        r = requests.get(
            "https://www.faire.com/external-api/v2/orders",
            headers = headers,
            params  = params,
        )
        r.raise_for_status()
        data  = r.json()
        batch = data.get("orders", [])

        active  = [o for o in batch if o.get("state", "").upper() in INCLUDE_STATES]
        orders += [parse_order(o) for o in active]

        cursor = data.get("cursor")
        if not cursor or not batch:
            break

    return orders


def fetch_packing_slip(raw_id: str) -> bytes:
    """Fetch a single packing slip PDF as bytes."""
    headers = {"X-FAIRE-ACCESS-TOKEN": FAIRE_API_KEY}
    url     = f"https://www.faire.com/external-api/v2/orders/{raw_id}/packing-slip-pdf"
    r       = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.content


# ─────────────────────────────────────────────
# EXCEL BUILDER
# ─────────────────────────────────────────────

def build_excel(orders: list) -> bytes:
    """Build the Excel workbook in memory and return as bytes."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Order Data"

    lookup = {
        order["order_number"]: {item["sku"]: item["quantity"] for item in order["items"]}
        for order in orders
    }

    ROW_DATE, ROW_ORDER, ROW_CUSTOMER, ROW_BLANK, ROW_SKU_START = 1, 2, 3, 4, 5

    for row, label in [(ROW_DATE, "Order Date"), (ROW_ORDER, "Order #"), (ROW_CUSTOMER, "Customer")]:
        cell = ws.cell(row=row, column=1, value=label)
        cell.font      = Font(bold=True, name="Arial", size=10)
        cell.fill      = PatternFill("solid", start_color="D9E1F2")
        cell.alignment = Alignment(horizontal="left", vertical="center")

    ws.cell(row=ROW_BLANK, column=1, value="")

    for i, sku in enumerate(ALL_SKUS):
        cell = ws.cell(row=ROW_SKU_START + i, column=1, value=sku)
        cell.font      = Font(name="Arial", size=10)
        cell.alignment = Alignment(horizontal="left", vertical="center")

    for col_offset, order in enumerate(orders):
        col = col_offset + 2

        date_cell = ws.cell(row=ROW_DATE,     column=col, value=order["created_at"])
        date_cell.font      = Font(name="Arial", size=10)
        date_cell.alignment = Alignment(horizontal="center", vertical="center")

        ord_cell  = ws.cell(row=ROW_ORDER,    column=col, value=order["order_number"])
        ord_cell.font      = Font(bold=True, name="Arial", size=10, color="FFFFFF")
        ord_cell.fill      = PatternFill("solid", start_color="2F5496")
        ord_cell.alignment = Alignment(horizontal="center", vertical="center")

        cust_cell = ws.cell(row=ROW_CUSTOMER, column=col, value=order["customer"])
        cust_cell.font      = Font(name="Arial", size=10)
        cust_cell.alignment = Alignment(horizontal="center", vertical="center")

        order_skus = lookup.get(order["order_number"], {})
        for i, sku in enumerate(ALL_SKUS):
            qty  = order_skus.get(sku, 0)
            cell = ws.cell(row=ROW_SKU_START + i, column=col, value=qty if qty else "")
            cell.font      = Font(name="Arial", size=10)
            cell.alignment = Alignment(horizontal="center", vertical="center")

    ws.column_dimensions["A"].width = 16
    for col_offset in range(len(orders)):
        ws.column_dimensions[get_column_letter(col_offset + 2)].width = 22

    for row in range(1, ROW_SKU_START + len(ALL_SKUS)):
        ws.row_dimensions[row].height = 16

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────

st.set_page_config(page_title="Faire Order Manager", page_icon="📦", layout="wide")

st.title("📦 Faire Order Manager")
st.caption("Showing NEW and PROCESSING orders only.")

if not FAIRE_API_KEY:
    st.error("No Faire API key found. Add FAIRE_API_KEY to your Streamlit secrets.")
    st.stop()

# ── Fetch orders ──────────────────────────────────────────────────────────────
col_refresh, col_status = st.columns([1, 5])
with col_refresh:
    if st.button("🔄 Refresh Orders"):
        st.cache_data.clear()

with st.spinner("Fetching orders from Faire..."):
    try:
        orders = fetch_orders()
    except Exception as e:
        st.error(f"Failed to fetch orders: {e}")
        st.stop()

if not orders:
    st.info("No NEW or PROCESSING orders found.")
    st.stop()

st.success(f"**{len(orders)} order(s)** found.")

# ── Download all as Excel ─────────────────────────────────────────────────────
st.subheader("📊 Download Order Data")
excel_bytes = build_excel(orders)
st.download_button(
    label     = "⬇️ Download Excel (All Orders)",
    data      = excel_bytes,
    file_name = "faire_orders.xlsx",
    mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

# ── Orders table + individual packing slips ───────────────────────────────────
st.subheader("📋 Orders")

# Header row
cols = st.columns([2, 3, 2, 2, 2])
cols[0].markdown("**Order #**")
cols[1].markdown("**Customer**")
cols[2].markdown("**Date**")
cols[3].markdown("**Status**")
cols[4].markdown("**Packing Slip**")

st.divider()

for order in orders:
    cols = st.columns([2, 3, 2, 2, 2])
    cols[0].write(order["order_number"])
    cols[1].write(order["customer"] or "—")
    cols[2].write(order["created_at"])
    cols[3].write(order["state"])

    # Packing slip download button per order
    customer_safe = (order["customer"] or "Unknown").replace("/", "-").replace("\\", "-")
    filename      = f"{order['order_number']}_{customer_safe}_PackingSlip.pdf"

    with cols[4]:
        try:
            pdf_bytes = fetch_packing_slip(order["raw_id"])
            st.download_button(
                label     = "⬇️ PDF",
                data      = pdf_bytes,
                file_name = filename,
                mime      = "application/pdf",
                key       = f"pdf_{order['raw_id']}",
            )
        except Exception:
            st.write("Unavailable")
