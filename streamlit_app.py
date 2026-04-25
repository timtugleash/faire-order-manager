"""
Faire Order Manager — Streamlit App
=====================================
- Role-based login (admin / user)
- Pulls NEW and PROCESSING orders from Faire API
- Downloads packing slips as PDFs
- Downloads order data as Excel
- View Current Inventory from Google Sheets
- WSP Orders entry (admin only)

SETUP:
  pip install streamlit requests openpyxl gspread google-auth pandas

STREAMLIT SECRETS FORMAT:
  FAIRE_API_KEY = "..."
  ADMIN_PASSWORD = "..."
  USER_PASSWORD = "..."
  SHEET_ID = "..."

  [gcp_service_account]
  type = "service_account"
  project_id = "..."
  private_key_id = "..."
  private_key = "..."
  client_email = "..."
  ...
"""

import io
import requests
import openpyxl
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
FAIRE_API_KEY = st.secrets.get("FAIRE_API_KEY", "")
SHEET_ID      = st.secrets.get("SHEET_ID", "")

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
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(page_title="Faire Order Manager", page_icon="📦", layout="wide")

# ─────────────────────────────────────────────
# ROLE-BASED LOGIN
# ─────────────────────────────────────────────
USERS = {
    "admin": {"password": st.secrets.get("ADMIN_PASSWORD", "shenzhen#1"), "role": "admin"},
    "jt":    {"password": st.secrets.get("USER_PASSWORD",  "tug2026"),    "role": "user"},
}

def login_screen():
    st.title("📦 Faire Order Manager")
    st.markdown("Please log in to continue.")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        user = USERS.get(username.lower())
        if user and password == user["password"]:
            st.session_state.authenticated = True
            st.session_state.role          = user["role"]
            st.session_state.username      = username.lower()
            st.rerun()
        else:
            st.error("Incorrect username or password.")
    st.stop()

if not st.session_state.get("authenticated"):
    login_screen()

role = st.session_state.get("role", "user")

# ─────────────────────────────────────────────
# GOOGLE SHEETS CONNECTION
# ─────────────────────────────────────────────
@st.cache_resource
def get_gsheet_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=scopes,
    )
    return gspread.authorize(creds)


def get_sheet(tab_name: str):
    client = get_gsheet_client()
    sh     = client.open_by_key(SHEET_ID)
    return sh.worksheet(tab_name)


# ─────────────────────────────────────────────
# FAIRE API FUNCTIONS
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
    headers = {"X-FAIRE-ACCESS-TOKEN": FAIRE_API_KEY}
    orders  = []
    cursor  = None

    while True:
        params = {"limit": 50, "sort_by": "CREATED_AT"}
        if cursor:
            params["cursor"] = cursor

        r = requests.get(
            "https://www.faire.com/external-api/v2/orders",
            headers=headers,
            params=params,
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
    headers = {"X-FAIRE-ACCESS-TOKEN": FAIRE_API_KEY}
    url     = f"https://www.faire.com/external-api/v2/orders/{raw_id}/packing-slip-pdf"
    r       = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.content


# ─────────────────────────────────────────────
# EXCEL BUILDER
# ─────────────────────────────────────────────
def build_excel(orders: list) -> bytes:
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

    for i, sku in enumerate(ALL_SKUS):
        cell = ws.cell(row=ROW_SKU_START + i, column=1, value=sku)
        cell.font      = Font(name="Arial", size=10)
        cell.alignment = Alignment(horizontal="left", vertical="center")

    for col_offset, order in enumerate(orders):
        col = col_offset + 2

        date_cell = ws.cell(row=ROW_DATE, column=col, value=order["created_at"])
        date_cell.font      = Font(name="Arial", size=10)
        date_cell.alignment = Alignment(horizontal="center", vertical="center")

        ord_cell = ws.cell(row=ROW_ORDER, column=col, value=order["order_number"])
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

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────
# HEADER + NAVIGATION
# ─────────────────────────────────────────────
col_title, col_logout = st.columns([6, 1])
with col_title:
    st.title("📦 Faire Order Manager")
    st.caption(f"Logged in as **{st.session_state.username}** ({role})")
with col_logout:
    st.write("")
    if st.button("Logout"):
        st.session_state.clear()
        st.rerun()

pages = ["📋 Orders", "📊 Inventory"]
if role == "admin":
    pages.append("🛒 WSP Orders")

page = st.sidebar.radio("Navigation", pages)
st.sidebar.divider()
st.sidebar.caption(f"Logged in as **{st.session_state.username}**")


# ─────────────────────────────────────────────
# PAGE: ORDERS
# ─────────────────────────────────────────────
if page == "📋 Orders":
    st.header("📋 New & Processing Orders")
    st.caption("Showing NEW and PROCESSING orders from Faire only.")

    if not FAIRE_API_KEY:
        st.error("No Faire API key found.")
        st.stop()

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

    if role == "admin":
        excel_bytes = build_excel(orders)
        st.download_button(
            label     = "⬇️ Download Excel (All Orders)",
            data      = excel_bytes,
            file_name = "faire_orders.xlsx",
            mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.divider()

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


# ─────────────────────────────────────────────
# PAGE: INVENTORY
# ─────────────────────────────────────────────
elif page == "📊 Inventory":
    st.header("📊 Current Inventory")
    st.caption("Read-only view from Google Sheets.")

    try:
        client = get_gsheet_client()
        sh     = client.open_by_key(SHEET_ID)
        ws     = sh.worksheet("Inventory_Display")
        rows   = ws.get_all_values()

        if not rows or len(rows) < 2:
            st.info("No inventory data found.")
        else:
            headers   = rows[0]
            data_rows = rows[1:]

            inv_data = []
            for row in data_rows:
                if len(row) < 2 or not row[1]:
                    continue
                inv_data.append({
                    "Product":           row[0]  if len(row) > 0  else "",
                    "SKU":               row[1]  if len(row) > 1  else "",
                    "Storage Box":       row[2]  if len(row) > 2  else "",
                    "Pcs/Carton":        row[3]  if len(row) > 3  else "",
                    "Total Received":    row[4]  if len(row) > 4  else "",
                    "Current Inventory": row[5]  if len(row) > 5  else "",
                    "Storage Box Qty":   row[6]  if len(row) > 6  else "",
                    "Total Output":      row[7]  if len(row) > 7  else "",
                    "Avg Units/Day":     row[8]  if len(row) > 8  else "",
                    "Days Available":    row[9]  if len(row) > 9  else "",
                    "Refill Qty (pcs)":  row[10] if len(row) > 10 else "",
                    "Refill Qty (ctn)":  row[11] if len(row) > 11 else "",
                })

            df = pd.DataFrame(inv_data)
            st.dataframe(df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Could not load inventory: {e}")
        st.info("Tip: Make sure the tab is named exactly 'Inventory' and the service account has Editor access to the sheet.")


# ─────────────────────────────────────────────
# PAGE: WSP ORDERS (Admin only)
# ─────────────────────────────────────────────
elif page == "🛒 WSP Orders":
    st.header("🛒 WholesalePet.com Orders")
    st.caption("Admin only. Enter new WSP orders below.")

    # View existing WSP orders in spreadsheet layout
    try:
        ws         = get_sheet("WSP Orders")
        data       = ws.get_all_values()
        order_rows = [r for r in data[1:] if len(r) >= 2 and r[1]]

        if order_rows:
            st.subheader("Existing WSP Orders")

            orders_dict = {}
            for row in order_rows:
                order_num = row[1]
                orders_dict[order_num] = {
                    "Order Date": row[0] if len(row) > 0 else "",
                    "Customer":   row[2] if len(row) > 2 else "",
                }
                for i, sku in enumerate(ALL_SKUS):
                    col_idx = i + 3
                    val = row[col_idx] if col_idx < len(row) else ""
                    orders_dict[order_num][sku] = val

            order_nums = list(orders_dict.keys())
            table_rows = []

            for meta in ["Order Date", "Customer"]:
                row_data = {"SKU": meta}
                for o in order_nums:
                    row_data[o] = orders_dict[o].get(meta, "")
                table_rows.append(row_data)

            table_rows.append({"SKU": "─" * 10})

            for sku in ALL_SKUS:
                row_data = {"SKU": sku}
                for o in order_nums:
                    row_data[o] = orders_dict[o].get(sku, "")
                table_rows.append(row_data)

            df = pd.DataFrame(table_rows)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No WSP orders entered yet.")

    except Exception as e:
        st.error(f"Could not load WSP orders: {e}")

    st.divider()
    st.subheader("➕ Enter New WSP Order")

    with st.form("wsp_order_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            order_date = st.date_input("Order Date")
        with col2:
            order_num = st.text_input("Order #")
        with col3:
            customer = st.text_input("Customer", value="WholesalePet.com")

        st.markdown("**Enter quantities for each SKU (leave at 0 if not ordered):**")

        quantities   = {}
        cols_per_row = 4
        sku_chunks   = [ALL_SKUS[i:i + cols_per_row] for i in range(0, len(ALL_SKUS), cols_per_row)]

        for chunk in sku_chunks:
            cols = st.columns(cols_per_row)
            for i, sku in enumerate(chunk):
                with cols[i]:
                    quantities[sku] = st.number_input(sku, min_value=0, value=0, step=1, key=f"wsp_{sku}")

        submitted = st.form_submit_button("💾 Save WSP Order")

        if submitted:
            if not order_num:
                st.error("Please enter an Order #.")
            else:
                try:
                    ws  = get_sheet("WSP Orders")
                    row = [str(order_date), order_num, customer] + [
                        quantities[sku] if quantities[sku] > 0 else "" for sku in ALL_SKUS
                    ]
                    ws.append_row(row)
                    st.success(f"✅ Order {order_num} saved successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to save order: {e}")
