"""
Faire Order Manager — Streamlit App
=====================================
- Role-based login (admin / user)
- Pulls NEW and PROCESSING orders from Faire API
- WSP orders appear on Orders screen as PROCESSING
- PDF packing slip upload on WSP screen, download on Orders screen
- Downloads order data as Excel
- View Current Inventory from Google Sheets

SETUP:
  pip install streamlit requests openpyxl gspread google-auth google-api-python-client pandas

STREAMLIT SECRETS FORMAT:
  FAIRE_API_KEY = "..."
  ADMIN_PASSWORD = "..."
  USER_PASSWORD = "..."
  SHEET_ID = "..."
  DRIVE_FOLDER_ID = "..."  # Google Drive folder ID for packing slip PDFs

  [gcp_service_account]
  type = "service_account"
  project_id = "..."
  ...
"""

import io
import requests
import openpyxl
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request as GoogleAuthRequest
from datetime import datetime
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
FAIRE_API_KEY    = st.secrets.get("FAIRE_API_KEY", "")
SHEET_ID         = st.secrets.get("SHEET_ID", "")
DRIVE_FOLDER_ID  = st.secrets.get("DRIVE_FOLDER_ID", "")

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
# GOOGLE SHEETS + DRIVE CONNECTION
# ─────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def get_credentials():
    return Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES,
    )

@st.cache_resource
def get_gsheet_client():
    return gspread.authorize(get_credentials())

@st.cache_resource
def get_drive_service():
    return build("drive", "v3", credentials=get_credentials())


def get_sheet(tab_name: str):
    client = get_gsheet_client()
    sh     = client.open_by_key(SHEET_ID)
    return sh.worksheet(tab_name)


def upload_pdf_to_drive(pdf_bytes: bytes, filename: str) -> str:
    """Upload a PDF to Google Drive and return the file ID."""
    service    = get_drive_service()
    media      = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf")
    file_meta  = {"name": filename, "parents": [DRIVE_FOLDER_ID]}
    file       = service.files().create(body=file_meta, media_body=media, fields="id").execute()
    return file.get("id", "")


def download_pdf_from_drive(file_id: str) -> bytes:
    """Download a PDF from Google Drive by file ID."""
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    buf     = io.BytesIO()
    dl      = MediaIoBaseDownload(buf, request)
    done    = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf.read()


def get_wsp_orders() -> list:
    """Fetch WSP orders from Google Sheets and return as list of order dicts."""
    try:
        ws         = get_sheet("WSP Orders")
        data       = ws.get_all_values()
        order_rows = [r for r in data[1:] if len(r) >= 2 and r[1]]
        orders     = []
        for row in order_rows:
            items = []
            for i, sku in enumerate(ALL_SKUS):
                col_idx = i + 4  # Date, Order#, Customer, DriveFileID, then SKUs
                qty_str = row[col_idx] if col_idx < len(row) else ""
                try:
                    qty = int(qty_str)
                except (ValueError, TypeError):
                    qty = 0
                if qty > 0:
                    items.append({"sku": sku, "quantity": qty})
            orders.append({
                "order_number": row[1],
                "raw_id":       f"wsp_{row[1]}",
                "created_at":   row[0] if len(row) > 0 else "",
                "state":        "PROCESSING",
                "customer":     row[2] if len(row) > 2 else "",
                "drive_file_id": row[3] if len(row) > 3 else "",
                "items":        items,
                "source":       "WSP",
            })
        return orders
    except Exception:
        return []


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
        "drive_file_id": "",
        "items":        items,
        "source":       "FAIRE",
    }


@st.cache_data(ttl=300, show_spinner=False)
def fetch_faire_orders() -> list:
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
    st.caption("Showing NEW and PROCESSING orders from Faire and WholesalePet.com.")

    if not FAIRE_API_KEY:
        st.error("No Faire API key found.")
        st.stop()

    if st.button("🔄 Refresh Orders"):
        st.cache_data.clear()

    with st.spinner("Fetching orders..."):
        try:
            faire_orders = fetch_faire_orders()
        except Exception as e:
            st.error(f"Failed to fetch Faire orders: {e}")
            faire_orders = []

        wsp_orders = get_wsp_orders()
        all_orders = faire_orders + wsp_orders

    if not all_orders:
        st.info("No NEW or PROCESSING orders found.")
        st.stop()

    faire_count = len(faire_orders)
    wsp_count   = len(wsp_orders)
    st.success(f"**{len(all_orders)} order(s)** found — {faire_count} from Faire, {wsp_count} from WholesalePet.com")

    if role == "admin":
        excel_bytes = build_excel(all_orders)
        st.download_button(
            label     = "⬇️ Download Excel (All Orders)",
            data      = excel_bytes,
            file_name = "faire_orders.xlsx",
            mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.divider()

    cols = st.columns([2, 3, 2, 2, 1, 2])
    cols[0].markdown("**Order #**")
    cols[1].markdown("**Customer**")
    cols[2].markdown("**Date**")
    cols[3].markdown("**Status**")
    cols[4].markdown("**Source**")
    cols[5].markdown("**Packing Slip**")
    st.divider()

    for order in all_orders:
        cols = st.columns([2, 3, 2, 2, 1, 2])
        cols[0].write(order["order_number"])
        cols[1].write(order["customer"] or "—")
        cols[2].write(order["created_at"])
        cols[3].write(order["state"])
        cols[4].write("🛒 WSP" if order["source"] == "WSP" else "🏪 Faire")

        customer_safe = (order["customer"] or "Unknown").replace("/", "-").replace("\\", "-")
        filename      = f"{order['order_number']}_{customer_safe}_PackingSlip.pdf"

        with cols[5]:
            if order["source"] == "WSP":
                # Download from Google Drive if file ID exists
                if order.get("drive_file_id"):
                    try:
                        pdf_bytes = download_pdf_from_drive(order["drive_file_id"])
                        st.download_button(
                            label     = "⬇️ PDF",
                            data      = pdf_bytes,
                            file_name = filename,
                            mime      = "application/pdf",
                            key       = f"pdf_{order['raw_id']}",
                        )
                    except Exception:
                        st.write("Unavailable")
                else:
                    st.write("No PDF")
            else:
                # Fetch from Faire API
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

    def sheet_to_excel(tab_name: str) -> bytes:
        """Export a Google Sheet tab as formatted Excel using Google export API."""
        # Get the sheet GID (tab ID) for the specific tab
        client = get_gsheet_client()
        sh     = client.open_by_key(SHEET_ID)
        ws     = sh.worksheet(tab_name)
        gid    = ws.id

        # Use Google Sheets export URL to download as formatted xlsx
        export_url = (
            f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export"
            f"?format=xlsx&gid={gid}"
        )
        creds = get_credentials()
        creds.refresh(GoogleAuthRequest())
        headers  = {"Authorization": f"Bearer {creds.token}"}
        response = requests.get(export_url, headers=headers)
        response.raise_for_status()
        return response.content

    try:
        client = get_gsheet_client()
        sh     = client.open_by_key(SHEET_ID)
        ws     = sh.worksheet("Inventory")
        rows   = ws.get_all_values()

        if not rows or len(rows) < 2:
            st.info("No inventory data found.")
        else:
            data_rows = rows[1:]
            inv_data  = []
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
            st.dataframe(df, use_container_width=True, hide_index=True, height=(len(inv_data) + 1) * 35 + 3)

            if role == "admin":
                st.divider()
                st.subheader("⬇️ Download Sheets")
                dl_col1, dl_col2 = st.columns(2)
                with dl_col1:
                    try:
                        inv_excel = sheet_to_excel("Inventory")
                        st.download_button(
                            label     = "⬇️ Download Inventory",
                            data      = inv_excel,
                            file_name = "inventory.xlsx",
                            mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                    except Exception as e:
                        st.error(f"Could not prepare Inventory download: {e}")
                with dl_col2:
                    try:
                        rcv_excel = sheet_to_excel("Inventory Received")
                        st.download_button(
                            label     = "⬇️ Download Inventory Received",
                            data      = rcv_excel,
                            file_name = "inventory_received.xlsx",
                            mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                    except Exception as e:
                        st.error(f"Could not prepare Inventory Received download: {e}")

    except Exception as e:
        st.error(f"Could not load inventory: {e}")
        st.info("Tip: Make sure the tab is named exactly 'Inventory' and the service account has Editor access to the sheet.")


# ─────────────────────────────────────────────
# PAGE: WSP ORDERS (Admin only)
# ─────────────────────────────────────────────
elif page == "🛒 WSP Orders":
    st.header("🛒 WholesalePet.com Orders")
    st.caption("Admin only. Enter new WSP orders below.")

    # View existing WSP orders
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
                    "Order Date":    row[0] if len(row) > 0 else "",
                    "Customer":      row[2] if len(row) > 2 else "",
                    "PDF":           "✅" if (len(row) > 3 and row[3]) else "—",
                    "drive_file_id": row[3] if len(row) > 3 else "",
                }
                for i, sku in enumerate(ALL_SKUS):
                    col_idx = i + 4
                    val = row[col_idx] if col_idx < len(row) else ""
                    orders_dict[order_num][sku] = val

            order_nums = list(orders_dict.keys())
            table_rows = []

            for meta in ["Order Date", "Customer", "PDF"]:
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
            st.dataframe(df, use_container_width=True, hide_index=True,
                         height=(len(table_rows) + 1) * 35 + 3)

            # Delete an order
            st.divider()
            st.subheader("🗑️ Delete a WSP Order")
            order_to_delete = st.selectbox("Select order to delete", options=["— select —"] + order_nums)
            if order_to_delete != "— select —":
                st.warning(f"You are about to delete order **{order_to_delete}** ({orders_dict[order_to_delete]['Customer']} — {orders_dict[order_to_delete]['Order Date']}). This cannot be undone.")
                if st.button("🗑️ Confirm Delete", type="primary"):
                    try:
                        ws_del   = get_sheet("WSP Orders")
                        all_rows = ws_del.get_all_values()
                        row_index = None
                        for i, row in enumerate(all_rows):
                            if len(row) >= 2 and row[1] == order_to_delete:
                                row_index = i + 1
                                break
                        if row_index:
                            file_id = orders_dict[order_to_delete].get("drive_file_id", "")
                            if file_id:
                                try:
                                    get_drive_service().files().delete(fileId=file_id).execute()
                                except Exception:
                                    pass
                            ws_del.delete_rows(row_index)
                            st.success(f"✅ Order {order_to_delete} deleted successfully!")
                            st.rerun()
                        else:
                            st.error("Could not find that order in the sheet.")
                    except Exception as e:
                        st.error(f"Failed to delete order: {e}")
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
            customer = st.text_input("Customer", value="")

        uploaded_pdf = st.file_uploader("Upload Packing Slip PDF (optional)", type=["pdf"])

        st.markdown("**Enter quantities for each SKU (leave at 0 if not ordered):**")
        quantities = {}
        for sku in ALL_SKUS:
            quantities[sku] = st.number_input(sku, min_value=0, value=0, step=1, key=f"wsp_{sku}")

        submitted = st.form_submit_button("💾 Save WSP Order")

        if submitted:
            if not order_num:
                st.error("Please enter an Order #.")
            else:
                try:
                    # Upload PDF to Google Drive if provided
                    drive_file_id = ""
                    if uploaded_pdf and DRIVE_FOLDER_ID:
                        customer_safe = customer.replace("/", "-").replace("\\", "-")
                        pdf_filename  = f"{order_num}_{customer_safe}_PackingSlip.pdf"
                        drive_file_id = upload_pdf_to_drive(uploaded_pdf.read(), pdf_filename)

                    # Save order to Google Sheets
                    # Row format: Date, Order#, Customer, DriveFileID, SKU quantities...
                    ws  = get_sheet("WSP Orders")
                    row = [str(order_date), order_num, customer, drive_file_id] + [
                        quantities[sku] if quantities[sku] > 0 else "" for sku in ALL_SKUS
                    ]
                    ws.append_row(row)
                    st.success(f"✅ Order {order_num} saved!" + (" PDF uploaded." if drive_file_id else ""))
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to save order: {e}")
