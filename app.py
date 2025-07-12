# === FILE: app.py ===
import streamlit as st
import time
from config.logger import setup_logger
from services.salesforce_service import connect_salesforce, get_items, get_sf_fields
from services.arango_service import connect_arango, ensure_db, ensure_collection
from utils.transfer import transfer_data_with_progress
from utils.preview import preview_salesforce_data
import pandas as pd

# Initialize logger
logger = setup_logger()

st.set_page_config(page_title="Salesforce → ArangoDB Migration", layout="wide")

st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>🔁 Salesforce to ArangoDB Migration</h1>
    <p>Seamlessly migrate your Salesforce data to ArangoDB with real-time progress tracking and preview</p>
</div>
""", unsafe_allow_html=True)

# Sidebar: Authentication Section
st.sidebar.header("🔐 Authentication")
sf_username = st.sidebar.text_input("Salesforce Username")
sf_password = st.sidebar.text_input("Salesforce Password", type="password")
sf_token = st.sidebar.text_input("Salesforce Token", type="password")
sf_instance = st.sidebar.text_input("Salesforce Instance URL", value="https://login.salesforce.com")
arango_url = st.sidebar.text_input("ArangoDB URL", value="http://localhost:8529")
arango_user = st.sidebar.text_input("ArangoDB Username", value="root")
arango_pass = st.sidebar.text_input("ArangoDB Password", type="password", value="")
db_name = st.sidebar.text_input("ArangoDB Target Database Name", value="sf_migration")

# Session state init
if "sf_client" not in st.session_state:
    st.session_state.sf_client = None
if "connected" not in st.session_state:
    st.session_state.connected = False
if "migration_progress" not in st.session_state:
    st.session_state.migration_progress = {}
if "migration_status" not in st.session_state:
    st.session_state.migration_status = {}

if st.sidebar.button("🔗 Connect to Services", type="primary"):
    if sf_username and sf_password and sf_token and sf_instance and arango_url and arango_user and db_name:
        with st.spinner("Connecting to services..."):
            try:
                sf = connect_salesforce(sf_username, sf_password, sf_token, sf_instance)
                arango = connect_arango(arango_url, arango_user, arango_pass)
                st.session_state.sf_client = sf
                st.session_state.arango_conn = arango
                st.session_state.db_name = db_name
                st.session_state.arango_user = arango_user
                st.session_state.arango_pass = arango_pass
                st.session_state.connected = True
                st.sidebar.success("✅ Connected successfully!")
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"❌ Connection failed: {e}")
    else:
        st.sidebar.warning("⚠️ Please fill in all required fields")

# Main UI logic
if st.session_state.connected:
    sf = st.session_state.sf_client
    arango = st.session_state.arango_conn

    st.success("🔗 Connected to Salesforce and ArangoDB")
    st.markdown("---")
    st.subheader("📦 Configure Migration Settings")

    items = get_items(sf)
    selected_items = st.multiselect("Select Salesforce Objects to Migrate", items)

    if selected_items:
        tabs = st.tabs([f"📋 {item}" for item in selected_items])
        item_inputs = []

        for tab, item in zip(tabs, selected_items):
            with tab:
                st.markdown(f"### Configuration for {item}")
                fields = get_sf_fields(sf, item)
                selected_fields = st.multiselect(f"Select fields from {item}", fields, key=f"fields_{item}")
                col_name = st.text_input(f"ArangoDB Collection Name", value=item.lower(), key=f"col_{item}")
                col_type = st.radio("Collection Type", ["Document", "Edge"], key=f"type_{item}", horizontal=True)
                item_inputs.append((item, selected_fields, col_name, col_type == "Edge"))

                if selected_fields:
                    with st.expander("🔍 Preview Data", expanded=False):
                        preview_limit = st.number_input("Preview Record Limit", min_value=5, max_value=100, value=20, step=5, key=f"limit_{item}")
                        if st.button(f"Preview {item} data", key=f"preview_{item}"):
                            with st.spinner("Fetching preview data..."):
                                try:
                                    field_map = {f: f for f in selected_fields}
                                    raw_records, arango_docs = preview_salesforce_data(sf, item, field_map, limit=preview_limit)
                                    sf_df = pd.DataFrame(raw_records).drop(columns='attributes', errors='ignore')
                                    arango_df = pd.DataFrame(arango_docs)
                                    st.markdown("#### 🗂 Salesforce Records")
                                    st.dataframe(sf_df, use_container_width=True)
                                    st.download_button("Download Salesforce CSV", sf_df.to_csv(index=False).encode("utf-8"), f"{item}_sf.csv", "text/csv")
                                    st.markdown("#### 🧾 Transformed ArangoDB Docs")
                                    st.dataframe(arango_df, use_container_width=True)
                                    st.download_button("Download ArangoDB JSON", arango_df.to_json(orient="records", indent=2), f"{item}_arango.json", "application/json")
                                except Exception as e:
                                    st.error(f"Preview failed: {e}")

        st.markdown("---")
        st.subheader("🚀 Start Migration")

        if st.button("Start Migration Now", use_container_width=True, type="primary"):
            st.session_state.migration_progress = {}
            st.session_state.migration_status = {}

            for item_name, _, _, _ in item_inputs:
                st.session_state.migration_progress[item_name] = 0
                st.session_state.migration_status[item_name] = "pending"

            progress_containers = {item: st.empty() for item, *_ in item_inputs}
            overall_progress = st.progress(0)
            overall_status = st.empty()

            try:
                adb = ensure_db(arango, st.session_state.db_name, st.session_state.arango_user, st.session_state.arango_pass)
                completed_items = 0

                for item_name, fields, col_name, is_edge in item_inputs:
                    if not fields:
                        st.warning(f"⚠️ No fields selected for `{item_name}` - skipping")
                        continue

                    st.session_state.migration_status[item_name] = "processing"

                    with progress_containers[item_name]:
                        st.markdown(f"🔄 Migrating {item_name} → {col_name}")

                    item_progress = st.progress(0)
                    item_status = st.empty()

                    try:
                        collection = ensure_collection(adb, col_name, is_edge)
                        field_map = {f: f for f in fields}

                        def progress_callback(current, total):
                            pct = int((current / total) * 100) if total > 0 else 0
                            st.session_state.migration_progress[item_name] = pct
                            item_progress.progress(pct / 100)
                            item_status.text(f"Migrated {current:,} of {total:,} records")

                        count = transfer_data_with_progress(
    sf=sf,
    adb=adb,
    object_name=item_name,
    collection_name=col_name,
    field_mappings=field_map,
    is_edge=is_edge,
    progress_callback=progress_callback
)
                        actual = collection.count()

                        st.session_state.migration_status[item_name] = "success"
                        st.session_state.migration_progress[item_name] = 100
                        item_progress.progress(1.0)
                        item_status.text(f"✅ Completed - {actual:,} records")

                    except Exception as e:
                        st.session_state.migration_status[item_name] = "error"
                        st.error(f"❌ Migration failed for {item_name}: {e}")
                        logger.error(f"Migration failed: {e}")

                    completed_items += 1
                    overall_progress.progress(completed_items / len(item_inputs))
                    overall_status.text(f"Completed {completed_items} of {len(item_inputs)}")

                st.success("🎉 Migration Complete!")
            except Exception as e:
                st.error(f"Migration setup failed: {e}")
else:
    st.info("🔌 Please connect to services using the sidebar.")


