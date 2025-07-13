# # === FILE: app.py ===
# import streamlit as st
# import time
# import json
# import sys
# from config.logger import setup_logger
# from services.salesforce_service import connect_salesforce, get_items, get_sf_fields
# from services.arango_service import connect_arango, ensure_db, ensure_collection
# from utils.transfer import transfer_data_with_progress, calculate_data_size
# from utils.preview import preview_salesforce_data
# import pandas as pd

# # Initialize logger
# logger = setup_logger()

# # Query tool functions defined inline
# def execute_soql_query(sf, query):
#     """Execute SOQL query against Salesforce"""
#     try:
#         result = sf.query(query)
#         return result['records']
#     except Exception as e:
#         raise Exception(f"SOQL query execution failed: {e}")

# def execute_aql_query(adb, query):
#     """Execute AQL query against ArangoDB"""
#     try:
#         cursor = adb.aql.execute(query)
#         result = []
#         for doc in cursor:
#             result.append(doc)
#         return result
#     except Exception as e:
#         raise Exception(f"AQL query execution failed: {e}")

# st.set_page_config(page_title="Salesforce → ArangoDB Migration", layout="wide")

# st.markdown("""
# <style>
#     .main-header {
#         background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
#         padding: 2rem;
#         border-radius: 10px;
#         margin-bottom: 2rem;
#         text-align: center;
#         color: white;
#     }
#     .query-container {
#         background: #f8f9fa;
#         padding: 1rem;
#         border-radius: 8px;
#         margin: 1rem 0;
#     }
#     .preview-container {
#         background: #e8f5e8;
#         padding: 1rem;
#         border-radius: 8px;
#         margin: 1rem 0;
#     }
#     .success-metric {
#         background: #d4edda;
#         color: #155724;
#         padding: 0.5rem;
#         border-radius: 5px;
#         margin: 0.2rem 0;
#     }
#     .section-divider {
#         border-top: 2px solid #dee2e6;
#         margin: 2rem 0;
#     }
#     .select-all-info {
#         background: #cce5ff;
#         color: #004085;
#         padding: 0.5rem;
#         border-radius: 5px;
#         margin: 0.5rem 0;
#         font-weight: bold;
#     }
#     .stTabs [data-baseweb="tab-list"] {
#         overflow-x: auto;
#         white-space: nowrap;
#         scrollbar-width: thin;
#         scrollbar-color: #888 #f1f1f1;
#     }
#     .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {
#         height: 8px;
#     }
#     .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-track {
#         background: #f1f1f1;
#         border-radius: 4px;
#     }
#     .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
#         background: #888;
#         border-radius: 4px;
#     }
#     .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb:hover {
#         background: #555;
#     }
# </style>
# """, unsafe_allow_html=True)

# st.markdown("""
# <div class="main-header">
#     <h1>🔁 Salesforce to ArangoDB Migration</h1>
#     <p>Seamlessly migrate your Salesforce data to ArangoDB with real-time progress tracking, preview, and query tools</p>
# </div>
# """, unsafe_allow_html=True)

# # Sidebar: Authentication Section
# st.sidebar.header("🔐 Authentication")
# sf_username = st.sidebar.text_input("Salesforce Username")
# sf_password = st.sidebar.text_input("Salesforce Password", type="password")
# sf_token = st.sidebar.text_input("Salesforce Token", type="password")
# sf_instance = st.sidebar.text_input("Salesforce Instance URL", value="https://login.salesforce.com")
# arango_url = st.sidebar.text_input("ArangoDB URL", value="http://localhost:8529")
# arango_user = st.sidebar.text_input("ArangoDB Username", value="root")
# arango_pass = st.sidebar.text_input("ArangoDB Password", type="password", value="")
# db_name = st.sidebar.text_input("ArangoDB Target Database Name", value="sf_migration")

# # Session state init
# if "sf_client" not in st.session_state:
#     st.session_state.sf_client = None
# if "connected" not in st.session_state:
#     st.session_state.connected = False
# if "migration_progress" not in st.session_state:
#     st.session_state.migration_progress = {}
# if "migration_status" not in st.session_state:
#     st.session_state.migration_status = {}
# if "adb" not in st.session_state:
#     st.session_state.adb = None

# if st.sidebar.button("🔗 Connect to Services", type="primary"):
#     if sf_username and sf_password and sf_token and sf_instance and arango_url and arango_user and db_name:
#         with st.spinner("Connecting to services..."):
#             try:
#                 # Connect to Salesforce
#                 st.sidebar.info("🔄 Connecting to Salesforce...")
#                 sf = connect_salesforce(sf_username, sf_password, sf_token, sf_instance)
#                 st.sidebar.success("✅ Salesforce connected!")
                
#                 # Connect to ArangoDB
#                 st.sidebar.info("🔄 Connecting to ArangoDB...")
#                 arango = connect_arango(arango_url, arango_user, arango_pass)
#                 st.sidebar.success("✅ ArangoDB connected!")
                
#                 # First verification and creation of database
#                 st.sidebar.info(f"🔄 Verifying/Creating database '{db_name}'...")
#                 adb = ensure_db(arango, db_name, arango_user, arango_pass)
#                 st.sidebar.success(f"✅ Database '{db_name}' verified/created!")
                
#                 # Store connections in session state
#                 st.session_state.sf_client = sf
#                 st.session_state.arango_conn = arango
#                 st.session_state.adb = adb
#                 st.session_state.db_name = db_name
#                 st.session_state.arango_user = arango_user
#                 st.session_state.arango_pass = arango_pass
#                 st.session_state.connected = True
                
#                 st.sidebar.success("🎉 All connections established successfully!")
#                 st.rerun()
#             except Exception as e:
#                 st.sidebar.error(f"❌ Connection failed: {e}")
#                 logger.error(f"Connection failed: {e}")
#     else:
#         st.sidebar.warning("⚠️ Please fill in all required fields")

# # Main UI logic
# if st.session_state.connected:
#     sf = st.session_state.sf_client
#     arango = st.session_state.arango_conn
#     adb = st.session_state.adb

#     st.success(f"🔗 Connected to Salesforce and ArangoDB (Database: {st.session_state.db_name})")
#     st.markdown("---")
#     st.subheader("📦 Configure Migration Settings")

#     items = get_items(sf)
    
#     # Add Select All option at the top
#     items_with_select_all = ["Select All"] + items
    
#     selected_items_raw = st.multiselect("Select Salesforce Objects to Migrate", items_with_select_all)
    
#     # Handle Select All logic
#     if "Select All" in selected_items_raw:
#         selected_items = items  # All items except "Select All"
#         st.markdown('<div class="select-all-info">📋 All items are Selected</div>', unsafe_allow_html=True)
#     else:
#         selected_items = selected_items_raw

#     if selected_items:
#         tabs = st.tabs([f"📋 {item}" for item in selected_items])
#         item_inputs = []

#         for tab, item in zip(tabs, selected_items):
#             with tab:
#                 st.markdown(f"### Configuration for {item}")
#                 fields = get_sf_fields(sf, item)
#                 selected_fields = st.multiselect(f"Select fields from {item}", fields, key=f"fields_{item}")
#                 col_name = st.text_input(f"ArangoDB Collection Name", value=item.lower(), key=f"col_{item}")
#                 col_type = st.radio("Collection Type", ["Document", "Edge"], key=f"type_{item}", horizontal=True)
#                 item_inputs.append((item, selected_fields, col_name, col_type == "Edge"))

#                 if selected_fields:
#                     # Preview Data Section
#                     st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
#                     with st.expander("🔍 Preview Data", expanded=False):
#                         st.markdown('<div class="preview-container">', unsafe_allow_html=True)
#                         st.markdown(f"#### 📊 Data Preview for {item}")
                        
#                         preview_limit = st.number_input(
#                             "Preview Record Limit", 
#                             min_value=5, 
#                             max_value=100, 
#                             value=20, 
#                             step=5, 
#                             key=f"limit_{item}",
#                             help="Number of records to preview"
#                         )
                        
#                         col1, col2 = st.columns([1, 1])
                        
#                         with col1:
#                             if st.button(f"🔍 Preview {item} Data", key=f"preview_{item}", type="secondary"):
#                                 with st.spinner("Fetching preview data..."):
#                                     try:
#                                         field_map = {f: f for f in selected_fields}
#                                         raw_records, arango_docs = preview_salesforce_data(sf, item, field_map, limit=preview_limit)
                                        
#                                         # Calculate data size
#                                         data_size_mb = calculate_data_size(arango_docs)
                                        
#                                         # Store preview data in session state
#                                         st.session_state[f"preview_data_{item}"] = {
#                                             "sf_records": raw_records,
#                                             "arango_docs": arango_docs,
#                                             "data_size_mb": data_size_mb
#                                         }
                                        
#                                         st.success(f"✅ Preview loaded successfully! ({len(raw_records)} records)")
#                                         st.rerun()
                                        
#                                     except Exception as e:
#                                         st.error(f"❌ Preview failed: {e}")
                        
#                         # Display preview data if available
#                         if f"preview_data_{item}" in st.session_state:
#                             preview_data = st.session_state[f"preview_data_{item}"]
                            
#                             st.markdown("##### 🗂 Salesforce Records")
#                             sf_df = pd.DataFrame(preview_data["sf_records"]).drop(columns='attributes', errors='ignore')
#                             st.dataframe(sf_df, use_container_width=True)
                            
#                             st.download_button(
#                                 "📥 Download Salesforce CSV", 
#                                 sf_df.to_csv(index=False).encode("utf-8"), 
#                                 f"{item}_salesforce_preview.csv", 
#                                 "text/csv",
#                                 key=f"dl_sf_{item}"
#                             )
                            
#                             st.markdown("##### 🧾 Transformed ArangoDB Documents")
#                             arango_df = pd.DataFrame(preview_data["arango_docs"])
#                             st.dataframe(arango_df, use_container_width=True)
                            
#                             st.download_button(
#                                 "📥 Download ArangoDB JSON", 
#                                 arango_df.to_json(orient="records", indent=2), 
#                                 f"{item}_arango_preview.json", 
#                                 "application/json",
#                                 key=f"dl_arango_{item}"
#                             )
                            
#                             st.info(f"📊 Preview data size: **{preview_data['data_size_mb']:.2f} MB**")
                        
#                         st.markdown('</div>', unsafe_allow_html=True)
                
#                 # Query Tool Section
#                 st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
#                 with st.expander("🔧 Query Tool", expanded=False):
#                     st.markdown('<div class="query-container">', unsafe_allow_html=True)
#                     st.markdown(f"#### 🔍 Query Tool for {item}")
#                     st.markdown("Test your data with custom queries before and after migration")
                    
#                     query_type = st.radio(
#                         "Select Query Type", 
#                         ["SOQL (Salesforce)", "AQL (ArangoDB)"], 
#                         key=f"query_type_{item}", 
#                         horizontal=True,
#                         help="Choose between Salesforce SOQL or ArangoDB AQL queries"
#                     )
                    
#                     if query_type == "SOQL (Salesforce)":
#                         st.markdown("##### 📝 SOQL Query Editor")
#                         st.markdown("Query your Salesforce data using SOQL syntax")
                        
#                         # Build default query with selected fields
#                         selected_fields_str = ", ".join(selected_fields[:5]) if selected_fields else "Id, Name"
#                         default_soql = f"SELECT {selected_fields_str} FROM {item} LIMIT 10"
                        
#                         soql_query = st.text_area(
#                             "Enter SOQL Query", 
#                             value=default_soql, 
#                             key=f"soql_{item}", 
#                             height=100,
#                             help="Enter your SOQL query to fetch data from Salesforce"
#                         )
                        
#                         col1, col2 = st.columns([1, 3])
#                         with col1:
#                             if st.button("▶️ Execute SOQL", key=f"exec_soql_{item}", type="primary"):
#                                 with st.spinner("Executing SOQL query..."):
#                                     try:
#                                         result = execute_soql_query(sf, soql_query)
#                                         if result:
#                                             st.success(f"✅ Query executed successfully! Found **{len(result)}** records.")
#                                             result_df = pd.DataFrame(result).drop(columns='attributes', errors='ignore')
                                            
#                                             st.markdown("##### 📊 Query Results")
#                                             st.dataframe(result_df, use_container_width=True)
                                            
#                                             st.download_button(
#                                                 "📥 Download SOQL Results", 
#                                                 result_df.to_csv(index=False).encode("utf-8"), 
#                                                 f"{item}_soql_results.csv", 
#                                                 "text/csv", 
#                                                 key=f"dl_soql_{item}"
#                                             )
#                                         else:
#                                             st.info("ℹ️ No records found.")
#                                     except Exception as e:
#                                         st.error(f"❌ SOQL query failed: {e}")
#                     else:  # AQL Query
#                         st.markdown("##### 📝 AQL Query Editor")
#                         st.markdown("Query your ArangoDB data using AQL syntax")
                        
#                         collection_name = col_name if col_name else item.lower()
#                         default_aql = f"FOR doc IN {collection_name} LIMIT 10 RETURN doc"
                        
#                         aql_query = st.text_area(
#                             "Enter AQL Query", 
#                             value=default_aql, 
#                             key=f"aql_{item}", 
#                             height=100,
#                             help="Enter your AQL query to fetch data from ArangoDB"
#                         )
                        
#                         col1, col2 = st.columns([1, 3])
#                         with col1:
#                             if st.button("▶️ Execute AQL", key=f"exec_aql_{item}", type="primary"):
#                                 with st.spinner("Executing AQL query..."):
#                                     try:
#                                         result = execute_aql_query(adb, aql_query)
#                                         if result:
#                                             st.success(f"✅ Query executed successfully! Found **{len(result)}** records.")
                                            
#                                             st.markdown("##### 📊 Query Results (JSON Format)")
#                                             st.json(result)
                                            
#                                             # Convert to DataFrame for download options
#                                             if isinstance(result, list) and len(result) > 0:
#                                                 try:
#                                                     result_df = pd.DataFrame(result)
                                                    
#                                                     col1, col2 = st.columns(2)
#                                                     with col1:
#                                                         st.download_button(
#                                                             "📥 Download as CSV", 
#                                                             result_df.to_csv(index=False).encode("utf-8"), 
#                                                             f"{item}_aql_results.csv", 
#                                                             "text/csv", 
#                                                             key=f"dl_aql_csv_{item}"
#                                                         )
#                                                     with col2:
#                                                         st.download_button(
#                                                             "📥 Download as JSON", 
#                                                             json.dumps(result, indent=2), 
#                                                             f"{item}_aql_results.json", 
#                                                             "application/json", 
#                                                             key=f"dl_aql_json_{item}"
#                                                         )
#                                                 except Exception as e:
#                                                     st.warning(f"Could not convert to DataFrame: {e}")
#                                             else:
#                                                 st.info("ℹ️ No records found.")
#                                         except Exception as e:
#                                             st.error(f"❌ AQL query failed: {e}")
#                    
#                     st.markdown('</div>', unsafe_allow_html=True)

#         # Migration Section
#         st.markdown("---")
#         st.subheader("🚀 Start Migration")
#         st.markdown("Ready to migrate your selected objects? Click below to begin the transfer process.")

#         if st.button("🚀 Start Migration Now", use_container_width=True, type="primary"):
#             # Second verification of database before migration
#             try:
#                 with st.spinner("Verifying database connection before migration..."):
#                     # Re-verify database connection
#                     adb_migration = ensure_db(arango, st.session_state.db_name, st.session_state.arango_user, st.session_state.arango_pass)
#                     st.session_state.adb = adb_migration
#                     st.success(f"✅ Database '{st.session_state.db_name}' verified for migration!")
                    
#             except Exception as e:
#                 st.error(f"❌ Database verification failed before migration: {e}")
#                 st.stop()
            
#             st.session_state.migration_progress = {}
#             st.session_state.migration_status = {}
#             st.session_state.migration_metrics = {}

#             for item_name, _, _, _ in item_inputs:
#                 st.session_state.migration_progress[item_name] = 0
#                 st.session_state.migration_status[item_name] = "pending"
#                 st.session_state.migration_metrics[item_name] = {}

#             progress_containers = {item: st.empty() for item, *_ in item_inputs}
#             overall_progress = st.progress(0)
#             overall_status = st.empty()

#             try:
#                 completed_items = 0
#                 total_size_mb = 0

#                 for item_name, fields, col_name, is_edge in item_inputs:
#                     if not fields:
#                         st.warning(f"⚠️ No fields selected for `{item_name}` - skipping")
#                         continue

#                     st.session_state.migration_status[item_name] = "processing"

#                     with progress_containers[item_name]:
#                         st.markdown(f"🔄 Migrating {item_name} → {col_name}")

#                     item_progress = st.progress(0)
#                     item_status = st.empty()

#                     try:
#                         collection = ensure_collection(adb, col_name, is_edge)
#                         field_map = {f: f for f in fields}

#                         def progress_callback(current, total):
#                             pct = int((current / total) * 100) if total > 0 else 0
#                             st.session_state.migration_progress[item_name] = pct
#                             item_progress.progress(pct / 100)
#                             item_status.text(f"Migrated {current:,} of {total:,} records")

#                         count, data_size_mb = transfer_data_with_progress(
#                             sf=sf,
#                             adb=adb,
#                             object_name=item_name,
#                             collection_name=col_name,
#                             field_mappings=field_map,
#                             is_edge=is_edge,
#                             progress_callback=progress_callback
#                         )
                        
#                         actual = collection.count()
#                         total_size_mb += data_size_mb

#                         st.session_state.migration_status[item_name] = "success"
#                         st.session_state.migration_progress[item_name] = 100
#                         st.session_state.migration_metrics[item_name] = {
#                             "records": actual,
#                             "size_mb": data_size_mb
#                         }
                        
#                         item_progress.progress(1.0)
#                         item_status.markdown(f'<div class="success-metric">✅ Completed - {actual:,} records ({data_size_mb:.2f} MB)</div>', unsafe_allow_html=True)

#                     except Exception as e:
#                         st.session_state.migration_status[item_name] = "error"
#                         st.error(f"❌ Migration failed for {item_name}: {e}")
#                         logger.error(f"Migration failed: {e}")

#                     completed_items += 1
#                     overall_progress.progress(completed_items / len(item_inputs))
#                     overall_status.text(f"Completed {completed_items} of {len(item_inputs)}")

#                 st.success(f"🎉 Migration Complete! Total data transferred: **{total_size_mb:.2f} MB**")
                
#                 # Display summary metrics
#                 st.markdown("### 📊 Migration Summary")
#                 summary_data = []
#                 for item_name, metrics in st.session_state.migration_metrics.items():
#                     if metrics:
#                         summary_data.append({
#                             "Object": item_name,
#                             "Records": f"{metrics['records']:,}",
#                             "Size (MB)": f"{metrics['size_mb']:.2f}"
#                         })
                
#                 if summary_data:
#                     summary_df = pd.DataFrame(summary_data)
#                     st.dataframe(summary_df, use_container_width=True)
                    
#             except Exception as e:
#                 st.error(f"❌ Migration setup failed: {e}")
#                 logger.error(f"Migration setup failed: {e}")
# else:
#     st.info("🔌 Please connect to services using the sidebar to begin.")
#     st.markdown("""
#     ### 🚀 Getting Started
#     1. **Fill in your credentials** in the sidebar
#     2. **Connect to services** to verify connections
#     3. **Select objects** to migrate
#     4. **Preview data** and test queries
#     5. **Start migration** when ready
#     """)


import streamlit as st
import time
import json
import sys
from config.logger import setup_logger
from services.salesforce_service import connect_salesforce, get_items, get_sf_fields, get_sf_field_types
from services.arango_service import connect_arango, ensure_db, ensure_collection, get_collections, get_collection_fields
from utils.transfer import transfer_data_with_progress, calculate_data_size, transfer_arango_to_salesforce_with_progress, preview_arango_data
from utils.preview import preview_salesforce_data
import pandas as pd
import re

# Initialize logger
logger = setup_logger()

# Query tool functions defined inline
def execute_soql_query(sf, query):
    """Execute SOQL query against Salesforce"""
    try:
        result = sf.query(query)
        return result['records']
    except Exception as e:
        raise Exception(f"SOQL query execution failed: {e}")

def execute_aql_query(adb, query):
    """Execute AQL query against ArangoDB"""
    try:
        cursor = adb.aql.execute(query)
        result = []
        for doc in cursor:
            result.append(doc)
        return result
    except Exception as e:
        raise Exception(f"AQL query execution failed: {e}")

st.set_page_config(page_title="Salesforce → ArangoDB Migration", layout="wide")

st.markdown("""
<style>
    /* Modern Color Scheme - Light Theme */
    :root {
        --primary-color: #4f46e5;
        --primary-light: #6366f1;
        --secondary-color: #06b6d4;
        --success-color: #10b981;
        --warning-color: #f59e0b;
        --error-color: #ef4444;
        --background-light: #f8fafc;
        --surface-light: #ffffff;
        --border-light: #e2e8f0;
        --text-primary: #1e293b;
        --text-secondary: #64748b;
    }

    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2.5rem;
        border-radius: 20px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
        box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);
        position: relative;
        overflow: hidden;
    }
    
    .main-header::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><defs><pattern id="grain" width="100" height="100" patternUnits="userSpaceOnUse"><circle cx="25" cy="25" r="1" fill="white" opacity="0.1"/><circle cx="75" cy="75" r="1" fill="white" opacity="0.1"/><circle cx="50" cy="10" r="0.5" fill="white" opacity="0.1"/><circle cx="10" cy="60" r="0.5" fill="white" opacity="0.1"/><circle cx="90" cy="40" r="0.5" fill="white" opacity="0.1"/></pattern></defs><rect width="100" height="100" fill="url(%23grain)"/></svg>');
        pointer-events: none;
    }

    .main-header h1 {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }

    .main-header p {
        font-size: 1.1rem;
        opacity: 0.9;
        margin: 0;
    }

    /* Direction Selector Styling */
    .direction-selector {
        background: var(--surface-light);
        border: 2px solid var(--border-light);
        border-radius: 15px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
    }

    .direction-selector:hover {
        border-color: var(--primary-color);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
    }

    /* Flow Diagram */
    .flow-diagram {
        background: var(--background-light);
        border-radius: 15px;
        padding: 2rem;
        margin: 1.5rem 0;
        text-align: center;
        border: 1px solid var(--border-light);
    }

    .flow-step {
        display: inline-block;
        background: var(--surface-light);
        border: 2px solid var(--primary-color);
        border-radius: 50%;
        width: 60px;
        height: 60px;
        line-height: 60px;
        margin: 0 1rem;
        font-size: 1.5rem;
        font-weight: bold;
        color: var(--primary-color);
        position: relative;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }

    .flow-arrow {
        display: inline-block;
        font-size: 2rem;
        color: var(--primary-color);
        margin: 0 0.5rem;
        animation: pulse 2s infinite;
    }

    @keyframes pulse {
        0%, 100% { transform: scale(1); opacity: 1; }
        50% { transform: scale(1.1); opacity: 0.7; }
    }

    /* Data Transfer Animation */
    .transfer-animation {
        background: linear-gradient(90deg, var(--primary-light), var(--secondary-color));
        height: 4px;
        border-radius: 2px;
        margin: 1rem 0;
        position: relative;
        overflow: hidden;
    }

    .transfer-animation::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.6), transparent);
        animation: transfer-flow 2s infinite;
    }

    @keyframes transfer-flow {
        0% { left: -100%; }
        100% { left: 100%; }
    }

    /* Progress Bar Enhancement */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, var(--primary-color), var(--secondary-color));
        border-radius: 10px;
    }

    /* Card Styling */
    .config-card {
        background: var(--surface-light);
        border: 1px solid var(--border-light);
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        transition: all 0.3s ease;
    }

    .config-card:hover {
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
        transform: translateY(-2px);
    }

    /* Status Indicators */
    .status-indicator {
        display: inline-flex;
        align-items: center;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: 500;
        margin: 0.5rem 0;
    }

    .status-pending {
        background: #fef3c7;
        color: #92400e;
        border: 1px solid #fbbf24;
    }

    .status-processing {
        background: #dbeafe;
        color: #1e40af;
        border: 1px solid #3b82f6;
        animation: processing-pulse 1.5s infinite;
    }

    @keyframes processing-pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }

    .status-success {
        background: #d1fae5;
        color: #065f46;
        border: 1px solid #10b981;
    }

    .status-error {
        background: #fee2e2;
        color: #991b1b;
        border: 1px solid #ef4444;
    }

    /* Migration Metrics */
    .metric-card {
        background: linear-gradient(135deg, var(--surface-light) 0%, var(--background-light) 100%);
        border: 1px solid var(--border-light);
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
        text-align: center;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }

    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: var(--primary-color);
        margin: 0.5rem 0;
    }

    .metric-label {
        color: var(--text-secondary);
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    /* Query Container Enhancement */
    .query-container {
        background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
        padding: 1.5rem;
        border-radius: 12px;
        margin: 1rem 0;
        border: 1px solid var(--border-light);
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }

    .preview-container {
        background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
        padding: 1.5rem;
        border-radius: 12px;
        margin: 1rem 0;
        border: 1px solid #bbf7d0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }

    /* Success Metric Enhancement */
    .success-metric {
        background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);
        color: #065f46;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        border: 1px solid #10b981;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        animation: success-bounce 0.6s ease-out;
    }

    @keyframes success-bounce {
        0% { transform: scale(0.8); opacity: 0; }
        50% { transform: scale(1.05); }
        100% { transform: scale(1); opacity: 1; }
    }

    /* Select All Info Enhancement */
    .select-all-info {
        background: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 100%);
        color: #1e40af;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        font-weight: 600;
        border: 1px solid #3b82f6;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }

    /* Section Divider Enhancement */
    .section-divider {
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, var(--primary-color), transparent);
        margin: 2rem 0;
        border-radius: 1px;
    }

    /* Tab Enhancement */
    .stTabs [data-baseweb="tab-list"] {
        background: var(--background-light);
        border-radius: 10px;
        padding: 0.5rem;
        border: 1px solid var(--border-light);
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        transition: all 0.3s ease;
    }

    .stTabs [data-baseweb="tab"]:hover {
        background: var(--primary-light);
        color: white;
    }

    /* Button Enhancement */
    .stButton > button {
        border-radius: 10px;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }

    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
    }

    /* Data Transfer Visualization */
    .transfer-visualization {
        display: flex;
        align-items: center;
        justify-content: space-between;
        background: var(--background-light);
        border-radius: 15px;
        padding: 2rem;
        margin: 1.5rem 0;
        border: 2px dashed var(--border-light);
        position: relative;
        overflow: hidden;
    }

    .source-db, .target-db {
        text-align: center;
        flex: 1;
    }

    .db-icon {
        font-size: 3rem;
        margin-bottom: 1rem;
        display: block;
    }

    .db-name {
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 0.5rem;
    }

    .db-status {
        font-size: 0.9rem;
        color: var(--text-secondary);
    }

    .transfer-arrow {
        font-size: 2rem;
        color: var(--primary-color);
        margin: 0 2rem;
        animation: transfer-bounce 1s infinite;
    }

    @keyframes transfer-bounce {
        0%, 100% { transform: translateX(0); }
        50% { transform: translateX(5px); }
    }

    /* Data Flow Particles */
    .data-particles {
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        pointer-events: none;
        overflow: hidden;
    }

    .particle {
        position: absolute;
        width: 4px;
        height: 4px;
        background: var(--primary-color);
        border-radius: 50%;
        animation: particle-flow 3s infinite linear;
    }

    @keyframes particle-flow {
        0% { 
            left: 20%; 
            top: 50%; 
            opacity: 0; 
            transform: scale(0);
        }
        20% { 
            opacity: 1; 
            transform: scale(1);
        }
        80% { 
            opacity: 1; 
            transform: scale(1);
        }
        100% { 
            left: 80%; 
            top: 50%; 
            opacity: 0; 
            transform: scale(0);
        }
    }

    /* Responsive Design */
    @media (max-width: 768px) {
        .main-header h1 {
            font-size: 2rem;
        }
        
        .flow-step {
            width: 50px;
            height: 50px;
            line-height: 50px;
            font-size: 1.2rem;
            margin: 0 0.5rem;
        }
        
        .transfer-visualization {
            flex-direction: column;
            gap: 1rem;
        }
        
        .transfer-arrow {
            transform: rotate(90deg);
            margin: 1rem 0;
        }
    }

.xp-copy-area {
    background: #f8fafc;
    border-radius: 16px;
    border: 1.5px solid #e2e8f0;
    padding: 2rem 1.5rem;
    margin: 2rem 0 1.5rem 0;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    text-align: center;
    position: relative;
}
.xp-copy-bar {
    width: 100%;
    height: 32px;
    background: #e0e7ef;
    border-radius: 8px;
    overflow: hidden;
    margin: 1.5rem 0 0.5rem 0;
    position: relative;
    border: 1.5px solid #cbd5e1;
}
.xp-copy-bar-inner {
    height: 100%;
    background: repeating-linear-gradient(135deg, #60a5fa 0 20px, #3b82f6 20px 40px);
    background-size: 40px 40px;
    animation: xp-bar-stripes 1.2s linear infinite;
    border-radius: 8px 0 0 8px;
    transition: width 0.3s cubic-bezier(.4,2,.6,1);
}
@keyframes xp-bar-stripes {
    0% { background-position: 0 0; }
    100% { background-position: 40px 0; }
}
.xp-copy-icons {
    display: flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 1.2rem;
    gap: 1.5rem;
}
.xp-copy-file {
    font-size: 2.2rem;
    margin-right: 0.5rem;
    animation: xp-file-move 2s linear infinite;
}
.xp-copy-arrow {
    font-size: 2rem;
    color: #3b82f6;
    margin: 0 0.5rem;
    animation: xp-arrow-bounce 1.2s infinite;
}
.xp-copy-folder {
    font-size: 2.2rem;
    margin-left: 0.5rem;
}
@keyframes xp-file-move {
    0% { transform: translateX(0); opacity: 1; }
    40% { transform: translateX(30px); opacity: 1; }
    60% { transform: translateX(30px); opacity: 1; }
    100% { transform: translateX(0); opacity: 1; }
}
@keyframes xp-arrow-bounce {
    0%, 100% { transform: translateY(0); }
    50% { transform: translateY(-6px); }
}
.xp-copy-label {
    font-size: 1.1rem;
    color: #334155;
    margin-bottom: 0.5rem;
    font-weight: 500;
}
.xp-copy-stats {
    font-size: 0.98rem;
    color: #64748b;
    margin-top: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>🔁 Salesforce ↔ ArangoDB Migration</h1>
    <p>Seamlessly migrate data between Salesforce and ArangoDB with real-time progress tracking, preview, and query tools</p>
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
if "adb" not in st.session_state:
    st.session_state.adb = None
if "field_cache" not in st.session_state:
    st.session_state.field_cache = {}

if st.sidebar.button("🔗 Connect to Services", type="primary"):
    if sf_username and sf_password and sf_token and sf_instance and arango_url and arango_user and db_name:
        with st.spinner("Connecting to services..."):
            try:
                # Connect to Salesforce
                st.sidebar.info("🔄 Connecting to Salesforce...")
                sf = connect_salesforce(sf_username, sf_password, sf_token, sf_instance)
                st.sidebar.success("✅ Salesforce connected!")
                
                # Connect to ArangoDB
                st.sidebar.info("🔄 Connecting to ArangoDB...")
                arango = connect_arango(arango_url, arango_user, arango_pass)
                st.sidebar.success("✅ ArangoDB connected!")
                
                # First verification and creation of database
                st.sidebar.info(f"🔄 Verifying/Creating database '{db_name}'...")
                adb = ensure_db(arango, db_name, arango_user, arango_pass)
                st.sidebar.success(f"✅ Database '{db_name}' verified/created!")
                
                # Store connections in session state
                st.session_state.sf_client = sf
                st.session_state.arango_conn = arango
                st.session_state.adb = adb
                st.session_state.db_name = db_name
                st.session_state.arango_user = arango_user
                st.session_state.arango_pass = arango_pass
                st.session_state.connected = True
                
                st.sidebar.success("🎉 All connections established successfully!")
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"❌ Connection failed: {e}")
                logger.error(f"Connection failed: {e}")
    else:
        st.sidebar.warning("⚠️ Please fill in all required fields")

# Main UI logic
if st.session_state.connected:
    sf = st.session_state.sf_client
    arango = st.session_state.arango_conn
    adb = st.session_state.adb

    st.success(f"🔗 Connected to Salesforce and ArangoDB (Database: {st.session_state.db_name})")
    st.markdown("---")
    
    # Direction Selection with Enhanced UI
    st.markdown('<div class="direction-selector">', unsafe_allow_html=True)
    st.subheader("🔄 Migration Direction")
    migration_direction = st.radio(
        "Select Migration Direction",
        ["Salesforce → ArangoDB", "ArangoDB → Salesforce"],
        horizontal=True,
        help="Choose the direction of data migration"
    )
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data Transfer Visualization
    st.markdown('<div class="transfer-visualization">', unsafe_allow_html=True)
    if migration_direction == "Salesforce → ArangoDB":
        st.markdown("""
        <div class="source-db">
            <span class="db-icon">☁️</span>
            <div class="db-name">Salesforce</div>
            <div class="db-status">Source Database</div>
        </div>
        <div class="transfer-arrow">➡️</div>
        <div class="target-db">
            <span class="db-icon">🗄️</span>
            <div class="db-name">ArangoDB</div>
            <div class="db-status">Target Database</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="source-db">
            <span class="db-icon">🗄️</span>
            <div class="db-name">ArangoDB</div>
            <div class="db-status">Source Database</div>
        </div>
        <div class="transfer-arrow">➡️</div>
        <div class="target-db">
            <span class="db-icon">☁️</span>
            <div class="db-name">Salesforce</div>
            <div class="db-status">Target Database</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Add animated particles for data flow
    st.markdown("""
    <div class="data-particles">
        <div class="particle" style="animation-delay: 0s;"></div>
        <div class="particle" style="animation-delay: 0.5s;"></div>
        <div class="particle" style="animation-delay: 1s;"></div>
        <div class="particle" style="animation-delay: 1.5s;"></div>
        <div class="particle" style="animation-delay: 2s;"></div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    st.subheader("📦 Configure Migration Settings")

    if migration_direction == "Salesforce → ArangoDB":
        items = get_items(sf)
    else:
        # ArangoDB → Salesforce
        try:
            items = get_collections(adb)
        except Exception as e:
            st.error(f"❌ Failed to get ArangoDB collections: {e}")
            items = []
    
    # Add Select All option at the top
    items_with_select_all = ["Select All"] + items
    selected_items_raw = st.multiselect("Select Salesforce Objects to Migrate", items_with_select_all, key="object_select")
    
    # Handle Select All logic
    if "Select All" in selected_items_raw:
        st.markdown("### All Salesforce objects will be migrated to ArangoDB as document collections.")
        progress_bar = st.progress(0)
        status_text = st.empty()
        if st.button("🚀 Start Migration (All Objects)", type="primary"):
            with st.spinner("Migrating all Salesforce objects in batches of 500 items..."):
                # Ensure sf is set and valid
                sf = st.session_state.get("sf_client")
                if sf is None:
                    st.error("Salesforce connection not established. Please connect first.")
                    st.stop()
                try:
                    summary = []
                    all_items = get_items(sf)
                    batch_size = 500
                    num_batches = (len(all_items) + batch_size - 1) // batch_size
                    for batch_idx, batch_start in enumerate(range(0, len(all_items), batch_size)):
                        batch = all_items[batch_start:batch_start+batch_size]
                        for idx, item in enumerate(batch):
                            try:
                                fields = get_sf_fields(sf, item)
                                field_map = {f: f for f in fields}
                                collection_name = item.lower()
                                collection = ensure_collection(adb, collection_name, is_edge=False)
                                total_records = 0
                                record_batch_size = 500
                                offset = 0
                                error = None
                                while True:
                                    soql_fields = ', '.join(fields)
                                    soql = f"SELECT {soql_fields} FROM {item} LIMIT {record_batch_size} OFFSET {offset}"
                                    try:
                                        records = sf.query(soql)['records']
                                    except Exception as e:
                                        error_msg = str(e)
                                        if 'MALFORMED_QUERY' in error_msg or 'filter on a reified column' in error_msg:
                                            error = error_msg
                                            break
                                        else:
                                            raise
                                    if not records:
                                        break
                                    # Transform and insert into ArangoDB
                                    docs = []
                                    for rec in records:
                                        doc = {f: rec.get(f) for f in fields}
                                        docs.append(doc)
                                    if docs:
                                        collection.import_bulk(docs)
                                    total_records += len(docs)
                                    offset += record_batch_size
                                    status_text.info(f"Batch {batch_idx+1}/{num_batches} - Migrating {item}: {total_records} records transferred...")
                                summary.append({
                                    "object": item,
                                    "collection": collection_name,
                                    "records": total_records,
                                    "error": error
                                })
                            except Exception as e:
                                summary.append({
                                    "object": item,
                                    "collection": item.lower(),
                                    "records": 0,
                                    "error": str(e)
                                })
                        progress_bar.progress((batch_start + len(batch)) / len(all_items))
                    st.success("Migration completed successfully!")
                    st.markdown("### Migration Summary")
                    import pandas as pd
                    df = pd.DataFrame(summary)
                    st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error(f"Migration failed: {e}")
    else:
        selected_items = selected_items_raw
        if selected_items:
            # Create tabs but don't load content until activated
            tabs = st.tabs([f"📋 {item}" for item in selected_items])
            item_inputs = []

            for tab, item in zip(tabs, selected_items):
                with tab:
                    # Only load content when tab is active
                    with st.spinner(f"Loading configuration for {item}..."):
                        if migration_direction == "Salesforce → ArangoDB":
                            # Check if fields are cached, otherwise fetch
                            if item not in st.session_state.field_cache:
                                try:
                                    fields = get_sf_fields(sf, item)
                                    st.session_state.field_cache[item] = fields
                                except Exception as e:
                                    st.error(f"❌ Failed to load fields for {item}: {e}")
                                    continue
                            else:
                                fields = st.session_state.field_cache[item]

                            st.markdown(f"### Configuration for {item}")
                            selected_fields = st.multiselect(f"Select fields from {item}", fields, key=f"fields_{item}")
                            col_name = st.text_input(f"ArangoDB Collection Name", value=item.lower(), key=f"col_{item}")
                            col_type = st.radio("Collection Type", ["Document", "Edge"], key=f"type_{item}", horizontal=True)
                            item_inputs.append((item, selected_fields, col_name, col_type == "Edge"))
                        else:
                            # ArangoDB → Salesforce
                            try:
                                fields = get_collection_fields(adb, item)
                                st.session_state.field_cache[item] = fields
                            except Exception as e:
                                st.error(f"❌ Failed to load fields for {item}: {e}")
                                continue

                            st.markdown(f"### Configuration for {item}")
                            selected_fields = st.multiselect(f"Select fields from {item}", fields, key=f"fields_{item}")
                            
                            # Get Salesforce objects for mapping
                            sf_objects = get_items(sf)
                            target_object = st.selectbox(f"Target Salesforce Object", sf_objects, key=f"target_{item}")
                            
                            # Operation type for Salesforce
                            operation_type = st.selectbox(
                                "Operation Type",
                                ["insert", "update", "upsert"],
                                key=f"op_type_{item}",
                                help="insert: Create new records, update: Update existing records, upsert: Insert or update based on external ID"
                            )
                            
                            external_id_field = None
                            if operation_type == "upsert":
                                external_id_field = st.text_input(
                                    "External ID Field",
                                    value="Id",
                                    key=f"ext_id_{item}",
                                    help="Field to use for upsert operations (must be unique)"
                                )
                            
                            item_inputs.append((item, selected_fields, target_object, operation_type, external_id_field))

                        if selected_fields:
                            # Preview Data Section
                            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
                            with st.expander("🔍 Preview Data", expanded=False):
                                st.markdown('<div class="preview-container">', unsafe_allow_html=True)
                                st.markdown(f"#### 📊 Data Preview for {item}")
                                
                                preview_limit = st.number_input(
                                    "Preview Record Limit", 
                                    min_value=5, 
                                    max_value=100, 
                                    value=20, 
                                    step=5, 
                                    key=f"limit_{item}",
                                    help="Number of records to preview"
                                )
                                
                                col1, col2 = st.columns([1, 1])
                                
                                with col1:
                                    if st.button(f"🔍 Preview {item} Data", key=f"preview_{item}", type="secondary"):
                                        with st.spinner("Fetching preview data..."):
                                            try:
                                                if migration_direction == "Salesforce → ArangoDB":
                                                    field_map = {f: f for f in selected_fields}
                                                    raw_records, arango_docs = preview_salesforce_data(sf, item, field_map, limit=preview_limit)
                                                    print(f"DEBUG: raw_records for {item}:", raw_records)
                                                    print(f"DEBUG: arango_docs for {item}:", arango_docs)
                                                    st.session_state[f"preview_data_{item}"] = {
                                                        "source_records": raw_records,
                                                        "target_docs": arango_docs,
                                                        "direction": "sf_to_arango"
                                                    }
                                                    print(f"DEBUG: st.session_state['preview_data_{item}']:", st.session_state[f"preview_data_{item}"])
                                                    st.success(f"✅ Preview loaded successfully! ({len(raw_records)} records)")
                                                else:
                                                    # ArangoDB → Salesforce
                                                    field_map = {f: f for f in selected_fields}
                                                    arango_docs, sf_records = preview_arango_data(adb, item, field_map, limit=preview_limit)
                                                    print(f"DEBUG: arango_docs for {item}:", arango_docs)
                                                    print(f"DEBUG: sf_records for {item}:", sf_records)
                                                    st.session_state[f"preview_data_{item}"] = {
                                                        "source_records": arango_docs,
                                                        "target_docs": sf_records,
                                                        "direction": "arango_to_sf"
                                                    }
                                                    print(f"DEBUG: st.session_state['preview_data_{item}']:", st.session_state[f"preview_data_{item}"])
                                                    st.success(f"✅ Preview loaded successfully! ({len(arango_docs)} records)")
                                                st.rerun()
                                            except Exception as e:
                                                st.error(f"❌ Preview failed: {e}")
                                
                                # Display preview data if available
                                key = f"preview_data_{item}"
                                if key in st.session_state:
                                    preview_data = st.session_state[key]
                                    direction = preview_data.get("direction", "sf_to_arango")
                                    
                                    if direction == "sf_to_arango":
                                        st.markdown("##### 🗂 Salesforce Records")
                                        source_df = pd.DataFrame(preview_data["source_records"]).drop(columns='attributes', errors='ignore')
                                        st.dataframe(source_df, use_container_width=True)
                                        st.download_button(
                                            "📥 Download Salesforce CSV", 
                                            source_df.to_csv(index=False).encode("utf-8"), 
                                            f"{item}_salesforce_preview.csv", 
                                            "text/csv",
                                            key=f"dl_sf_{item}"
                                        )
                                        st.markdown("##### 🧾 Transformed ArangoDB Documents")
                                        target_df = pd.DataFrame(preview_data["target_docs"])
                                        st.dataframe(target_df, use_container_width=True)
                                        json_data = target_df.to_json(orient="records", indent=2)
                                        if json_data is not None:
                                            st.download_button(
                                                "📥 Download ArangoDB JSON", 
                                                json_data.encode("utf-8"),
                                                f"{item}_arango_preview.json", 
                                                "application/json",
                                                key=f"dl_arango_{item}"
                                            )
                                    else:  # arango_to_sf
                                        st.markdown("##### 🗂 ArangoDB Documents")
                                        source_df = pd.DataFrame(preview_data["source_records"])
                                        st.dataframe(source_df, use_container_width=True)
                                        json_data = source_df.to_json(orient="records", indent=2)
                                        if json_data is not None:
                                            st.download_button(
                                                "📥 Download ArangoDB JSON", 
                                                json_data.encode("utf-8"),
                                                f"{item}_arango_preview.json", 
                                                "application/json",
                                                key=f"dl_arango_{item}"
                                            )
                                        st.markdown("##### 🧾 Transformed Salesforce Records")
                                        target_df = pd.DataFrame(preview_data["target_docs"])
                                        st.dataframe(target_df, use_container_width=True)
                                        st.download_button(
                                            "📥 Download Salesforce CSV", 
                                            target_df.to_csv(index=False).encode("utf-8"), 
                                            f"{item}_salesforce_preview.csv", 
                                            "text/csv",
                                            key=f"dl_sf_{item}"
                                        )
                                else:
                                    st.info("No preview data available. Please click 'Preview Data' first.")
                                
                                st.markdown('</div>', unsafe_allow_html=True)
                        
                        # Query Tool Section
                        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
                        with st.expander("🔧 Query Tool", expanded=False):
                            st.markdown('<div class="query-container">', unsafe_allow_html=True)
                            st.markdown(f"#### 🔍 Query Tool for {item}")
                            st.markdown("Test your data with custom queries before and after migration")

                            if migration_direction == "Salesforce → ArangoDB":
                                # Only show SOQL
                                st.markdown("##### 📝 SOQL Query Editor")
                                st.markdown("Query your Salesforce data using SOQL syntax")
                                selected_fields_str = ", ".join(selected_fields[:5]) if selected_fields else "Id, Name"
                                default_soql = f"SELECT {selected_fields_str} FROM {item} LIMIT 10"
                                soql_query = st.text_area(
                                    "Enter SOQL Query",
                                    value=default_soql,
                                    key=f"soql_{item}",
                                    height=100,
                                    help="Enter your SOQL query to fetch data from Salesforce"
                                )
                                col1, col2 = st.columns([1, 3])
                                with col1:
                                    if st.button("▶️ Execute SOQL", key=f"exec_soql_{item}", type="primary"):
                                        with st.spinner("Executing SOQL query..."):
                                            try:
                                                result = execute_soql_query(sf, soql_query)
                                                if result:
                                                    st.success(f"✅ Query executed successfully! Found **{len(result)}** records.")
                                                    result_df = pd.DataFrame(result).drop(columns='attributes', errors='ignore')
                                                    st.markdown("##### 📊 Query Results")
                                                    st.dataframe(result_df, use_container_width=True)
                                                    st.download_button(
                                                        "📥 Download SOQL Results",
                                                        result_df.to_csv(index=False).encode("utf-8"),
                                                        f"{item}_soql_results.csv",
                                                        "text/csv",
                                                        key=f"dl_soql_{item}"
                                                    )
                                                else:
                                                    st.info("ℹ️ No records found.")
                                            except Exception as e:
                                                st.error(f"❌ SOQL query failed: {e}")
                            elif migration_direction == "ArangoDB → Salesforce":
                                # Only show AQL
                                st.markdown("##### 📝 AQL Query Editor")
                                st.markdown("Query your ArangoDB data using AQL syntax")
                                collection_name = item.lower()
                                default_aql = f"FOR doc IN {collection_name} LIMIT 10 RETURN doc"
                                aql_query = st.text_area(
                                    "Enter AQL Query",
                                    value=default_aql,
                                    key=f"aql_{item}",
                                    height=100,
                                    help="Enter your AQL query to fetch data from ArangoDB"
                                )
                                col1, col2 = st.columns([1, 3])
                                with col1:
                                    if st.button("▶️ Execute AQL", key=f"exec_aql_{item}", type="primary"):
                                        with st.spinner("Executing AQL query..."):
                                            try:
                                                result = execute_aql_query(adb, aql_query)
                                                if result:
                                                    st.success(f"✅ Query executed successfully! Found **{len(result)}** records.")
                                                    st.markdown("##### 📊 Query Results (JSON Format)")
                                                    st.json(result)
                                                    if isinstance(result, list) and len(result) > 0:
                                                        try:
                                                            result_df = pd.DataFrame(result)
                                                            col1, col2 = st.columns(2)
                                                            with col1:
                                                                st.download_button(
                                                                    "📥 Download as CSV",
                                                                    result_df.to_csv(index=False).encode("utf-8"),
                                                                    f"{item}_aql_results.csv",
                                                                    "text/csv",
                                                                    key=f"dl_aql_csv_{item}"
                                                                )
                                                            with col2:
                                                                st.download_button(
                                                                    "📥 Download as JSON",
                                                                    json.dumps(result, indent=2),
                                                                    f"{item}_aql_results.json",
                                                                    "application/json",
                                                                    key=f"dl_aql_json_{item}"
                                                                )
                                                        except Exception as e:
                                                            st.warning(f"Could not convert to DataFrame: {e}")
                                                else:
                                                    st.info("ℹ️ No records found.")
                                            except Exception as e:
                                                    st.error(f"❌ AQL query failed: {e}")
                            st.markdown('</div>', unsafe_allow_html=True)

            # Migration Section
            st.markdown("---")
            st.subheader("🚀 Start Migration")
            st.markdown("Ready to migrate your selected objects? Click below to begin the transfer process.")

            if st.button("🚀 Start Migration Now", use_container_width=True, type="primary"):
                # Second verification of database before migration
                try:
                    with st.spinner("Verifying database connection before migration..."):
                        # Re-verify database connection
                        adb_migration = ensure_db(arango, st.session_state.db_name, st.session_state.arango_user, st.session_state.arango_pass)
                        st.session_state.adb = adb_migration
                        st.success(f"✅ Database '{st.session_state.db_name}' verified for migration!")
                        
                except Exception as e:
                    st.error(f"❌ Database verification failed before migration: {e}")
                    st.stop()
                
                st.session_state.migration_progress = {}
                st.session_state.migration_status = {}
                st.session_state.migration_metrics = {}

                for item_input in item_inputs:
                    item_name = item_input[0]  # First element is always the name
                    st.session_state.migration_progress[item_name] = 0
                    st.session_state.migration_status[item_name] = "pending"
                    st.session_state.migration_metrics[item_name] = {}

                progress_containers = {item_input[0]: st.empty() for item_input in item_inputs}
                overall_progress = st.progress(0)
                overall_status = st.empty()

                try:
                    completed_items = 0
                    total_size_mb = 0

                    for item_input in item_inputs:
                        if migration_direction == "Salesforce → ArangoDB":
                            item_name, fields, col_name, is_edge = item_input
                            
                            if not fields:
                                st.warning(f"⚠️ No fields selected for `{item_name}` - skipping")
                                continue

                            st.session_state.migration_status[item_name] = "processing"

                            with progress_containers[item_name]:
                                st.markdown(f"""
                                <div class="config-card">
                                    <h4>🔄 Migrating {item_name} → {col_name}</h4>
                                    <div class="transfer-animation"></div>
                                    <div class="status-indicator status-processing">Processing...</div>
                                </div>
                                """, unsafe_allow_html=True)

                            item_progress = st.progress(0)
                            item_status = st.empty()

                            try:
                                collection = ensure_collection(adb, col_name, is_edge)
                                field_map = {f: f for f in fields}

                                def progress_callback(current, total):
                                    pct = int((current / total) * 100) if total > 0 else 0
                                    st.session_state.migration_progress[item_name] = pct
                                    item_progress.progress(pct / 100)
                                    
                                    # Enhanced progress display with animations
                                    with progress_containers[item_name]:
                                        st.markdown(f'''
                                        <div class="xp-copy-area">
                                          <div class="xp-copy-label">Transferring data...</div>
                                          <div class="xp-copy-icons">
                                            <span class="xp-copy-file">📄</span>
                                            <span class="xp-copy-arrow">➡️</span>
                                            <span class="xp-copy-folder">🗄️</span>
                                          </div>
                                          <div class="xp-copy-bar">
                                            <div class="xp-copy-bar-inner" style="width: {pct}%;"></div>
                                          </div>
                                          <div class="xp-copy-stats">{current:,} of {total:,} records &nbsp;|&nbsp; {pct}%</div>
                                        </div>
                                        ''', unsafe_allow_html=True)

                                count, data_size_mb = transfer_data_with_progress(
                                    sf=sf,
                                    adb=adb,
                                    object_name=item_name,
                                    collection_name=col_name,
                                    field_mappings=field_map,
                                    is_edge=is_edge,
                                    progress_callback=progress_callback
                                )
                                
                                actual = collection.count()
                                total_size_mb += data_size_mb

                                st.session_state.migration_status[item_name] = "success"
                                st.session_state.migration_progress[item_name] = 100
                                st.session_state.migration_metrics[item_name] = {
                                    "records": actual
                                }
                                
                                item_progress.progress(1.0)
                                with progress_containers[item_name]:
                                    st.markdown(f"""
                                    <div class="config-card">
                                        <h4>✅ Migration Complete</h4>
                                        <div class="metric-card">
                                            <div class="metric-value">{actual:,}</div>
                                            <div class="metric-label">Records Transferred</div>
                                        </div>
                                        <div class="status-indicator status-success">Successfully Completed</div>
                                    </div>
                                    """, unsafe_allow_html=True)

                            except Exception as e:
                                st.session_state.migration_status[item_name] = "error"
                                with progress_containers[item_name]:
                                    st.markdown(f"""
                                    <div class="config-card">
                                        <h4>❌ Migration Failed</h4>
                                        <div class="status-indicator status-error">Error: {str(e)[:100]}...</div>
                                    </div>
                                    """, unsafe_allow_html=True)
                                logger.error(f"Migration failed: {e}")

                        elif migration_direction == "ArangoDB → Salesforce":
                            # ArangoDB → Salesforce
                            item_name, fields, target_object, operation_type, external_id_field = item_input
                            
                            if not fields:
                                st.warning(f"⚠️ No fields selected for `{item_name}` - skipping")
                                continue

                            st.session_state.migration_status[item_name] = "processing"

                            with progress_containers[item_name]:
                                st.markdown(f"""
                                <div class="config-card">
                                    <h4>🔄 Migrating {item_name} → {target_object}</h4>
                                    <div class="transfer-animation"></div>
                                    <div class="status-indicator status-processing">Processing...</div>
                                </div>
                                """, unsafe_allow_html=True)

                            item_progress = st.progress(0)
                            item_status = st.empty()

                            try:
                                field_map = {f: f for f in fields}

                                def progress_callback(current, total):
                                    pct = int((current / total) * 100) if total > 0 else 0
                                    st.session_state.migration_progress[item_name] = pct
                                    item_progress.progress(pct / 100)
                                    
                                    # Enhanced progress display with animations
                                    with progress_containers[item_name]:
                                        st.markdown(f'''
                                        <div class="xp-copy-area">
                                          <div class="xp-copy-label">Transferring data...</div>
                                          <div class="xp-copy-icons">
                                            <span class="xp-copy-folder">🗄️</span>
                                            <span class="xp-copy-arrow">➡️</span>
                                            <span class="xp-copy-file">☁️</span>
                                          </div>
                                          <div class="xp-copy-bar">
                                            <div class="xp-copy-bar-inner" style="width: {pct}%;"></div>
                                          </div>
                                          <div class="xp-copy-stats">{current:,} of {total:,} records &nbsp;|&nbsp; {pct}%</div>
                                        </div>
                                        ''', unsafe_allow_html=True)

                                count, data_size_mb = transfer_arango_to_salesforce_with_progress(
                                    adb=adb,
                                    sf=sf,
                                    collection_name=item_name,
                                    object_name=target_object,
                                    field_mappings=field_map,
                                    operation_type=operation_type,
                                    external_id_field=external_id_field,
                                    progress_callback=progress_callback
                                )
                                
                                total_size_mb += data_size_mb

                                st.session_state.migration_status[item_name] = "success"
                                st.session_state.migration_progress[item_name] = 100
                                st.session_state.migration_metrics[item_name] = {
                                    "records": count
                                }
                                
                                item_progress.progress(1.0)
                                with progress_containers[item_name]:
                                    st.markdown(f"""
                                    <div class="config-card">
                                        <h4>✅ Migration Complete</h4>
                                        <div class="metric-card">
                                            <div class="metric-value">{count:,}</div>
                                            <div class="metric-label">Records Transferred</div>
                                        </div>
                                        <div class="status-indicator status-success">Successfully Completed</div>
                                    </div>
                                    """, unsafe_allow_html=True)

                            except Exception as e:
                                st.session_state.migration_status[item_name] = "error"
                                with progress_containers[item_name]:
                                    st.markdown(f"""
                                    <div class="config-card">
                                        <h4>❌ Migration Failed</h4>
                                        <div class="status-indicator status-error">Error: {str(e)[:100]}...</div>
                                    </div>
                                    """, unsafe_allow_html=True)
                                logger.error(f"Migration failed: {e}")

                        completed_items += 1
                        overall_progress.progress(completed_items / len(item_inputs))
                        overall_status.text(f"Completed {completed_items} of {len(item_inputs)}")

                    # Enhanced Migration Completion
                    st.markdown("""
                    <div class="config-card" style="text-align: center; background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);">
                        <h2>🎉 Migration Complete!</h2>
                        <div class="metric-card" style="display: inline-block; margin: 1rem;">
                            <div class="metric-value">{}</div>
                            <div class="metric-label">Objects Processed</div>
                        </div>
                    </div>
                    """.format(len(item_inputs)), unsafe_allow_html=True)
                    
                    # Display summary metrics
                    st.markdown("### 📊 Migration Summary")
                    summary_data = []
                    for i, item_input in enumerate(item_inputs):
                        item_name = item_input[0]  # First element is always the name
                        if item_name in st.session_state.migration_metrics:
                            metrics = st.session_state.migration_metrics[item_name]
                        if metrics:
                                if migration_direction == "Salesforce → ArangoDB":
                                    _, _, col_name, _ = item_input
                                    summary_data.append({
                                                "Source Object": item_name,
                                                "Target Collection": col_name,
                                                "Records": f"{metrics['records']:,}"
                                            })
                                else:
                                    _, _, target_object, _, _ = item_input
                                    summary_data.append({
                                        "Source Collection": item_name,
                                        "Target Object": target_object,
                                        "Records": f"{metrics['records']:,}"
                                    })
                    
                    if summary_data:
                        summary_df = pd.DataFrame(summary_data)
                        st.dataframe(summary_df, use_container_width=True)
                        
                except Exception as e:
                    st.error(f"❌ Migration setup failed: {e}")
                    logger.error(f"Migration setup failed: {e}")
else:
    st.info("🔌 Please connect to services using the sidebar to begin.")
    st.markdown("""
    ### 🚀 Getting Started
    1. **Fill in your credentials** in the sidebar
    2. **Connect to services** to verify connections
    3. **Choose migration direction** (Salesforce → ArangoDB or ArangoDB → Salesforce)
    4. **Select objects/collections** to migrate
    5. **Preview data** and test queries
    6. **Start migration** when ready
    
    **Note:** For ArangoDB → Salesforce migration, you can only transfer data to existing Salesforce objects. New objects cannot be created.
    """)

