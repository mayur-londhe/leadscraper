import streamlit as st
import asyncio
import sys
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import os
import pandas as pd
from datetime import datetime
import nest_asyncio
import os
import sys
import subprocess

def ensure_playwright_installed():
    try:
        import playwright
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright"])

    subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"])

ensure_playwright_installed()

nest_asyncio.apply()

from lead_pipeline import (
    run_step1, run_step2, run_step3, 
    STEP1_OUTPUT, STEP2_OUTPUT, STEP3_OUTPUT, 
    CITIES, SESSION_FILE
)

st.set_page_config(page_title="AAC Blocks Lead Pipeline", layout="wide", page_icon="🏗️")

# --- UI HEADER ---
st.title("🏗️ Unified Lead Pipeline")
st.markdown("### IndiaMart Scraper + Google Places + Gemini AI")

# --- SIDEBAR CONFIGURATION ---
st.sidebar.header("🔑 API Credentials")
gemini_key = st.secrets.get("GEMINI_API_KEY", os.getenv("GEMINI_API_KEY", ""))
places_key = st.secrets.get("PLACES_API_KEY", os.getenv("PLACES_API_KEY", ""))
if gemini_key and places_key:
    st.sidebar.success("✅ API keys loaded from environment or secrets")
else:
    st.sidebar.warning("Set GEMINI_API_KEY and PLACES_API_KEY in Streamlit secrets or your environment")

st.sidebar.divider()
st.sidebar.header("📱 Scraper Login")
# If session exists, we can show a status instead of forcing mobile input
if os.path.exists(SESSION_FILE):
    st.sidebar.success("✅ Login Session Found")
    mobile_number = st.sidebar.text_input("Mobile (for backup)", value="0000000000")
else:
    mobile_number = st.sidebar.text_input("IndiaMart Mobile", placeholder="10-digit number")

st.sidebar.divider()
st.sidebar.info("Tip: Keep the server terminal/browser window visible to monitor the scraping process.")

# --- MAIN CONTROLS ---
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("🛠️ Pipeline Parameters")
    
    c_step1, c_step2 = st.columns(2)
    with c_step1:
        prod_slug = st.text_input("Product Slug", value="hollow-blocks", help="Used in IndiaMart URL (e.g., tiles, bricks)")
        target_cities = st.multiselect("Target Cities", options=CITIES, default=CITIES)
    
    with c_step2:
        g_search_terms = st.text_input("Google Search Terms", value="building materials", help="Added to company name for better Google results")
        batch_size = 10

    # Filtering Options
    st.divider()
    st.subheader("🔍 Filters & Modes")
    f_col1, f_col2 = st.columns(2)
    with f_col1:
        selected_steps = st.multiselect("Steps to Execute", options=[1, 2, 3], default=[1, 2, 3], format_func=lambda x: f"Step {x}")
    with f_col2:
        test_mode = st.checkbox("Test Mode (Limit to 10 rows)", value=False)
        filter_city = st.selectbox("Filter Results by City", options=[""] + CITIES, index=0)

with col2:
    st.subheader("🔑 OTP Bridge")
    st.write("If IndiaMart asks for an OTP, enter it here:")
    otp_input = st.text_input("4-Digit OTP", max_chars=4, placeholder="----")
    if st.button("Submit OTP to Scraper"):
        if len(otp_input) == 4:
            with open("otp_signal.txt", "w") as f:
                f.write(otp_input)
            st.success("✅ OTP sent! Check the scraper window.")
        else:
            st.error("Enter a valid 4-digit code.")

# --- EXECUTION ---
st.divider()

if st.button("🚀 START PIPELINE", use_container_width=True):
    if not gemini_key or not places_key:
        st.warning("⚠️ Please provide both API keys in the sidebar.")
    else:
        # Update OS Environment so your existing script can read them
        os.environ["GEMINI_API_KEY"] = gemini_key
        os.environ["PLACES_API_KEY"] = places_key
        
        status_area = st.empty()
        log_area = st.expander("Detailed Logs", expanded=True)
        
        try:
            step1_csv = STEP1_OUTPUT
            step2_csv = STEP2_OUTPUT

            # --- STEP 1 ---
            # In app.py, replace the asyncio.run line with this:
            if 1 in selected_steps:
                status_area.info("🏃 Step 1: Scraping IndiaMart...")
                print("[DEBUG] Calling run_step1")
                asyncio.run(run_step1(mobile_number, product_slug=prod_slug, cities=target_cities))
                st.toast("Step 1 Complete!")
                status_area.info("🏃 Step 1: Scraping IndiaMart... (Watch for Browser/OTP)")
                
            # --- STEP 2 ---
            if 2 in selected_steps:
                status_area.info("🏃 Step 2: Google Places Enrichment...")
                run_step2(
                    step1_csv, 
                    batch_size=batch_size, 
                    test_mode=test_mode, 
                    filter_city=filter_city, 
                    search_terms=g_search_terms
                )
                st.toast("Step 2 Complete!")

            # --- STEP 3 ---
            if 3 in selected_steps:
                status_area.info("🏃 Step 3: AI Business Classification...")
                final_file = run_step3(step2_csv, filter_city=filter_city, batch_size=batch_size)
                
                status_area.success(f"🎉 Pipeline Finished! File saved to: {final_file}")
                
                # Show results
                df_final = pd.read_csv(final_file)
                st.dataframe(df_final)
                st.download_button("📥 Download Result CSV", df_final.to_csv(index=False), "final_leads.csv", "text/csv")

        except Exception as e:
            st.error(f"❌ Error during execution: {str(e)}")

# --- DATA PREVIEW ---
if os.path.exists(STEP3_OUTPUT):
    with st.expander("📊 Preview Latest Final Leads"):
        st.dataframe(pd.read_csv(STEP3_OUTPUT).head(20))