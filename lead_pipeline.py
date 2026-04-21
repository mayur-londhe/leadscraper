"""
╔══════════════════════════════════════════════════════════════════╗
║   UNIFIED LEAD PIPELINE — AAC Blocks / IndiaMart                ║
╠══════════════════════════════════════════════════════════════════╣
║  STEP 1 → IndiaMart scraper  (Playwright)                       ║
║  STEP 2 → Google Places enrichment (Places API + ClearTax)      ║
║  STEP 3 → Business-type classification via Gemini               ║
╚══════════════════════════════════════════════════════════════════╝

Usage:
  python lead_pipeline.py
  
  ✨ The script will ask you for configuration interactively:
     - Product slug (e.g., concrete-blocks, tiles, bricks)
     - Search terms for Google Places (e.g., building materials)
     - Optional: Filter by city
     - Batch size for API calls
     - Test mode (first 10 rows only)
     - Which steps to run (1=scrape, 2=enrich, 3=classify)

Examples of what you can do:
  • Scrape IndiaMart for hollow blocks: slug=hollow-blocks, search="hollow blocks"
  • Enrich tiles data: slug=tiles, search="tiles materials"
  • Filter to specific city: filter_city="Jodhpur"
  • Run only enrichment step: steps=2,3
"""

import asyncio
import csv
import os
import re
import time
import sys
from datetime import datetime
from google.genai import types  # <--- Add this
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st

from google import genai
import pandas as pd
import requests
import json


from dotenv import load_dotenv
load_dotenv()

STATE_TO_CODE = {
    "rajasthan": "08", "gujarat": "24", "maharashtra": "27", "delhi": "07",
    "uttar pradesh": "09", "haryana": "06", "punjab": "03", "karnataka": "29",
    "tamil nadu": "33", "west bengal": "19", "telangana": "36", "bihar": "10"
}

# ── Lazy-import heavy deps so missing packages fail loudly ──────────────────
def _import_playwright():
    try:
        from playwright.async_api import async_playwright
        return async_playwright
    except ImportError:
        sys.exit("❌  Playwright not installed. Run: pip install playwright && playwright install chromium")

def _import_selenium():
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from webdriver_manager.chrome import ChromeDriverManager
        return webdriver, Service, Options, By, WebDriverWait, EC, ChromeDriverManager
    except ImportError:
        sys.exit("❌  Selenium not installed. Run: pip install selenium webdriver-manager")



# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION  — edit keys / cities / file names here
# ══════════════════════════════════════════════════════════════════════════════

GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", os.getenv("GEMINI_API_KEY", ""))
PLACES_API_KEY = st.secrets.get("PLACES_API_KEY", os.getenv("PLACES_API_KEY", ""))
PLACES_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

if not GEMINI_API_KEY:
    sys.exit("❌ GEMINI_API_KEY environment variable not set")
if not PLACES_API_KEY:
    sys.exit("❌ PLACES_API_KEY environment variable not set")

# Near the top of your script where you define the client:
client = genai.Client(api_key=GEMINI_API_KEY, http_options={'api_version': 'v1alpha'})

# Define the search tool
search_tool = {"google_search": {}}

# IndiaMart target cities & product keyword
CITIES = [
    "Jodhpur","Solapur","Karur","Vellore",
]
INDIAMART_PRODUCT_SLUG = "hollow-blocks"     # used in URL: /<city>/<slug>.html

# File names
SCRIPT_DIR          = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR          = os.path.join(SCRIPT_DIR, "indiamart_leads")
SESSION_FILE        = os.path.join(SCRIPT_DIR, "session.json")
STEP1_OUTPUT        = os.path.join(OUTPUT_DIR, "step1_scraped.csv")
STEP2_UNFILTERED    = os.path.join(OUTPUT_DIR, "step2_unfiltered.csv")
STEP2_OUTPUT        = os.path.join(OUTPUT_DIR, "step2_enriched.csv")
STEP3_OUTPUT        = os.path.join(OUTPUT_DIR, "step3_final.csv")

# Filter criteria for step 2 (eliminate rows matching any condition)
FILTER_CITY        = "jodhpur"        # case-insensitive; set "" to skip city filter

#
#  SHARED CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
GST_RE      = re.compile(r'\b\d{2}[A-Z]{5}\d{4}[A-Z][A-Z0-9]Z[A-Z0-9]\b')
CONCURRENCY = 8   # parallel card extraction (Step 1)

CARD_SEL = (
    "li.temp4-card, "
    "li.pCard1, "
    "li.dfd.grid-card, "
    "article.template7-product-card"
)

GST_STATE_MAP = {
    "01": "Jammu and Kashmir", "02": "Himachal Pradesh", "03": "Punjab",
    "04": "Chandigarh",        "05": "Uttarakhand",      "06": "Haryana",
    "07": "Delhi",             "08": "Rajasthan",        "09": "Uttar Pradesh",
    "10": "Bihar",             "11": "Sikkim",           "12": "Arunachal Pradesh",
    "13": "Nagaland",          "14": "Manipur",          "15": "Mizoram",
    "16": "Tripura",           "17": "Meghalaya",        "18": "Assam",
    "19": "West Bengal",       "20": "Jharkhand",        "21": "Odisha",
    "22": "Chhattisgarh",      "23": "Madhya Pradesh",   "24": "Gujarat",
    "25": "Daman and Diu",     "26": "Dadra and Nagar Haveli", "27": "Maharashtra",
    "28": "Andhra Pradesh",    "29": "Karnataka",        "30": "Goa",
    "31": "Lakshadweep",       "32": "Kerala",           "33": "Tamil Nadu",
    "34": "Puducherry",        "35": "Andaman and Nicobar", "36": "Telangana",
    "37": "Andhra Pradesh",    "38": "Ladakh",
}


# ══════════════════════════════════════════════════════════════════════════════
#
#  STEP 1 — IndiaMart Scraper  (Playwright / async)
#
# ══════════════════════════════════════════════════════════════════════════════

# ── Login helpers ─────────────────────────────────────────────────────────────

async def is_logged_in(page) -> bool:
    try:
        sign_out = await page.query_selector("a.dfusr, a:has-text('Sign Out')")
        if sign_out and await sign_out.is_visible():
            return True
        user_area = await page.query_selector(".usrnm, .prf-dtl")
        if user_area:
            return True
    except Exception:
        pass
    return False


async def wait_for_otp_boxes(page, timeout_secs=8):
    for _ in range(timeout_secs):
        for frame in [page] + list(page.frames):
            try:
                boxes   = await frame.query_selector_all("input[maxlength='1']")
                visible = [b for b in boxes if await b.is_visible()]
                if len(visible) >= 4:
                    return visible[:4], frame
            except Exception:
                pass
        await page.wait_for_timeout(1000)
    return [], page


async def dismiss_modal(page):
    await page.wait_for_timeout(600)
    await page.keyboard.press("Escape")
    await page.wait_for_timeout(400)
    for sel in [
        "button:has-text('×')", "button:has-text('✕')", "button:has-text('Close')",
        "button.close", "button[aria-label='Close']", "[class*='close']",
        "[class*='modal-close']", "[class*='popup-close']",
    ]:
        try:
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                await el.click()
                await page.wait_for_timeout(400)
                break
        except Exception:
            pass


async def fill_otp(page, mobile, otp_boxes, otp_frame):
    SIGNAL_FILE = "otp_signal.txt"
    
    # Clear any old signals before starting
    if os.path.exists(SIGNAL_FILE):
        os.remove(SIGNAL_FILE)

    print(f"\n📡 WAITING FOR OTP: Please enter the code for +91-{mobile} in the Web UI...")
    
    # --- UI BRIDGE LOOP ---
    otp = ""
    timeout = 120  # 2 minutes for your colleague to notice and type it
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        if os.path.exists(SIGNAL_FILE):
            try:
                with open(SIGNAL_FILE, "r") as f:
                    otp = f.read().strip()
                if len(otp) == 4:
                    os.remove(SIGNAL_FILE) # Clean up after reading
                    print(f"  [✓] Received OTP from UI: {otp}")
                    break
            except Exception:
                pass # Handle file-access collisions
        
        await asyncio.sleep(1) # Don't burn CPU while waiting
    
    if not otp:
        print("  [!] OTP Timeout: No code received from Web UI.")
        return False

    # --- FILLING LOGIC ---
    if otp_boxes:
        for i, digit in enumerate(otp[:4]):
            try:
                await otp_boxes[i].click()
                await page.wait_for_timeout(100)
                await otp_boxes[i].fill(digit)
                await page.wait_for_timeout(100)
            except Exception as e:
                print(f"  [!] Error filling OTP box {i}: {e}")
        print("  [✓] OTP digits entered")
    else:
        # Fallback for single-input fields
        for sel in ["input[name='otp']", "input[placeholder*='OTP']", "input[maxlength='4']"]:
            try:
                f = await otp_frame.query_selector(sel)
                if f and await f.is_visible():
                    await f.fill(otp)
                    break
            except Exception:
                pass

    # --- VERIFICATION LOGIC ---
    await page.wait_for_timeout(400)
    for sel in [
        "button:has-text('Verify & Login')", "button:has-text('Verify')",
        "a:has-text('Verify')", "button:has-text('Submit')",
        "button:has-text('Login')", "button[type='submit']",
    ]:
        try:
            btn = await otp_frame.query_selector(sel)
            if btn and await btn.is_visible():
                await btn.click()
                print(f"  [✓] Verify clicked: {sel}")
                break
        except Exception:
            pass
    else:
        await page.keyboard.press("Enter")

    # --- SUCCESS CHECK ---
    for _ in range(15):
        await page.wait_for_timeout(1000)
        if await is_logged_in(page):
            print("  [✓] Login successful!\n")
            await dismiss_modal(page)
            return True

    await dismiss_modal(page)
    print("  [!] Login status uncertain — continuing.")
    return False

async def do_login(page, mobile):
    if await is_logged_in(page):
        print("[*] Already logged in — skipping.")
        return True
    print("[*] Starting login...")
    for sel in ["a.ico-usr", "a.rmv.cpo.ico-usr", "span:has-text('Sign In')", "a:has-text('Sign In')"]:
        try:
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                await el.click()
                print(f"  [✓] Sign In clicked: {sel}")
                break
        except Exception:
            pass
    await page.wait_for_timeout(2500)
    MOBILE_SELS = [
        "input#mobile", "input.fw_fn-1",
        "input[placeholder='Enter Your Mobile Number']",
        "input[placeholder*='Mobile Number']",
        "input[placeholder*='Mobile No']",
        "input.un2_s", "input[maxlength='10'][type='text']",
    ]
    mobile_input = None
    for _ in range(15):
        for sel in MOBILE_SELS:
            try:
                el = await page.query_selector(sel)
                if el and await el.is_visible():
                    mobile_input = el
                    print(f"  [✓] Mobile input: {sel}")
                    break
            except Exception:
                pass
        if mobile_input:
            break
        await page.wait_for_timeout(1000)
    if not mobile_input:
        print("  [!] Mobile input not found.")
        return False
    await mobile_input.click(click_count=3)
    await mobile_input.fill("")
    await page.wait_for_timeout(150)
    await mobile_input.type(mobile, delay=60)
    print(f"  [✓] Typed: {mobile}")
    await page.wait_for_timeout(400)
    submitted = False
    for sel in [
        "button.un2_s_btn", "a.un2_s_btn", "button[class*='un2']",
        "button:has-text('Send OTP')", "button:has-text('Get OTP')",
        "a:has-text('Send OTP')", "input[type='submit']",
    ]:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                await btn.click()
                submitted = True
                break
        except Exception:
            pass
    if not submitted:
        await mobile_input.press("Enter")
    await page.wait_for_timeout(2500)
    otp_boxes, otp_frame = await wait_for_otp_boxes(page, timeout_secs=8)
    if otp_boxes:
        print("  [✓] OTP boxes appeared.")
        await fill_otp(page, mobile, otp_boxes, otp_frame)
        return True
    else:
        print("  [*] OTP boxes not yet visible.")
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(400)
        return False


# ── Card loading ──────────────────────────────────────────────────────────────

async def load_all_cards(page, mobile):
    SHOW_MORE_SELS = [
        "button:has-text('Show More Products')", "button:has-text('Show more products')",
        "button:has-text('Show More Results')", "button:has-text('Show more results')",
        "button:has-text('Show More')", "button:has-text('Load More')",
        "button.showMoreBtn", "[class*='showMore']", "[class*='loadMore']",
    ]
    round_num = 0
    stuck     = 0
    MAX_STUCK = 4
    initial   = len(await page.query_selector_all(CARD_SEL))
    print(f"[*] Loading all cards (initial: {initial})...")
    while True:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1200)
        otp_boxes, otp_frame = await wait_for_otp_boxes(page, timeout_secs=2)
        if otp_boxes:
            print("\n  [*] OTP prompt appeared!")
            await fill_otp(page, mobile, otp_boxes, otp_frame)
            await page.wait_for_timeout(1500)
            continue
        for sel in ["button:has-text('Send OTP')", "a:has-text('Send OTP')", "button:has-text('Get OTP')"]:
            try:
                btn = await page.query_selector(sel)
                if btn and await btn.is_visible():
                    await btn.click()
                    await page.wait_for_timeout(2500)
                    ob2, of2 = await wait_for_otp_boxes(page, timeout_secs=10)
                    await fill_otp(page, mobile, ob2, of2)
                    await page.wait_for_timeout(1500)
                    break
            except Exception:
                pass
        btn_found = False
        for sel in SHOW_MORE_SELS:
            try:
                btn = await page.query_selector(sel)
                if btn and await btn.is_visible():
                    round_num += 1
                    before = len(await page.query_selector_all(CARD_SEL))
                    await btn.scroll_into_view_if_needed()
                    await page.wait_for_timeout(200)
                    await btn.click()
                    await page.wait_for_timeout(2500)
                    after  = len(await page.query_selector_all(CARD_SEL))
                    gained = after - before
                    print(f"  Click #{round_num:>3} │ Cards: {after:>4} (+{gained})")
                    if gained == 0:
                        stuck += 1
                        if stuck >= MAX_STUCK:
                            print(f"\n[*] No new cards after {MAX_STUCK} clicks.")
                            return after
                    else:
                        stuck = 0
                    btn_found = True
                    break
            except Exception:
                pass
        if not btn_found:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1500)
            still_there = False
            for sel in SHOW_MORE_SELS:
                try:
                    btn = await page.query_selector(sel)
                    if btn and await btn.is_visible():
                        still_there = True
                        break
                except Exception:
                    pass
            if not still_there:
                final = len(await page.query_selector_all(CARD_SEL))
                print(f"\n[✓] All {final} cards loaded ({round_num} clicks).")
                return final
        if round_num >= 50:
            print("[*] 50-click safety cap reached.")
            return len(await page.query_selector_all(CARD_SEL))


# ── Card data extraction (exact JS-evaluate approach from original) ───────────

async def extract_card(card_el):
    return await card_el.evaluate("""(card) => {
        // ── Company ──────────────────────────────────────────────────────────
        const coEl       = card.querySelector('a.cncf1, a[class*="cncf"]');
        const company    = coEl ? coEl.innerText.trim() : 'N/A';
        const companyUrl = coEl ? (coEl.getAttribute('href') || '') : '';

        // ── Product ───────────────────────────────────────────────────────────
        const prodEl     = card.querySelector('a.prdtitle, h2 a');
        const product    = prodEl ? prodEl.innerText.trim() : 'N/A';
        const productUrl = prodEl ? (prodEl.getAttribute('href') || '') : '';

        // ── Price ─────────────────────────────────────────────────────────────
        const prcEl  = card.querySelector('.prc');
        const unitEl = card.querySelector('.prcut');
        const price  = prcEl
            ? (prcEl.innerText.trim() + (unitEl ? ' / ' + unitEl.innerText.trim() : ''))
            : 'N/A';

        // ── Location ──────────────────────────────────────────────────────────
        const locEl    = card.querySelector('address span');
        const location = locEl ? locEl.innerText.trim() : 'N/A';

        // ── Trust badges ──────────────────────────────────────────────────────
        let memberSince = 'N/A', responseRate = 'N/A', trustSeal = 'No', gstBadge = 'No';

        const allSpans = [...card.querySelectorAll('span, div')];
        for (const el of allSpans) {
            const icon = el.querySelector('i');
            const cls  = icon ? (icon.className || '') : '';
            const txt  = (el.innerText || '').trim();
            if      (cls.includes('memSincD'))  memberSince = txt.replace(/[^\\d\\s\\/yrsmonth]/gi, '').trim() || txt;
            else if (cls.includes('tvfSlrD'))   trustSeal   = 'Yes';
            else if (cls.includes('greengstD')) gstBadge    = 'Yes';
        }

        // ── Template 7: member since uses SVG, not i.memSincD ────────────────
        if (memberSince === 'N/A') {
            const svgEl = card.querySelector('svg.template7-member-since-watch');
            if (svgEl) {
                const txt = (svgEl.nextSibling || {}).textContent || '';
                const trimmed = txt.trim();
                if (trimmed) memberSince = trimmed;
            }
        }

        // Response rate — dedicated span (new layouts)
        const rrEl = card.querySelector('.response-rate-text, [class*="response"]');
        if (rrEl) {
            const rrTxt = rrEl.innerText.trim();
            if (rrTxt.includes('%')) responseRate = rrTxt;
        }

        // Fall back to old .trstelems approach
        card.querySelectorAll('.trstelems span').forEach(span => {
            const txt  = span.innerText.trim();
            const icon = span.querySelector('i');
            const cls  = icon ? (icon.className || '') : '';
            if      (cls.includes('memSincD'))  memberSince  = txt;
            else if (cls.includes('tvfSlrD'))   trustSeal    = 'Yes';
            else if (cls.includes('greengstD')) gstBadge     = 'Yes';
            else if (txt.includes('%'))         responseRate = txt;
        });

        // ── Rating ────────────────────────────────────────────────────────────
        const ratingEl = card.querySelector('.dag5');
        const rating   = ratingEl
            ? ratingEl.innerText.split(' ').filter(x => x).join(' ').trim()
            : 'N/A';

        // ── Supplier badge ────────────────────────────────────────────────────
        const badgeEl = card.querySelector('.badge');
        const badge   = badgeEl ? badgeEl.innerText.trim() : '';

        // ── dispId ────────────────────────────────────────────────────────────
        const ctaDiv = card.querySelector('[id^="dispId"]');
        const dispId = ctaDiv ? ctaDiv.id.replace('dispId', '') : '';

        return {
            company, companyUrl, product, productUrl, price, location,
            memberSince, responseRate, trustSeal, gstBadge, rating, badge, dispId,
        };
    }""")


async def fetch_enquiry_page(page, company_url):
    if not company_url or not company_url.startswith("http"):
        return "N/A", "N/A", "N/A", "N/A"
    base = company_url.rstrip("/")

    REVEAL_SELS = [
        "a:has-text('View Mobile Number')",
        "button:has-text('View Mobile Number')",
        "a:has-text('View Contact')",
        "button:has-text('View Contact')",
        "a[class*='view-mobile']", "button[class*='view-mobile']",
        ".cntct_btn", ".contact_btn",
        "[class*='callNow']", "[class*='viewNumber']", "[class*='view_number']",
        "a:has-text('Call')", "button:has-text('Call')",
    ]

    def _digits(raw):
        d = re.sub(r"\D", "", raw or "")
        return d[-10:] if len(d) >= 10 else None

    async def _scrape(url):
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=14000)
            if resp and resp.status == 404:
                return "N/A", "N/A", "N/A", "N/A"
            try:
                await page.wait_for_selector("a[href^='tel:'], a:has-text('Call')", timeout=5000)
            except Exception:
                await page.wait_for_timeout(1500)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(500)
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(150)

            r = await page.evaluate("""() => {
                let phone = '';
                for (const a of document.querySelectorAll('a[href^="tel:"]')) {
                    const digits = a.href.replace('tel:', '').replace(/\\D/g, '');
                    if (digits.length >= 10) { phone = digits.slice(-10); break; }
                }
                if (!phone) {
                    const txt = document.body.innerText || '';
                    const m = txt.match(/\\bCall\\s+(0?[6-9]\\d{9})\\b/i) ||
                              txt.match(/(?:Mobile|Phone|Contact|Ph)[\\s.:]*0?([6-9]\\d{9})/i) ||
                              txt.match(/\\b([6-9]\\d{9})\\b/);
                    if (m) phone = m[1].replace(/\\D/g, '').slice(-10);
                }
                const gstM = (document.body.innerText || '').match(
                    /\\b\\d{2}[A-Z]{5}\\d{4}[A-Z][A-Z0-9]Z[A-Z0-9]\\b/
                );
                const mapsA = [...document.querySelectorAll('a[href]')].find(a => {
                    const h = (a.href || '').toLowerCase();
                    const t = (a.innerText || a.textContent || '').toLowerCase();
                    return h.includes('maps.google') || h.includes('goo.gl/maps') ||
                           h.includes('maps.app.goo') || /get.?direction/i.test(t) || /view.?map/i.test(t);
                });
                let reachAddr = 'N/A';
                const reachUsHeader = [...document.querySelectorAll('p')]
                    .find(el => /Reach\\s+Us/i.test(el.innerText));
                if (reachUsHeader) {
                    const container = reachUsHeader.nextElementSibling;
                    const addrEl = container ? container.querySelector('p.FM_Lsp4') : null;
                    if (addrEl) {
                        reachAddr = addrEl.innerText.replace(/\\n/g, ', ').trim();
                    }
                }
                return {
                    phone: phone || '',
                    gst: gstM ? gstM[0] : 'N/A',
                    maps: mapsA ? mapsA.href : 'N/A',
                    reachUsAddress: reachAddr
                };
            }""")

            ph = _digits(r["phone"]) or "N/A"
            gv = r["gst"]
            mv = r["maps"]
            rv = r["reachUsAddress"]

            # Reveal hidden phone numbers
            if ph == "N/A":
                for sel in REVEAL_SELS:
                    try:
                        btn = await page.query_selector(sel)
                        if btn and await btn.is_visible():
                            await btn.scroll_into_view_if_needed()
                            await btn.click()
                            for _ in range(6):
                                await page.wait_for_timeout(500)
                                tel2 = await page.evaluate("""() => {
                                    for (const a of document.querySelectorAll('a[href^="tel:"]')) {
                                        const d = a.href.replace('tel:','').replace(/\\D/g,'');
                                        if (d.length >= 10) return d.slice(-10);
                                    }
                                    return '';
                                }""")
                                d = _digits(tel2)
                                if d:
                                    ph = d
                                    break
                            break
                    except Exception:
                        pass

            return ph, gv, mv, rv
        except Exception:
            return "N/A", "N/A", "N/A", "N/A"

    # Try enquiry page first, fall back to base URL
    phone, gst, maps, reach_us = await _scrape(f"{base}/enquiry.html")
    if phone == "N/A" and gst == "N/A" and reach_us == "N/A":
        phone, gst, maps, reach_us = await _scrape(base)
    return phone, gst, maps, reach_us


async def fetch_about_us(page, company_url):
    data = {
        "CEO": "N/A", "Annual Turnover": "N/A", "Employees": "N/A",
        "GST Reg Date": "N/A", "Registered Address": "N/A",
        "Legal Status": "N/A", "Nature of Business": "N/A",
        "GST Number": "N/A", "Brands We Deal In": "N/A",
    }
    if not company_url or not company_url.startswith("http"):
        return data
    base = company_url.rstrip("/")
    for url in [f"{base}/profile.html", f"{base}/aboutus.html", base]:
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=10000)
            if resp and resp.status == 404:
                continue
            await page.wait_for_timeout(1500)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(600)
            await page.evaluate("window.scrollTo(0, 0)")
            r = await page.evaluate("""() => {
                const getVal = (label) => {
                    const tds = [...document.querySelectorAll('td')];
                    for (let i = 0; i < tds.length-1; i++) {
                        if ((tds[i].innerText||'').trim().replace(/\\s+/g,' ') === label)
                            return (tds[i+1].innerText||'').trim();
                    }
                    const all = [...document.querySelectorAll('li,dt,span,p,div')];
                    for (let i = 0; i < all.length-1; i++) {
                        if ((all[i].innerText||'').trim() === label) {
                            const n = all[i].nextElementSibling;
                            if (n) return (n.innerText||'').trim();
                        }
                    }
                    return 'N/A';
                };
                let brands = 'N/A';
                const h2 = [...document.querySelectorAll('h2')].find(
                    h => h.innerText.trim() === 'Brands We Deal In'
                );
                if (h2) {
                    const c = h2.closest('.FM_pflePrd') || h2.parentElement;
                    const items = c ? [...c.querySelectorAll('ul li')] : [];
                    if (items.length) brands = items.map(li => li.innerText.trim()).filter(Boolean).join('\\n');
                }
                return {
                    CEO:               getVal('Company CEO'),
                    AnnualTurnover:    getVal('Annual Turnover'),
                    Employees:         getVal('Total Number of Employees'),
                    GSTRegDate:        getVal('GST Registration Date'),
                    RegisteredAddress: getVal('Registered Address'),
                    LegalStatus:       getVal('Legal Status of Firm'),
                    NatureOfBusiness:  getVal('Nature of Business'),
                    GSTNumber:         getVal('GST No.'),
                    BrandsWeDeaIn:     brands,
                };
            }""")
            for key, field in [
                ("CEO",               "CEO"),
                ("AnnualTurnover",    "Annual Turnover"),
                ("Employees",         "Employees"),
                ("GSTRegDate",        "GST Reg Date"),
                ("RegisteredAddress", "Registered Address"),
                ("LegalStatus",       "Legal Status"),
                ("NatureOfBusiness",  "Nature of Business"),
                ("GSTNumber",         "GST Number"),
                ("BrandsWeDeaIn",     "Brands We Deal In"),
            ]:
                if r[key] != "N/A":
                    data[field] = r[key]
            if data["CEO"] != "N/A" or data["GST Number"] != "N/A":
                break
        except Exception:
            continue
    return data


async def fetch_product_page(page, product_url):
    """Returns (specs, brochure_url, im_profile_url) — all in one product page visit."""
    specs = "N/A"; brochure = "N/A"; im_profile = ""
    if not product_url or not product_url.startswith("http"):
        return specs, brochure, im_profile
    try:
        resp = await page.goto(product_url, wait_until="domcontentloaded", timeout=12000)
        if resp and resp.status == 404:
            return specs, brochure, im_profile
        await page.wait_for_timeout(500)
        r = await page.evaluate("""() => {
            const specs = [...document.querySelectorAll('tr')]
                .map(tr => {
                    const cells = [...tr.querySelectorAll('td,th')];
                    if (cells.length < 2) return null;
                    const k = cells[0].innerText.trim();
                    const v = cells[1].innerText.trim();
                    if (!k || k.toLowerCase().includes('hindi')) return null;
                    return k + ': ' + v;
                }).filter(Boolean).join(' | ');
            let brochure = 'N/A', imProfile = '';
            try {
                const el = document.getElementById('__NEXT_DATA__');
                const pp = JSON.parse(el.textContent).props.pageProps;
                const d  = pp.serviceRes && pp.serviceRes.Data && pp.serviceRes.Data[0];
                if (d && pp.pathData && pp.pathData.show_pdf) {
                    const doc = (d.DOC_PATH||'').trim();
                    if (doc) brochure = doc.replace(/^http:/, 'https:');
                }
                if (d && d.URL && d.URL.includes('indiamart.com')) imProfile = d.URL;
            } catch(e) {}
            return { specs: specs||'N/A', brochure, imProfile };
        }""")
        specs      = r.get("specs",     "N/A")
        brochure   = r.get("brochure",  "N/A")
        im_profile = r.get("imProfile", "")
    except Exception:
        pass
    return specs, brochure, im_profile


async def fetch_all_trustseal_dates(page) -> dict:
    results = {}

    ts_cards = await page.evaluate("""() => {
        const cards = [...document.querySelectorAll('li.temp4-card, li.pCard1, li.dfd.grid-card, article.template7-product-card')];
        return cards
            .filter(el => el.querySelector('i.tvfSlrD'))
            .map(el => {
                const dispEl = el.querySelector('[id^="dispId"]');
                return dispEl ? { dispId: dispEl.id.replace('dispId', ''), name: el.innerText.split('\\n')[0] } : null;
            })
            .filter(c => c);
    }""")

    if not ts_cards:
        return results

    print(f"[*] Fetching TrustSEAL data for {len(ts_cards)} companies...")

    for card_info in ts_cards:
        disp_id = card_info["dispId"]
        name    = card_info["name"]
        try:
            ts_icon_wrapper = await page.evaluate_handle(f"""() => {{
                const dispEl = document.querySelector('[id="dispId{disp_id}"]');
                if (!dispEl) return null;
                const card = dispEl.closest('li, article');
                const icon = card.querySelector('i.tvfSlrD');
                return icon ? icon.parentElement : null;
            }}""")

            if not ts_icon_wrapper:
                continue

            popup_future = asyncio.get_event_loop().create_future()

            async def on_popup(popup_page):
                try:
                    await popup_page.wait_for_selector('.ts_cmpn_dtl', timeout=15000)
                    r = await popup_page.evaluate("""() => {
                        const getValByLabel = (label) => {
                            const items = [...document.querySelectorAll('.ts_cmpn_dtl li, footer li')];
                            const target = items.find(li => li.innerText.toLowerCase().includes(label.toLowerCase()));
                            const valEl = target ? target.querySelector('.fwm') : null;
                            return valEl ? valEl.innerText.trim() : 'N/A';
                        };
                        return {
                            issue:   getValByLabel('Issue date'),
                            expiry:  getValByLabel('Expiry date'),
                            address: getValByLabel('Business Address')
                        };
                    }""")
                    if not popup_future.done():
                        popup_future.set_result(r)
                except Exception:
                    if not popup_future.done():
                        popup_future.set_result({"issue": "N/A", "expiry": "N/A", "address": "N/A"})
                finally:
                    await popup_page.close()

            page.on("popup", on_popup)
            await ts_icon_wrapper.as_element().click()
            res = await asyncio.wait_for(popup_future, timeout=20)
            page.remove_listener("popup", on_popup)

            results[disp_id] = (res["issue"], res["expiry"], res["address"])

            print(f"\n[TRUST SEAL CERTIFICATE: {name}]")
            print(f"  > Full Address: {res['address']}")
            print(f"  > Issue Date  : {res['issue']}")
            print(f"  > Expiry Date : {res['expiry']}")
            print("-" * 30)

        except Exception:
            results[disp_id] = ("N/A", "N/A", "N/A")

    return results


async def deep_fetch_card(ctx, c):
    """Parallel fetch: enquiry page + about us + product page — three tabs at once."""
    enquiry_page = await ctx.new_page()
    about_page   = await ctx.new_page()
    prod_page    = await ctx.new_page()

    results = await asyncio.gather(
        fetch_enquiry_page(enquiry_page, c["companyUrl"]),
        fetch_about_us(about_page,       c["companyUrl"]),
        fetch_product_page(prod_page,    c["productUrl"]),
        return_exceptions=True,
    )

    await enquiry_page.close()
    await about_page.close()
    await prod_page.close()

    enquiry_r, about_r, prod_r = results

    phone, gst_val, maps, reach_us_addr = (
        enquiry_r if not isinstance(enquiry_r, Exception) else ("N/A", "N/A", "N/A", "N/A")
    )
    about = (
        about_r if not isinstance(about_r, Exception)
        else {"Registered Address": "N/A", "GST Number": "N/A"}
    )
    prod_specs, brochure, _ = (
        prod_r if not isinstance(prod_r, Exception) else ("N/A", "N/A", "")
    )

    return phone, gst_val, maps, about, prod_specs, brochure, reach_us_addr


async def scrape_city(ctx, page, city, mobile, output_csv, product_slug):
    city_slug  = city.lower()
    slug       = product_slug
    target_url = f"https://dir.indiamart.com/{city_slug}/{slug}.html"

    print(f"\n{'═'*52}")
    print(f"   City          : {city}")
    print(f"   URL           : {target_url}")
    print(f"   Output CSV    : {output_csv}")
    print(f"{'═'*52}\n")

    await page.goto(target_url, wait_until="domcontentloaded", timeout=20000)
    await page.wait_for_timeout(2000)

    # Re-navigate once if city slug disappeared (session flicker)
    if city_slug not in page.url:
        print("[*] Re-navigating to listing page...")
        await page.goto(target_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(2000)

    # ── REDIRECT DETECTION — skip city if IndiaMart sends us to a national page ──
    final_url = page.url
    if city_slug not in final_url:
        print(f"   [⚠] WARNING: IndiaMart redirected to national page for {city}.")
        print(f"       Final URL : {final_url}")
        print(f"       Skipping {city} — results would be national suppliers, not city-specific.\n")
        return 0

    print("[*] Waiting for cards to render...")
    try:
        await page.wait_for_selector(CARD_SEL, timeout=15000)
    except Exception:
        print(f"[!] Cards not found for {city}. Skipping.")
        return 0

    otp_boxes, otp_frame = await wait_for_otp_boxes(page, timeout_secs=3)
    if otp_boxes:
        await fill_otp(page, mobile, otp_boxes, otp_frame)

    total = await load_all_cards(page, mobile)
    print(f"\n[*] All {total} cards loaded for {city}.")

    trustseal_data = await fetch_all_trustseal_dates(page)
    print(f"[*] TrustSEAL data collected: {len(trustseal_data)} companies\n")

    card_els = await page.query_selector_all(CARD_SEL)
    seen     = set()
    sem      = asyncio.Semaphore(CONCURRENCY)

    async def process_card(idx, el):
        async with sem:
            try:
                c = await extract_card(el)
                if c["company"] == "N/A":
                    return None
                key = c["company"] + "|" + c["companyUrl"] + "|" + c["productUrl"]
                if key in seen:
                    return None
                seen.add(key)

                ts_marker = " [TS]" if c["trustSeal"] == "Yes" else ""
                print(f"[{idx+1:>3}/{len(card_els)}] {c['company'][:48]:<48}{ts_marker}   {c['location']}")

                phone, gst_val, maps, about, prod_specs, brochure, reach_us_addr = \
                    await deep_fetch_card(ctx, c)

                ts_issue, ts_expiry, ts_cert_addr = trustseal_data.get(c["dispId"], ("N/A", "N/A", "N/A"))

                # Priority address: TrustSeal cert > Registered Address > Reach Us > listing snippet
                if ts_cert_addr not in ("N/A", ""):
                    final_address = ts_cert_addr
                elif about.get("Registered Address") not in ("N/A", ""):
                    final_address = about["Registered Address"]
                elif reach_us_addr not in ("N/A", ""):
                    final_address = reach_us_addr
                else:
                    final_address = c["location"]

                return {
                    "Company Name":            c["company"],
                    "Subcategory":             product_slug.replace('-', ' ').title(),
                    "Final Address":           final_address,
                    "City":                    city,
                    "Phone":                   phone,
                    "GST Number":              about["GST Number"] if about["GST Number"] != "N/A" else gst_val,
                    # "GST Reg Date":            about["GST Reg Date"],
                    # "Maps Link":               maps,
                    # "CEO":                     about["CEO"],
                    # "Annual Turnover":         about["Annual Turnover"],
                    # "Employees":               about["Employees"],
                    # "Listing Snippet Address": c["location"],
                    # "Legal Status":            about["Legal Status"],
                    "Nature of Business":      about["Nature of Business"],
                    "Rating":                  c["rating"],
                    "Response Rate":           c["responseRate"],
                    "Price":                   c["price"],
                    # "Brands We Deal In":       about["Brands We Deal In"],
                    "TrustSEAL":               c["trustSeal"],
                    "Product":                 c["product"],
                    # "Product Specs":           prod_specs,
                    "Product Brochure":        brochure,
                    # "Member Since":            c["memberSince"],
                    # "TrustSEAL Issue Date":    ts_issue,
                    # "TrustSEAL Expiry Date":   ts_expiry,
                    "GST Badge":               c["gstBadge"],
                    "Supplier Badge":           c["badge"],
                    "Company URL":             c["companyUrl"],
                    "Product URL":             c["productUrl"],
                    # "Scraped At":              datetime.now().strftime("%Y-%m-%d %H:%M"),
                }
            except Exception as e:
                print(f"   [!] Error on card {idx+1}: {e}")
                return None

    tasks   = [process_card(i, el) for i, el in enumerate(card_els)]
    results = await asyncio.gather(*tasks)
    leads   = [r for r in results if r is not None]

    if leads:
        write_header = not os.path.exists(output_csv)
        with open(output_csv, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(leads[0].keys()), quoting=csv.QUOTE_ALL)
            if write_header:
                w.writeheader()
            w.writerows(leads)
        print(f"  ✅ {len(leads)} leads written for {city} → {output_csv}")
    else:
        print(f"  [!] No leads collected for {city}.")

    return len(leads)


async def run_step1(mobile, product_slug="concrete-blocks", cities=None):
    async_playwright = _import_playwright()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not cities:
        cities = CITIES
    elif isinstance(cities, str):
        cities = [cities]
    cities = [c.strip() for c in cities if isinstance(c, str) and c.strip()]
    if not cities:
        cities = CITIES

    print(f"""
╔══════════════════════════════════════════════════╗
║   STEP 1 — IndiaMart Scraper                     ║
╠══════════════════════════════════════════════════╣
  Mobile     : {mobile}
  Product    : {product_slug}
  Cities     : {', '.join(cities)}
  Output     : {STEP1_OUTPUT}
╚══════════════════════════════════════════════════╝
""")

    # Clear existing step-1 output so we start fresh
    if os.path.exists(STEP1_OUTPUT):
        os.remove(STEP1_OUTPUT)

    async with async_playwright() as pw:
        browser  = await pw.chromium.launch(headless=True)
        ctx_args = {
            "user_agent": USER_AGENT,
            "viewport":   {"width": 1366, "height": 768},
            "locale":     "en-IN",
        }
        if os.path.exists(SESSION_FILE):
            print("[*] Reusing saved session")
            ctx_args["storage_state"] = SESSION_FILE

        ctx  = await browser.new_context(**ctx_args)
        page = await ctx.new_page()

        first_url = f"https://dir.indiamart.com/{cities[0].lower()}/{product_slug}.html"
        print(f"[*] Opening: {first_url}")
        await page.goto(first_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(2000)

        if not await is_logged_in(page):
            await do_login(page, mobile)
            await ctx.storage_state(path=SESSION_FILE)
            await page.wait_for_timeout(1500)
        else:
            print("[*] Session active — already logged in.")

        city_summary = []
        for i, city in enumerate(cities):
            print(f"\n[►] City {i+1}/{len(cities)}: {city}")
            try:
                count = await scrape_city(ctx, page, city, mobile, STEP1_OUTPUT, product_slug)
                city_summary.append((city, count, "✓"))
                await ctx.storage_state(path=SESSION_FILE)
            except Exception as e:
                print(f"[!!] Fatal error for {city}: {e}")
                city_summary.append((city, 0, f"✗ {e}"))

        print("\n╔══ STEP 1 COMPLETE ══╗")
        total = 0
        for city, cnt, status in city_summary:
            print(f"  {status}  {city:<18} {cnt:>4} leads")
            total += cnt
        print(f"  TOTAL  {total} leads → {STEP1_OUTPUT}")

        await ctx.storage_state(path=SESSION_FILE)
        await browser.close()

    return STEP1_OUTPUT


# ══════════════════════════════════════════════════════════════════════════════
#
#  STEP 2 — Google Places Enrichment
#
# ══════════════════════════════════════════════════════════════════════════════

def extract_pincode(text):
    if not text:
        return None
    matches = re.findall(r'\b[1-9]\d{5}\b', str(text))
    return matches[-1] if matches else None


def get_state_from_gst(gst_number):
    if not gst_number or pd.isna(gst_number):
        return None
    code = str(gst_number)[:2]
    return GST_STATE_MAP.get(code)


def normalize_name(name):
    if not name:
        return ""
    return re.sub(r'[^0-9a-zA-Z]+', ' ', str(name).lower()).strip()


def filter_company_tokens(name):
    text = normalize_name(name)
    stopwords = {
        "ltd", "limited", "pvt", "private", "co", "company", "llp",
        "india", "pvtltd", "gmbh", "inc", "incorporated", "enterprises",
        "enterprise", "builders", "traders", "suppliers", "associate",
        "associates", "industries", "industry", "solutions", "services",
        "group", "holdings", "exports", "marketing", "office",
        "works", "unit", "factory", "plant", "establishment",
        "point", "of", "interest"
    }
    return [token for token in text.split() if len(token) > 2 and token not in stopwords]


def is_company_match(company_name, place_name):
    company_norm = normalize_name(company_name)
    place_norm   = normalize_name(place_name)
    if not company_norm or not place_norm:
        return False
    if company_norm in place_norm:
        return True
    company_tokens = filter_company_tokens(company_name)
    place_tokens   = filter_company_tokens(place_name)
    if not company_tokens or not place_tokens:
        return False
    if " ".join(company_tokens) in " ".join(place_tokens) or \
       " ".join(place_tokens) in " ".join(company_tokens):
        return True
    common = set(company_tokens) & set(place_tokens)
    min_t  = min(len(company_tokens), len(place_tokens))
    if min_t > 0 and len(common) >= 0.8 * min_t:
        return True
    return False


def get_place_display_name(place):
    dn = place.get("displayName", {})
    if isinstance(dn, dict):
        return dn.get("text", "") or dn.get("value", "")
    return place.get("name", "")


def fetch_google_data(company_name, city, pincode, state=None, search_terms="building materials"):
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": PLACES_API_KEY,
        "X-Goog-FieldMask": (
            "places.displayName,places.name,places.formattedAddress,"
            "places.nationalPhoneNumber,places.rating,places.userRatingCount,"
            "places.types,places.businessStatus"
        )
    }
    query = f"{company_name} {pincode} {city} {state or ''} {search_terms}".strip()
    body  = {"textQuery": query, "maxResultCount": 1}
    try:
        resp = requests.post(PLACES_SEARCH_URL, json=body, headers=headers, timeout=15)
        data = resp.json()
        if "places" in data and len(data["places"]) > 0:
            p          = data["places"][0]
            place_name = get_place_display_name(p)
            if place_name and not is_company_match(company_name, place_name):
                print(f"    ⚠️ Name mismatch: '{company_name}' vs '{place_name}'. Skipping.")
                return None
            if not place_name:
                place_name = p.get("name", "N/A")
            return {
                "Google Name":           place_name,
                "Google Full Address":    p.get("formattedAddress", "N/A"),
                "Google Contact Number":  p.get("nationalPhoneNumber", "N/A"),
                "Google Rating":          p.get("rating", "N/A"),
                "Google Reviews":         p.get("userRatingCount", "N/A"),
                "Google Business Type":   ", ".join(p.get("types", [])).replace("_", " "),
                "Google Business Status": p.get("businessStatus", "N/A"),
            }
    except Exception as e:
        print(f"    ⚠️ Google API error: {e}")
    return None


def fetch_google_data_batch(tasks, search_terms="building materials"):
    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_idx = {
            executor.submit(fetch_google_data, company, city, pincode, state, search_terms): idx
            for idx, company, city, pincode, state in tasks
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                print(f"    ⚠️ Batch error idx {idx}: {e}")
                results[idx] = None
    return results


def get_selenium_driver():
    webdriver, Service, Options, By, WebDriverWait, EC, ChromeDriverManager = _import_selenium()
    opts = Options()
    opts.add_argument("--window-size=1366,768")
    opts.add_argument("--headless=new") # Add this!
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(f"user-agent={USER_AGENT}")
    svc    = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=svc, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def make_driver_or_restart(driver):
    try:
        _ = driver.current_url
        return driver
    except Exception:
        print("    ⚡ Driver crashed — restarting...")
        try:
            driver.quit()
        except Exception:
            pass
        time.sleep(2)
        return get_selenium_driver()


def get_location_from_cleartax(driver, gst_number):
    """Returns (pincode, state) via ClearTax GST search."""
    if not gst_number or pd.isna(gst_number):
        return None, None
    webdriver, Service, Options, By, WebDriverWait, EC, ChromeDriverManager = _import_selenium()
    state = get_state_from_gst(gst_number)
    url   = "https://cleartax.in/gst-number-search/"
    for attempt in range(2):
        try:
            if "cleartax.in/gst-number-search" not in driver.current_url:
                driver.get(url)
                wait = WebDriverWait(driver, 20)
                wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
                time.sleep(1)
            else:
                wait = WebDriverWait(driver, 20)
            search_input = wait.until(EC.element_to_be_clickable((By.ID, "input")))
            driver.execute_script("arguments[0].value = '';", search_input)
            search_input.click()
            time.sleep(0.2)
            for char in str(gst_number):
                search_input.send_keys(char)
                time.sleep(0.03)
            time.sleep(0.5)
            search_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(@class, 'btn-blue') and contains(text(), 'SEARCH')]")
            ))
            driver.execute_script("arguments[0].click();", search_btn)
            # Wait for pincode link
            pincode_el = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'pincode')]"))
            )
            href    = pincode_el.get_attribute("href") or ""
            m       = re.search(r'/(\d{6})(?:/|$)', href)
            pincode = m.group(1) if m else None
            return pincode, state
        except Exception as e:
            print(f"    ⚠️ ClearTax attempt {attempt+1} failed: {e}")
    return None, state


def run_step2(input_csv, batch_size=10, test_mode=False, filter_city="", search_terms="building materials"):
    print(f"""
╔══════════════════════════════════════════════════╗
║   STEP 2 — Google Places Enrichment              ║
╠══════════════════════════════════════════════════╣
  Input  : {input_csv}
  Output : {STEP2_OUTPUT}
  Search : {search_terms}
╚══════════════════════════════════════════════════╝
""")
    df = pd.read_csv(input_csv)
    df.columns = df.columns.str.strip()

    # Optional city filter
    if filter_city and "City" in df.columns:
        df = df[df["City"].str.lower() == filter_city.lower()]

    print(f"  Records after filter: {len(df)}")
    
    # Log what filters are active
    if filter_city:
        print(f"  ✓ Filters active:")
        print(f"    - City: {filter_city}")
    else:
        print(f"  ✓ No filters — will process ALL {len(df)} rows")
    
    if test_mode:
        df = df.head(10)
        print("  [TEST MODE] using first 10 rows")

    for col in ["Google Name", "Google Full Address", "Google Contact Number",
                "Google Rating", "Google Reviews", "Google Business Type", "Google Business Status"]:
        df[col] = "N/A"

    driver       = get_selenium_driver()
    google_tasks = []

    def flush_batch(search_terms="building materials"):
        if not google_tasks:
            return
        print(f"\n  🚀 Firing {len(google_tasks)} Google API calls in parallel...")
        results = fetch_google_data_batch(google_tasks, search_terms)
        for task_idx, company, city, pincode, state in google_tasks:
            g_data = results.get(task_idx)
            if g_data:
                for k, v in g_data.items():
                    df.at[task_idx, k] = str(v) if v is not None else "N/A"
                print(f"    ✅ [{task_idx+1}] {company} — enriched")
            else:
                print(f"    ⚠️ [{task_idx+1}] {company} — no Google results")
        google_tasks.clear()

    try:
        for idx, row in df.iterrows():
            company = str(row.get("Company Name", "")).strip()
            addr    = str(row.get("Final Address", ""))
            gst     = row.get("GST Number", "")
            city    = str(row.get("City", "")).strip()

            print(f"\n[{idx+1}] Processing: {company}")

            pincode = extract_pincode(addr)
            state   = None
            if pincode:
                print(f"    📍 Pincode: {pincode}")
                state = get_state_from_gst(gst) if gst and not pd.isna(gst) else None
            if not pincode and gst and not pd.isna(gst):
                driver  = make_driver_or_restart(driver)
                pincode, state = get_location_from_cleartax(driver, gst)
            if not pincode:
                print(f"    🏙 Falling back to city: {city}")
                pincode = city

            print(f"    📋 Queued: {company} + {pincode} + {state or 'no state'}")
            google_tasks.append((idx, company, city, pincode, state))

            if len(google_tasks) >= batch_size:
                flush_batch(search_terms)
                os.makedirs(OUTPUT_DIR, exist_ok=True)
                df.to_csv(STEP2_UNFILTERED, index=False)
                print(f"  💾 Progress saved ({idx+1} rows processed)")

        flush_batch(search_terms)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df.to_csv(STEP2_UNFILTERED, index=False)
        print(f"\n📄 Unfiltered output saved: {STEP2_UNFILTERED}")

        # Apply elimination filters
        print("\n🔍 Applying filters...")

        def is_missing(col):
            return col.isna() | col.str.strip().isin(["", "N/A"])

        eliminate_mask = (
            (
                is_missing(df["Google Name"]) |
                is_missing(df["Google Contact Number"])
            ) |
            (df.get("Location", pd.Series(dtype=str)).str.contains("Deals in", case=False, na=False)) |
            (df["Product"].str.contains("Floor|Partition", case=False, na=False)) |
            (df["Google Business Status"].str.upper() == "CLOSED_PERMANENTLY")
        )

        df_filtered = df[~eliminate_mask].copy()
        print(f"  Eliminated : {len(df) - len(df_filtered)} rows")
        print(f"  Remaining  : {len(df_filtered)} rows")

        df_filtered.to_csv(STEP2_OUTPUT, index=False)
        print(f"✅ Enriched output saved: {STEP2_OUTPUT}")

    return STEP2_OUTPUT


# ══════════════════════════════════════════════════════════════════════════════
#
#  STEP 3 — Gemini Business-Type Classification
#
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_places_for_gemini(name, city, full_address):
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": PLACES_API_KEY,
        "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.types",
    }
    query_parts = [p for p in [name, city, full_address] if p]
    body = {"textQuery": ", ".join(query_parts), "maxResultCount": 1}
    try:
        resp = requests.post(PLACES_SEARCH_URL, json=body, headers=headers, timeout=15)
        data = resp.json()
        if "places" in data and len(data["places"]) > 0:
            return data["places"][0]
    except Exception:
        pass
    return None



def get_batch_business_details(batch_list):
    """
    Sends a batch of companies to Gemini. 
    Returns Category (Google Maps Label) and GST Number.
    """
    formatted_list = ""
    for idx, item in enumerate(batch_list):
        formatted_list += f"{idx+1}. Name: {item['name']}, City: {item['city']}, Address: {item['address']}, Types: {item['types']}\n"

    prompt = (
        f"You are a business research expert. Research these {len(batch_list)} Indian companies.\n\n"
        "FOR EACH COMPANY:\n"
        "1. CATEGORY: Based on the business name and types provided, return the single most accurate "
        "Google Maps category label (e.g., 'Cement manufacturer', 'Building materials store'). "
        "Return ONLY the label. If unsure, return 'N/A'.\n"
        "2. GST: Search for the official 15-digit GSTIN for this specific business address. "
        "If not found or not 100% certain, return 'N/A'.\n\n"
        f"Target List:\n{formatted_list}\n"
        "Return a JSON list of objects with: index, category, gst, and reasoning."
    )

    response_schema = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "index": {"type": "integer"},
                "category": {"type": "string"},
                "gst": {"type": "string"},
                "reasoning": {"type": "string"}
            },
            "required": ["index", "category", "gst", "reasoning"]
        }
    }

    try:
        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[{"google_search": {}}],
                response_mime_type="application/json",
                response_schema=response_schema,
                temperature=0.0
            ),
        )
        return json.loads(response.text)
    except Exception as exc:
        print(f"  ⚠️ API Error: {exc}")
        return [{"index": i+1, "category": "N/A", "gst": "N/A", "reasoning": str(exc)} for i in range(len(batch_list))]
     
def run_step3(input_csv, filter_city="", batch_size=10):
    print(f"\n🚀 STEP 3: Researching GST and Categories...")

    df = pd.read_csv(input_csv)
    df.columns = df.columns.str.strip()
    
    # Ensure columns exist
    if "Category" not in df.columns:
        df["Category"] = "N/A"
    
    # Identify rows with *** or missing GST
    mask = (
        df["GST Number"].astype(str).str.contains(r"\*", na=False) | 
        df["GST Number"].isna() | 
        df["GST Number"].astype(str).str.lower().str.strip().isin(["n/a", "nan", "", "none"])
    )
    to_process = df[mask]
    
    print(f"📊 Processing {len(to_process)} records...")

    for i in range(0, len(to_process), batch_size):
        chunk = to_process.iloc[i : i + batch_size]
        indices = chunk.index.tolist()
        
        batch_data = []
        for _, row in chunk.iterrows():
            batch_data.append({
                "name": row.get("Google Name") or row.get("Company Name"),
                "city": row.get("City"),
                "address": row.get("Google Full Address") or row.get("Final Address"),
                "types": row.get("Google Business Type") or "Unknown"
            })

        print(f"  📦 Batch {i//batch_size + 1}: {[b['name'] for b in batch_data]}")
        print(f"  🌐 Calling get_batch_business_details...")
        results = get_batch_business_details(batch_data)
        print(f"  ✅ Got {len(results)} results")
        
        # Update the dataframe with results
        for j, result in enumerate(results):
            idx = indices[j]
            df.at[idx, "Category"] = result.get("category", "N/A")
            if result.get("gst") and result["gst"] != "N/A":
                df.at[idx, "GST Number"] = result["gst"]
    
    # Save the updated dataframe
    df.to_csv(STEP3_OUTPUT, index=False)
    print(f"✅ Step 3 output saved: {STEP3_OUTPUT}")
    
    return STEP3_OUTPUT
    
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def parse_steps(steps_arg):
    """Parse comma-separated step numbers, e.g. '1,2' → {1, 2}"""
    if not steps_arg:
        return {1, 2, 3}
    return {int(s.strip()) for s in steps_arg.split(",")}


def get_user_input():
    """Get interactive user input for pipeline configuration."""
    print("\n" + "="*60)
    print("       UNIFIED LEAD PIPELINE — CONFIGURATION")
    print("="*60 + "\n")
    
    # Get steps to run FIRST
    print("📋 Steps to Run:")
    print("   1 = Scrape IndiaMart")
    print("   2 = Enrich with Google Places")
    print("   3 = Classify with Gemini AI")
    steps_input = input("   Enter steps (e.g., 1,2,3 or 2,3 for skipping scrape): ").strip()
    if not steps_input:
        steps_input = "1,2,3"
    steps = parse_steps(steps_input)
    
    # Initialize defaults
    mobile = "0000000000"  # dummy
    product_slug = "concrete-blocks"
    search_terms = "building materials"
    filter_city = ""
    filter_subcat = ""
    batch_size = 10
    test_mode = False
    
    # Ask for Step 1 inputs if needed
    if 1 in steps:
        print("\n" + "─"*50)
        print("📋 STEP 1 — IndiaMart Scraper")
        print("─"*50)
        
        # Check if session exists
        if os.path.exists(SESSION_FILE):
            print("   ✅ Session file found — reusing existing login session")
            print("   📱 Skipping mobile number input")
        else:
            # Get mobile number
            while True:
                mobile = input("📱 Enter IndiaMart Mobile Number (10 digits): ").strip()
                if mobile.isdigit() and len(mobile) == 10:
                    break
                print("   ❌ Please enter a valid 10-digit mobile number.\n")
        
        # Get product slug
        print("\n📦 Product Slug:")
        print("   Examples: concrete-blocks, hollow-blocks, tiles, bricks, fly-ash-bricks")
        product_slug = input("   Enter product slug (default: concrete-blocks): ").strip()
        if not product_slug:
            product_slug = "concrete-blocks"
    
    # Ask for Step 2 inputs if needed
    if 2 in steps:
        print("\n" + "─"*50)
        print("📋 STEP 2 — Google Places Enrichment")
        print("─"*50)
        
        # Get search terms
        print("🔍 Search Terms for Google Places:")
        print("   (These will be added to company name in search)")
        print("   Examples: 'building materials', 'bricks blocks', 'tiles materials'")
        search_terms = input("   Enter search terms (default: building materials): ").strip()
        if not search_terms:
            search_terms = "building materials"
        
        # Get batch size
        print("\n⚙️  Google API Batch Size:")
        while True:
            batch_str = input("   Enter batch size (default: 10): ").strip()
            if not batch_str:
                batch_size = 10
                break
            try:
                batch_size = int(batch_str)
                if batch_size > 0:
                    break
                print("   ❌ Batch size must be a positive number.\n")
            except ValueError:
                print("   ❌ Please enter a valid number.\n")
        
        # Get test mode
        print("\n🧪 Test Mode:")
        test_input = input("   Run in test mode (first 10 rows only)? (y/n, default: n): ").strip().lower()
        test_mode = test_input in ("y", "yes")
    
    # Ask for filters if Step 2 or 3 is selected
    if 2 in steps or 3 in steps:
        print("\n" + "─"*50)
        print("📋 FILTERS (for Steps 2 & 3)")
        print("─"*50)
        
        # Get filter city
        print("🏙️  Filter by City (Optional):")
        print(f"   Available cities: {', '.join(CITIES)}")
        filter_city = input("   Enter city to filter (or leave empty for all cities): ").strip()
    
    return {
        "mobile": mobile,
        "product_slug": product_slug,
        "search_terms": search_terms,
        "filter_city": filter_city,
        "batch_size": batch_size,
        "test_mode": test_mode,
        "steps": steps,
    }


def main():
    # Get user inputs interactively
    config = get_user_input()
    
    mobile = config["mobile"]
    product_slug = config["product_slug"]
    search_terms = config["search_terms"]
    filter_city = config["filter_city"]
    batch_size = config["batch_size"]
    test_mode = config["test_mode"]
    steps = config["steps"]
    
    step1_csv = STEP1_OUTPUT
    step2_csv = STEP2_OUTPUT

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║   UNIFIED LEAD PIPELINE                                      ║
╠══════════════════════════════════════════════════════════════╣
  Steps           : {sorted(steps)}
  Mobile          : {mobile}
  Product Slug    : {product_slug}
  Search Terms    : {search_terms}
  Filter City     : {filter_city or 'None (all cities)'}
  Test Mode       : {test_mode}
  Batch Size      : {batch_size}
  Output Dir      : {OUTPUT_DIR}
╚══════════════════════════════════════════════════════════════╝
""")

    # ── STEP 1 ────────────────────────────────────────────────────────────────
    if 1 in steps:
        step1_csv = asyncio.run(run_step1(mobile, product_slug=product_slug))
    else:
        if not os.path.exists(step1_csv):
            sys.exit(f"❌ Step 1 skipped but no input file found at: {step1_csv}")
        print(f"[SKIP] Step 1 — using existing: {step1_csv}")

    # ── STEP 2 ────────────────────────────────────────────────────────────────
    if 2 in steps:
        step2_csv = run_step2(
            step1_csv,
            batch_size=batch_size,
            test_mode=test_mode,
            filter_city=filter_city,
            search_terms=search_terms
        )
    else:
        if not os.path.exists(step2_csv):
            sys.exit(f"❌ Step 2 skipped but no input file found at: {step2_csv}")
        print(f"[SKIP] Step 2 — using existing: {step2_csv}")

    # ── STEP 3 ────────────────────────────────────────────────────────────────
    if 3 in steps:
        final = run_step3(step2_csv, filter_city=filter_city)
        print(f"\n🎉 Pipeline complete! Final output: {final}")
    else:
        print(f"\n🎉 Pipeline complete! Output: {step2_csv}")


if __name__ == "__main__":
    main()