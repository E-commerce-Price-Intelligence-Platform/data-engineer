"""
Spider Amazon — Selenium with proper page load waits
Run: python spiders/amazon_spider.py
"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import csv, json, os, re, time

OUTPUT_DIR     = os.environ.get("OUTPUT_DIR", "output")
SEARCH_QUERIES = [
    "samsung galaxy s24 smartphone",
    "samsung galaxy a55 smartphone",
    "apple iphone 16 smartphone",
    "apple iphone 15 smartphone",
]
BASE_URL  = "https://www.amazon.fr/s?k="
MAX_PAGES = 3
WAIT_PAGE = 10  # seconds to wait for results

MOTS_EXCLUS = [
    "coque", "étui", "housse", "verre trempé", "film protecteur",
    "chargeur", "câble", "batterie", "perche selfie",
    "écouteurs", "earphone", "headphone",
    "montre connectée", "galaxy watch", "galaxy buds", "galaxy fit",
    "bracelet", "brassard sport", "adaptateur",
    "stylet", "station de charge", "dock",
    "téléphone fixe", "dect", "logicom", "panasonic",
    "motorola", "xiaomi", "poco", "oppo", "honor", "pixel", "tcl",
    "drybag", "stabilisateur", "cardan", "carte mémoire",
    "powerbank", "visionneuse", "filtre nd",
    "topeak", "ram mount", "neewer", "piaggio",
    "enceinte", "bonnet bluetooth",
]


def make_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-notifications")
    options.add_argument("--lang=fr-FR")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    # Do NOT use page_load_strategy=none — we need the DOM to actually load

    selenium_url = os.environ.get("SELENIUM_REMOTE_URL")
    if selenium_url:
        driver = webdriver.Remote(command_executor=selenium_url, options=options)
    else:
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options,
        )
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def clean_price(price_str):
    if not price_str:
        return None
    cleaned = (
        price_str
        .replace("€", "").replace("EUR", "")
        .replace("\xa0", "").replace(" ", "")
        .replace(" ", "").replace(" ", "")
        .replace(",", ".").strip()
    )
    parts = cleaned.split(".")
    if len(parts) == 3:
        cleaned = parts[0] + parts[1] + "." + parts[2]
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_rating(text):
    """Extract float from '4.5 sur 5 étoiles' or '4.5 out of 5 stars'."""
    if not text:
        return None
    m = re.match(r"([\d,\.]+)", text.strip())
    if m:
        try:
            return float(m.group(1).replace(",", "."))
        except ValueError:
            pass
    return None


def parse_reviews(text):
    """Extract int from '1 234 évaluations' or '1,234 ratings'."""
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def extract_model(name):
    n = name.lower()
    models = [
        "s26 ultra", "s26+", "s26", "s25 ultra", "s25+", "s25",
        "s24 ultra", "s24+", "s24", "s23 ultra", "s23+", "s23",
        "a57", "a56", "a55", "a36", "a35", "a26", "a25", "a17",
        "a16", "a15", "a07", "a06", "a05",
        "z fold 7", "z fold 6", "z fold 5",
        "z flip 7", "z flip 6", "z flip 5",
        "iphone 17 pro max", "iphone 17 pro", "iphone 17 plus", "iphone 17",
        "iphone 16 pro max", "iphone 16 pro", "iphone 16 plus", "iphone 16",
        "iphone 15 pro max", "iphone 15 pro", "iphone 15 plus", "iphone 15",
        "iphone 14 pro max", "iphone 14 pro", "iphone 14",
        "iphone 13 pro max", "iphone 13 pro", "iphone 13",
    ]
    for m in models:
        if m in n:
            return m.title()
    return "Unknown"


def get_brand(name):
    n = name.lower()
    if "samsung" in n:
        return "samsung"
    elif "iphone" in n or "apple" in n:
        return "apple"
    return "other"


def is_smartphone(name):
    n = name.lower()
    if any(mot in n for mot in MOTS_EXCLUS):
        return False
    if "samsung" not in n and "iphone" not in n and "apple" not in n:
        return False
    return True


def scrape_listing_page(driver, url):
    driver.get(url)

    # Accept cookies if present
    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input#sp-cc-accept"))
        ).click()
        time.sleep(1)
    except Exception:
        pass

    # Wait for product results to load
    try:
        WebDriverWait(driver, WAIT_PAGE).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div[data-component-type='s-search-result']")
            )
        )
    except Exception:
        print(f"  ✗ Timeout waiting for results on {url}")
        return []

    # Scroll to trigger lazy-loaded ratings
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
    time.sleep(2)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)

    products = driver.find_elements(
        By.CSS_SELECTOR, "div[data-component-type='s-search-result']"
    )
    print(f"  → {len(products)} résultats bruts")

    items_data = []
    for product in products:
        try:
            name_el = product.find_elements(By.CSS_SELECTOR, "h2 span")
            name    = name_el[0].text.strip() if name_el else ""
            if not name or not is_smartphone(name):
                continue

            link_el = product.find_elements(By.CSS_SELECTOR, "h2 a")
            href    = link_el[0].get_attribute("href") if link_el else ""

            # Price — try whole+fraction first, fall back to .a-offscreen
            price = None
            price_whole = product.find_elements(By.CSS_SELECTOR, "span.a-price-whole")
            price_frac  = product.find_elements(By.CSS_SELECTOR, "span.a-price-fraction")
            if price_whole:
                whole = re.sub(r"[^\d]", "", price_whole[0].text)
                frac  = price_frac[0].text.strip() if price_frac else "00"
                frac  = re.sub(r"[^\d]", "", frac) or "00"
                try:
                    price = float(f"{whole}.{frac}")
                except Exception:
                    price = None
            if price is None:
                offscreen = product.find_elements(
                    By.CSS_SELECTOR, "span.a-price span.a-offscreen"
                )
                if offscreen:
                    price = clean_price(offscreen[0].get_attribute("innerHTML"))

            # Old price
            old_el    = product.find_elements(
                By.CSS_SELECTOR, "span.a-price.a-text-price span.a-offscreen"
            )
            old_price = clean_price(old_el[0].text) if old_el else None

            # Discount
            disc_el  = product.find_elements(By.CSS_SELECTOR, "span.a-badge-text")
            discount = disc_el[0].text.strip() if disc_el else None

            # Rating selectors — try most specific first
            rating = None
            rating_selectors = [
                # Numeric text inside popover trigger (product & listing page)
                "span.a-size-small.a-color-base",
                # Alt text inside mini star icon
                "i.a-icon-star-mini span.a-icon-alt",
                # Regular star icon alt text (listing page)
                "i[class*='a-icon-star'] span.a-icon-alt",
                "span.a-icon-alt",
            ]
            for sel in rating_selectors:
                for el in product.find_elements(By.CSS_SELECTOR, sel):
                    text = el.text.strip()
                    if not text:
                        continue
                    rating = parse_rating(text)
                    if rating:
                        break
                if rating:
                    break

            # Reviews — visible count "(1 234)" next to stars
            reviews = None
            for el in product.find_elements(
                By.CSS_SELECTOR,
                "span.a-size-base.s-underline-text, "
                "span[aria-label*='évaluation'], "
                "span[aria-label*='rating'], "
                "a[href*='customerReviews'] span",
            ):
                text = el.text.strip() or el.get_attribute("aria-label") or ""
                reviews = parse_reviews(text)
                if reviews:
                    break

            items_data.append({
                "name":        name,
                "brand":       get_brand(name),
                "model":       extract_model(name),
                "price":       price,
                "old_price":   old_price,
                "currency":    "EUR",
                "discount":    discount,
                "rating":      rating,
                "reviews":     reviews,
                "url":         href,
                "source_site": "amazon_fr",
                "scraped_at":  datetime.utcnow().isoformat(),
            })

        except Exception as e:
            print(f"    ✗ Erreur : {e}")
            continue

    return items_data


def get_next_page_url(driver):
    try:
        next_btn = driver.find_element(By.CSS_SELECTOR, "a.s-pagination-next")
        return next_btn.get_attribute("href")
    except Exception:
        return None


def scrape_query(driver, query):
    all_items = []
    seen_urls = set()
    url       = BASE_URL + query.replace(" ", "+")
    page      = 1

    while url and page <= MAX_PAGES:
        print(f"\n[Amazon] '{query}' — page {page}")
        items = scrape_listing_page(driver, url)

        new_items = []
        for it in items:
            key = it["url"] or it["name"]
            if key not in seen_urls:
                seen_urls.add(key)
                new_items.append(it)

        all_items.extend(new_items)
        print(f"  → {len(new_items)} uniques (sur {len(items)} filtrés)")
        for it in new_items:
            print(f"    ✓ {it['name'][:65]} — {it['price']} EUR | ★{it['rating']} ({it['reviews']} avis)")

        url  = get_next_page_url(driver)
        page += 1
        time.sleep(2)

    return all_items


def save_results(items):
    if not items:
        print("⚠️ Aucun produit — aucun fichier créé")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    csv_path = f"{OUTPUT_DIR}/amazon.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=items[0].keys())
        writer.writeheader()
        writer.writerows(items)
    print(f"\n✅ CSV : {csv_path}")

    json_path = f"{OUTPUT_DIR}/amazon.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON : {json_path}")


if __name__ == "__main__":
    print("=== Amazon Spider ===")
    driver    = make_driver()
    all_items = []

    try:
        for query in SEARCH_QUERIES:
            items = scrape_query(driver, query)
            all_items.extend(items)
            print(f"\n  → '{query}' : {len(items)} produits")
    finally:
        driver.quit()
        print("\nChrome fermé.")

    seen  = set()
    dedup = []
    for it in all_items:
        key = it["url"] or it["name"]
        if key not in seen:
            seen.add(key)
            dedup.append(it)

    print(f"\n=== Total après déduplication : {len(dedup)} produits ===")
    save_results(dedup)

    samsung = [i for i in dedup if i["brand"] == "samsung"]
    apple   = [i for i in dedup if i["brand"] == "apple"]
    print(f"Samsung : {len(samsung)}")
    print(f"Apple   : {len(apple)}")
