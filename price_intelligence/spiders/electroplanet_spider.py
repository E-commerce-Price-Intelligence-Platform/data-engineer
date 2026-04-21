"""
Spider Electroplanet — 100% Selenium, sans Scrapy
Lancement : python spiders/electroplanet_spider.py
"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime
import csv, json, os, time

# ── Config ────────────────────────────────────────────────
CHROMEDRIVER_PATH = r"C:\Users\hp\.wdm\drivers\chromedriver\win64\147.0.7727.57\chromedriver-win32\chromedriver.exe"
OUTPUT_DIR        = "output"

# ✅ CHANGEMENT 1
SEARCH_QUERIES    = ["samsung galaxy", "apple iphone"]

WAIT_PAGE         = 4
WAIT_PRODUCT      = 2

# ── Helpers ───────────────────────────────────────────────
def clean_price(price_str):
    if not price_str:
        return None
    cleaned = (
        price_str
        .replace("DH", "").replace("dh", "")
        .replace("\xa0", "").replace("\u202f", "")
        .replace("\u00a0", "").replace(" ", "")
        .replace(",", ".").strip()
    )
    parts = cleaned.split(".")
    if len(parts) == 3:
        cleaned = parts[0] + parts[1] + "." + parts[2]
    try:
        return float(cleaned)
    except ValueError:
        return None

def extract_model(name):
    n = name.lower()
    models = [
        "s26 ultra", "s26+", "s26", "s25 ultra", "s25+", "s25",
        "s24 ultra", "s24+", "s24", "s23 ultra", "s23+", "s23",
        "a55", "a35", "a25", "a15", "a07", "a06", "a05",
        "z fold 6", "z fold 5", "z flip 6", "z flip 5",
        "iphone 16 pro max", "iphone 16 pro", "iphone 16 plus", "iphone 16",
        "iphone 15 pro max", "iphone 15 pro", "iphone 15 plus", "iphone 15",
        "iphone 14 pro max", "iphone 14 pro", "iphone 14",
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

# ── Driver ────────────────────────────────────────────────
def make_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-notifications")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    return webdriver.Chrome(
        service=Service(CHROMEDRIVER_PATH),
        options=options,
    )

# ── Scraper ───────────────────────────────────────────────
def scrape_product(driver, url, fallback_name):
    try:
        driver.get(url)
        time.sleep(WAIT_PRODUCT)

        # ✅ CHANGEMENT 2 — NOM
        try:
            name = driver.find_element(By.CSS_SELECTOR, "h1.page-title").text.strip()
        except Exception:
            name = fallback_name

        if not name:
            return None

        # ✅ CHANGEMENT 3 — PRIX
        try:
            special = driver.find_elements(By.CSS_SELECTOR, "span.special-price span.price")
            if special:
                price     = clean_price(special[0].text)
                old_els   = driver.find_elements(By.CSS_SELECTOR, "span.old-price span.price")
                old_price = clean_price(old_els[0].text) if old_els else None
            else:
                price_el  = driver.find_element(By.CSS_SELECTOR, "div.price-final_price span.price")
                price     = clean_price(price_el.text)
                old_price = None
        except Exception:
            price, old_price = None, None

        # Discount
        if price and old_price and old_price > 0:
            discount = f"{round((1 - price / old_price) * 100)}%"
        else:
            discount = None

        # Rating
        try:
            r       = driver.find_element(By.CSS_SELECTOR, "div.rating-summary span.rating-result")
            rating  = r.get_attribute("title")
        except Exception:
            rating = None

        # Avis
        try:
            rv      = driver.find_element(By.CSS_SELECTOR, "a.action.view span")
            reviews = rv.text.strip()
        except Exception:
            reviews = None

        item = {
            "name":        name,
            "brand":       get_brand(name),
            "model":       extract_model(name),
            "price":       price,
            "old_price":   old_price,
            "currency":    "DH",
            "discount":    discount,
            "rating":      rating,
            "reviews":     reviews,
            "url":         url,
            "source_site": "electroplanet",
            "scraped_at":  datetime.utcnow().isoformat(),
        }
        print(f"    ✓ {name} — {price} DH")
        return item

    except Exception as e:
        print(f"    ✗ Erreur {url}: {e}")
        return None


def scrape_query(driver, query):
    items   = []
    page    = 1
    url     = f"https://www.electroplanet.ma/catalogsearch/result/?q={query.replace(' ', '+')}"

    while url:
        print(f"\n[Electroplanet] '{query}' — page {page}")
        driver.get(url)
        time.sleep(WAIT_PAGE)

        # Fermer popup
        try:
            driver.find_element(By.CSS_SELECTOR, "button.action-close").click()
            time.sleep(1)
        except Exception:
            pass

        # Produits
        els = driver.find_elements(By.CSS_SELECTOR, "a.product-item-link")
        print(f"  → {len(els)} produits trouvés")
        if not els:
            break

        products_data = [
            {"url": el.get_attribute("href"), "name": el.text.strip()}
            for el in els if el.get_attribute("href")
        ]

        for prod in products_data:
            item = scrape_product(driver, prod["url"], prod["name"])
            if item:
                items.append(item)

        driver.get(url)
        time.sleep(3)

        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, "a.action.next")
            url      = next_btn.get_attribute("href")
            page    += 1
        except Exception:
            print(f"  → Dernière page pour '{query}'")
            url = None

    return items


def save_results(items, spider_name="electroplanet"):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = f"{OUTPUT_DIR}/{spider_name}_{ts}.csv"
    if items:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=items[0].keys())
            writer.writeheader()
            writer.writerows(items)
        print(f"\n✅ CSV sauvegardé : {csv_path}")

    json_path = f"{OUTPUT_DIR}/{spider_name}_{ts}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON sauvegardé : {json_path}")

    return csv_path, json_path


# ── Main ──────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== Electroplanet Spider ===")
    driver = make_driver()
    all_items = []

    try:
        for query in SEARCH_QUERIES:
            items = scrape_query(driver, query)
            all_items.extend(items)
            print(f"\n  → '{query}' : {len(items)} produits scrapés")
    finally:
        driver.quit()
        print("\nChrome fermé.")

    print(f"\n=== Total : {len(all_items)} produits ===")
    save_results(all_items)

    samsung = [i for i in all_items if i["brand"] == "samsung"]
    apple   = [i for i in all_items if i["brand"] == "apple"]
    print(f"Samsung : {len(samsung)}")
    print(f"Apple   : {len(apple)}")