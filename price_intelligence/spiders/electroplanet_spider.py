"""
Spider Electroplanet — Selenium
Run: python spiders/electroplanet_spider.py
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
SEARCH_QUERIES = ["samsung galaxy", "apple iphone"]
WAIT_PAGE      = 15  # seconds to wait for product links


def clean_price(price_str):
    if not price_str:
        return None
    cleaned = (
        price_str
        .replace("DH", "").replace("dh", "")
        .replace("MAD", "").replace("mad", "")
        .replace("\xa0", "").replace(" ", "")
        .replace(" ", "").replace(" ", "")
        .replace(",", ".").strip()
    )
    parts = cleaned.split(".")
    if len(parts) == 3:
        cleaned = parts[0] + parts[1] + "." + parts[2]
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_magento_rating(driver):
    """
    DOM: <div class="rating-result" title="100%">
    title is a percentage — convert to 5-star scale.
    """
    els = driver.find_elements(By.CSS_SELECTOR, "div.rating-result")
    if els:
        title = els[0].get_attribute("title") or ""
        m = re.search(r"([\d.]+)\s*%", title)
        if m:
            return round(float(m.group(1)) / 100 * 5, 1)
    return None


def parse_reviews_count(driver):
    """DOM: <span itemprop="reviewCount">1</span>"""
    els = driver.find_elements(By.CSS_SELECTOR, "span[itemprop='reviewCount']")
    if els:
        digits = re.sub(r"[^\d]", "", els[0].text)
        if digits:
            return int(digits)
    return None


NON_PHONE_KEYWORDS = [
    "ecouteurs", "buds", "montre", "watch", "fit", "smarttag", "tag2",
    "cover", "coque", "câble", "chargeur", "casque", "enceinte",
    "flipsuit", "case", "bracelet", "tablette", "ipad",
]

MIN_PHONE_PRICE_DH = 800  # anything below this is an accessory/cashback artefact


def is_smartphone(driver, name, price):
    """
    Three-layer filter:
    1. Category link must point to smartphone/iphone URL
    2. Name must not contain non-phone keywords
    3. Price must be above minimum phone threshold
    """
    # Category check
    els = driver.find_elements(By.CSS_SELECTOR, "h1.page-title a.category-name")
    if els:
        href = els[0].get_attribute("href") or ""
        text = els[0].text.lower()
        if "smartphone" not in href and "iphone" not in href \
                and "smartphone" not in text and "iphone" not in text:
            return False
    # Keyword exclusion on name
    n = name.lower()
    if any(kw in n for kw in NON_PHONE_KEYWORDS):
        return False
    # Price sanity
    if price is not None and price < MIN_PHONE_PRICE_DH:
        return False
    return True


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


def make_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-notifications")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

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
    return driver


def dismiss_popups(driver):
    for selector in [
        "button.action-close", "button.accept-cookies",
        "#cookie-accept", ".modal-close", "button[data-role='closeBtn']",
    ]:
        try:
            driver.find_element(By.CSS_SELECTOR, selector).click()
            time.sleep(0.4)
        except Exception:
            pass


def scrape_product(driver, url, fallback_name):
    try:
        driver.get(url)

        # Wait for product title
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "h1.page-title, h1.product-name")
                )
            )
        except Exception:
            pass

        dismiss_popups(driver)

        # Name — span.ref holds the full product title
        name = fallback_name
        for sel in ["h1.page-title span.ref", "h1.page-title span.base", "h1.page-title"]:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els and els[0].text.strip():
                name = els[0].text.strip()
                break

        if not name:
            return None

        # Price — use data-price-amount attribute (exact numeric, no parsing errors)
        price, old_price = None, None
        try:
            final_el = driver.find_elements(
                By.CSS_SELECTOR, "span[data-price-type='finalPrice']"
            )
            if final_el:
                amt = final_el[0].get_attribute("data-price-amount")
                price = float(amt) if amt else None

            old_el = driver.find_elements(
                By.CSS_SELECTOR, "span[data-price-type='oldPrice']"
            )
            if old_el:
                amt = old_el[0].get_attribute("data-price-amount")
                old_price = float(amt) if amt else None
        except Exception:
            pass

        # Filter: category + name keywords + price sanity
        if not is_smartphone(driver, name, price):
            return None

        discount = None
        if price and old_price and old_price > price:
            discount = f"{round((1 - price / old_price) * 100)}%"

        # Rating and reviews
        rating  = parse_magento_rating(driver)
        reviews = parse_reviews_count(driver)

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
        print(f"    ✓ {name[:60]} — {price} DH | ★{rating} ({reviews} avis)")
        return item

    except Exception as e:
        print(f"    ✗ Erreur {url}: {e}")
        return None


def scrape_query(driver, query):
    items = []
    page  = 1
    url   = f"https://www.electroplanet.ma/catalogsearch/result/?q={query.replace(' ', '+')}"

    while url:
        print(f"\n[Electroplanet] '{query}' — page {page}")
        driver.get(url)

        dismiss_popups(driver)

        try:
            WebDriverWait(driver, WAIT_PAGE).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.product-item-link"))
            )
        except Exception:
            print(f"  ✗ Timeout — aucun produit sur page {page}")
            print(f"  ℹ titre: {driver.title[:80]}")
            break

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

        # Navigate back to listing for pagination
        driver.get(url)
        time.sleep(2)

        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, "a.action.next")
            url      = next_btn.get_attribute("href")
            page    += 1
        except Exception:
            print(f"  → Dernière page pour '{query}'")
            url = None

    return items


def save_results(items, spider_name="electroplanet"):
    if not items:
        print("⚠️ Aucun produit — aucun fichier créé")
        return None, None

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    csv_path = f"{OUTPUT_DIR}/{spider_name}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=items[0].keys())
        writer.writeheader()
        writer.writerows(items)
    print(f"\n✅ CSV : {csv_path}")

    json_path = f"{OUTPUT_DIR}/{spider_name}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON : {json_path}")

    return csv_path, json_path


if __name__ == "__main__":
    print("=== Electroplanet Spider ===")
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

    print(f"\n=== Total : {len(all_items)} produits ===")
    save_results(all_items)

    samsung = [i for i in all_items if i["brand"] == "samsung"]
    apple   = [i for i in all_items if i["brand"] == "apple"]
    print(f"Samsung : {len(samsung)}")
    print(f"Apple   : {len(apple)}")
