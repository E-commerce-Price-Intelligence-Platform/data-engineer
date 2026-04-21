from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

CHROMEDRIVER_PATH = r"C:\Users\hp\.wdm\drivers\chromedriver\win64\147.0.7727.57\chromedriver-win32\chromedriver.exe"

options = Options()
options.add_argument("--start-maximized")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)

try:
    driver.get("https://www.electroplanet.ma/p3102627-smartphone-galaxy-a07-4-64gb-black-samsung.html")
    time.sleep(4)

    # Tester tous les sélecteurs de nom
    tests = [
        "h1.page-title span.base",
        "h1.page-title",
        "span.base",
        "div.product-info-main h1",
        "h1",
    ]
    print("=== NOMS ===")
    for sel in tests:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        print(f"{sel:40} → {[e.text.strip() for e in els[:2]]}")

    # Tester tous les sélecteurs de prix
    print("\n=== PRIX ===")
    price_tests = [
        "span.price",
        "div.price-box span.price",
        "span.special-price span.price",
        "div[data-price-type='finalPrice'] span.price",
        "span[data-price-type='finalPrice']",
        "div.price-final_price span.price",
    ]
    for sel in price_tests:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        print(f"{sel:50} → {[e.text.strip() for e in els[:2]]}")

    # iPhone page
    print("\n=== TEST PAGE IPHONE ===")
    driver.get("https://www.electroplanet.ma/catalogsearch/result/?q=apple+iphone")
    time.sleep(4)
    els = driver.find_elements(By.CSS_SELECTOR, "a.product-item-link")
    print(f"Résultats 'apple iphone' : {len(els)}")
    for e in els[:5]:
        print(f"  - {e.text.strip()}")

finally:
    time.sleep(2)
    driver.quit()