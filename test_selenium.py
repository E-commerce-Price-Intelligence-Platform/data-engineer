from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time

options = Options()
options.add_argument("--start-maximized")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

try:
    # Test sur une page produit Samsung directe
    driver.get("https://www.electroplanet.ma/p3102627-smartphone-galaxy-a07-4-64gb-black-samsung.html")
    time.sleep(5)

    print(f"Titre : {driver.title}")

    # Nom du produit
    name = driver.find_elements(By.CSS_SELECTOR, "h1.page-title span")
    print(f"\nNom (h1.page-title span)     : {[n.text for n in name]}")

    # Prix
    price = driver.find_elements(By.CSS_SELECTOR, "span.price")
    print(f"Prix (span.price)            : {[p.text for p in price[:3]]}")

    # Prix spécial / barré
    special = driver.find_elements(By.CSS_SELECTOR, "span.special-price span.price")
    old     = driver.find_elements(By.CSS_SELECTOR, "span.old-price span.price")
    print(f"Prix spécial                 : {[s.text for s in special]}")
    print(f"Ancien prix                  : {[o.text for o in old]}")

    # Rating
    rating = driver.find_elements(By.CSS_SELECTOR, "div.rating-summary span.rating-result")
    print(f"Rating                       : {[r.get_attribute('title') for r in rating]}")

    # Avis
    reviews = driver.find_elements(By.CSS_SELECTOR, "a.action.view span")
    print(f"Avis                         : {[r.text for r in reviews]}")

    # Marque
    brand = driver.find_elements(By.CSS_SELECTOR, "div.product-brand img")
    brand2 = driver.find_elements(By.CSS_SELECTOR, "a[href*='brand']")
    print(f"Marque (img)                 : {[b.get_attribute('alt') for b in brand]}")

    # Maintenant tester la page listing avec recherche
    print("\n--- Test page recherche Samsung ---")
    driver.get("https://www.electroplanet.ma/catalogsearch/result/?q=samsung+smartphone")
    time.sleep(5)
    print(f"Titre : {driver.title}")
    products = driver.find_elements(By.CSS_SELECTOR, "a.product-item-link")
    print(f"Produits trouvés : {len(products)}")
    for p in products[:5]:
        print(f"  - {p.text.strip()}")

    # Tester aussi avec filtre URL
    print("\n--- Test URL avec filtre marque ---")
    driver.get("https://www.electroplanet.ma/telephonie/smartphones?brand=samsung")
    time.sleep(5)
    products2 = driver.find_elements(By.CSS_SELECTOR, "a.product-item-link")
    print(f"Produits avec filtre brand : {len(products2)}")
    for p in products2[:5]:
        print(f"  - {p.text.strip()}")

finally:
    time.sleep(2)
    driver.quit()
    print("\nChrome fermé.")