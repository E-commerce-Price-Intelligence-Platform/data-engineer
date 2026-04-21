import scrapy
from datetime import datetime
from price_intelligence.items import SmartphoneItem


class JumiaSpider(scrapy.Spider):
    name            = "jumia_ma"
    allowed_domains = ["jumia.ma"]

    start_urls = [
        "https://www.jumia.ma/smartphones/samsung/",
        "https://www.jumia.ma/smartphones/apple/",
    ]

    async def start(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_listing,
                headers={"Referer": "https://www.jumia.ma/"},
                meta={"page": 1},
            )

    def parse_listing(self, response):
        page = response.meta.get("page", 1)
        self.logger.info(f"[Jumia] Page {page} | {response.url} | status {response.status}")

        products = response.css("article.prd")
        self.logger.info(f"  → {len(products)} produits trouvés")

        for product in products:
            item = SmartphoneItem()

            # Nom
            item["name"] = (
                product.css("h3.name::text").get() or
                product.css("h3::text").get() or ""
            ).strip()

            if not item["name"]:
                continue

            # Marque
            n = item["name"].lower()
            if "samsung" in n:
                item["brand"] = "samsung"
            elif "iphone" in n or "apple" in n:
                item["brand"] = "apple"
            else:
                item["brand"] = "other"

            # Modèle
            item["model"] = self.extract_model(item["name"])

            # Prix
            item["price"]     = self.clean_price(product.css("div.prc::text").get() or "")
            item["currency"]  = "Dhs"
            item["old_price"] = self.clean_price(product.css("div.old::text").get() or "")

            # Réduction
            item["discount"] = (product.css("div.bdg._dsct::text").get() or "").strip() or None

            # Rating — ex: "4.3 out of 5" → on garde juste "4.3"
            rating_raw = (product.css("div.stars._s::text").get() or "").strip()
            if rating_raw:
                item["rating"] = rating_raw.split(" ")[0]  # garde "4.3"
            else:
                item["rating"] = None

            # Nombre d'avis
            reviews_raw = (product.css("div.rev::text").get() or "").strip()
            item["reviews"] = reviews_raw.strip("()") or None

            # URL complète
            href        = product.css("a.core::attr(href)").get() or ""
            item["url"] = response.urljoin(href)

            # Métadonnées
            item["source_site"] = "jumia_ma"
            item["scraped_at"]  = datetime.utcnow().isoformat()

            yield item

        # ── Pagination ──────────────────────────────────────────
        # Récupère tous les liens de page et trouve le max
        page_links = response.css("a[class*='pg']::attr(href)").getall()
        
        # Extraire les numéros de page depuis les URLs
        page_numbers = []
        for link in page_links:
            try:
                num = int(link.split("page=")[1].split("#")[0])
                page_numbers.append(num)
            except (IndexError, ValueError):
                continue

        max_page = max(page_numbers) if page_numbers else 1
        self.logger.info(f"  → Page actuelle: {page} / {max_page}")

        # Passer à la page suivante si elle existe
        next_page_num = page + 1
        if next_page_num <= max_page:
            # Construire l'URL de la page suivante
            base_url = response.url.split("?")[0]
            next_url = f"{base_url}?page={next_page_num}#catalog-listing"
            yield response.follow(
                next_url,
                callback=self.parse_listing,
                meta={"page": next_page_num},
                headers={"Referer": response.url},
            )

    def extract_model(self, name):
        n = name.lower()
        models = [
            "s24 ultra", "s24+", "s24", "s23 ultra", "s23+", "s23",
            "s22 ultra", "s22", "s21", "s25 ultra", "s25+", "s25",
            "a55", "a35", "a25", "a15", "a05", "a06", "a07",
            "z fold 6", "z fold 5", "z flip 6", "z flip 5",
            "iphone 16 pro max", "iphone 16 pro", "iphone 16 plus", "iphone 16",
            "iphone 15 pro max", "iphone 15 pro", "iphone 15 plus", "iphone 15",
            "iphone 14 pro max", "iphone 14 pro", "iphone 14",
            "iphone 13 pro max", "iphone 13 pro", "iphone 13",
        ]
        for m in models:
            if m in n:
                return m.title()
        return "Unknown"

    def clean_price(self, price_str):
        if not price_str:
            return None
        cleaned = (
            price_str
            .replace("Dhs", "").replace("MAD", "")
            .replace("\xa0", "").replace("\u202f", "")
            .replace(" ", "").replace(",", "")
            .strip()
        )
        try:
            return float(cleaned)
        except ValueError:
            return None