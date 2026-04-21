import csv, json, os

class CsvPipeline:
    def open_spider(self, spider):
        os.makedirs("output", exist_ok=True)
        self.fname  = f"output/{spider.name}.csv"
        self.file   = open(self.fname, "w", newline="", encoding="utf-8")
        self.writer = None

    def process_item(self, item, spider):
        if self.writer is None:
            self.writer = csv.DictWriter(self.file, fieldnames=item.keys())
            self.writer.writeheader()
        self.writer.writerow(dict(item))
        return item

    def close_spider(self, spider):
        if hasattr(self, "file") and self.file:
            self.file.close()

class JsonPipeline:
    def open_spider(self, spider):
        os.makedirs("output", exist_ok=True)
        self.fname      = f"output/{spider.name}.json"
        self.file       = open(self.fname, "w", encoding="utf-8")
        self.first_item = True
        self.file.write("[\n")

    def process_item(self, item, spider):
        if not self.first_item:
            self.file.write(",\n")
        json.dump(dict(item), self.file, ensure_ascii=False, indent=2)
        self.first_item = False
        return item

    def close_spider(self, spider):
        if hasattr(self, "file") and self.file:
            self.file.write("\n]")
            self.file.close()
