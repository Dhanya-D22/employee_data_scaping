import json
from process.process import EmployeeScraper

def main():
    with open("ingestion/run_scraper.json") as f:
        configs = json.load(f)

    config = next((cfg for cfg in configs if cfg["enabled"]), None)

    if config:
        scraper = EmployeeScraper(
            api_url=config["api_url"],
            retry_attempts=config["retry_attempts"],
            timeout=config["timeout"]
        )

        print(f"Running scraper: {config['scraper_name']}")
        data = scraper.fetch_data()

        if data:
            scraper.process_data(data)
            print("Employee data processed successfully.")

if __name__ == "__main__":
    main()
