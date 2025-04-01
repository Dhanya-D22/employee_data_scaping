import json
from process.process import EmployeeScraper

def main():
    # Load scraper config
    with open('run_scraper.json', 'r') as f:
        configs = json.load(f)

    config = next((cfg for cfg in configs if cfg['enabled']), None)

    if config:
        scraper = EmployeeScraper(
            api_url=config['api_url'],
            retry_attempts=config['retry_attempts'],
            timeout=config['timeout']
        )

        print(f"Starting scraper: {config['scraper_name']}")
        data = scraper.fetch_data()

        if data:
            print("Processing employee data...")
            scraper.process_data(data)
            print(f"Employee data pipeline completed successfully! - {config['scraper_name']}")

if __name__ == "__main__":
    main()
