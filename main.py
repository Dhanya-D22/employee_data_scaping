from process.process import EmployeeScraper

def main():
    api_url = "https://api.slingacademy.com/v1/sample-data/files/employees.json"

    scraper = EmployeeScraper(api_url)
    
    print("Fetching employee data")
    data = scraper.fetch_data()

    if data:
        print(" Processing employee data")
        scraper.process_data(data)
        print("Employee data pipeline completed successfully!")

if __name__ == "__main__":
    main()
