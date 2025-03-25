import json
import os
import requests

class EmployeeScraper:
    def __init__(self, api_url, retry_attempts=3, timeout=30):
        self.api_url = api_url
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def fetch_data(self):
        """Fetch employee data from API with retries and save raw data."""
        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(self.api_url, timeout=self.timeout)
                if response.status_code == 200:
                    raw_data = response.json()
                    self.save_json(raw_data, "output/raw_employees.json")
                    return raw_data  # Returns a list
                else:
                    print(f"Error: HTTP {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
        
        print("Failed to fetch data after retries.")
        return None

    def process_data(self, data):
        """Process employee data and save to JSON file."""
        processed_employees = []

        #  The API returns a list, so loop directly over `data`
        for emp in data:
            full_name = f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
            phone = "Invalid Number" if "x" in str(emp.get("phone", "")) else str(emp.get("phone", ""))
            
            # Determine Designation based on experience
            years_exp = emp.get("years_of_experience", 0)
            if years_exp < 3:
                designation = "System Engineer"
            elif 3 <= years_exp <= 5:
                designation = "Data Engineer"
            elif 5 < years_exp <= 10:
                designation = "Senior Data Engineer"
            else:
                designation = "Lead"

            processed_employees.append({
                "Full Name": full_name,
                "email": emp.get("email", ""),
                "phone": phone,
                "gender": emp.get("gender", ""),
                "age": emp.get("age", 0),
                "job_title": emp.get("job_title", ""),
                "years_of_experience": years_exp,
                "salary": emp.get("salary", 0),
                "department": emp.get("department", ""),
                "designation": designation
            })

        #  Save processed data to a JSON file
        self.save_json(processed_employees, "output/processed_employees.json")
        print("Processed data saved successfully!")
        return processed_employees
    
    def save_json(self, data, file_path):
        """Helper function to save data as a JSON file."""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data saved to {file_path}")
