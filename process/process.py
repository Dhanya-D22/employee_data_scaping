import json
import os
import requests
import logging
from typing import List, Dict, Optional

class EmployeeScraper:
    def __init__(self, api_url: str, retry_attempts: int = 3, timeout: int = 30):
        self.api_url = api_url
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        
        #  logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def fetch_data(self) -> Optional[List[Dict]]:
        """Fetch employee data from API with retries and save raw data."""
        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(self.api_url, timeout=self.timeout)
                if response.status_code == 200:
                    raw_data = response.json()
                    self.save_json(raw_data, "output/raw_employees.json")
                    return raw_data
                else:
                    self.logger.error(f"Error: HTTP {response.status_code}")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed (attempt {attempt + 1}/{self.retry_attempts}): {e}")
            
            if attempt < self.retry_attempts - 1:
                self.logger.info(f"Retrying in 3 seconds...")
                time.sleep(3)
        
        self.logger.error("Failed to fetch data after all retries.")
        return None

    def process_data(self, data: List[Dict]) -> List[Dict]:
        """Process employee data and save to JSON file."""
        if not data:
            self.logger.error("No data to process")
            return []

        processed_employees = []
        for emp in data:
            try:
                processed_emp = self._process_single_employee(emp)
                processed_employees.append(processed_emp)
            except Exception as e:
                self.logger.error(f"Error processing employee: {e}")
                continue

        self.save_json(processed_employees, "output/processed_employees.json")
        self.logger.info(f"Successfully processed {len(processed_employees)} employees")
        return processed_employees

    def _process_single_employee(self, emp: Dict) -> Dict:
        """Process a single employee record."""
        full_name = f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
        phone = "Invalid Number" if "x" in str(emp.get("phone", "")) else str(emp.get("phone", ""))
        years_exp = int(emp.get("years_of_experience", 0))

        designation = self._get_designation(years_exp)

        return {
            "Full Name": full_name,
            "email": str(emp.get("email", "")),
            "phone": phone,
            "gender": str(emp.get("gender", "")),
            "age": int(emp.get("age", 0)),
            "job_title": str(emp.get("job_title", "")),
            "years_of_experience": years_exp,
            "salary": int(emp.get("salary", 0)),
            "department": str(emp.get("department", "")),
            "designation": designation
        }

    def _get_designation(self, years_exp: int) -> str:
        """Determine designation based on years of experience."""
        if years_exp < 3:
            return "System Engineer"
        elif 3 <= years_exp <= 5:
            return "Data Engineer"
        elif 5 < years_exp <= 10:
            return "Senior Data Engineer"
        else:
            return "Lead"

    def save_json(self, data: List[Dict], file_path: str) -> None:
        """Save data as a JSON file."""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as json_file:
                json.dump(data, json_file, indent=4)
            self.logger.info(f"Data saved to {file_path}")
        except Exception as e:
            self.logger.error(f"Error saving JSON file: {e}")
