import json
import os
import requests
import logging
import time
import pandas as pd
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
from datetime import datetime

class EmployeeScraper:
    def __init__(self, api_url: str, retry_attempts: int = 3, timeout: int = 30):
        self.api_url = api_url
        self.retry_attempts = retry_attempts
        self.timeout = timeout

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def fetch_data(self) -> Optional[List[Dict]]:
        """Fetch employee data from API with retries."""
        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(self.api_url, timeout=self.timeout)
                if response.status_code == 200:
                    raw_data = response.json()
                    self.save_all_formats(raw_data, "raw_employees")
                    return raw_data
                else:
                    self.logger.error(f"Error: HTTP {response.status_code}")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed (attempt {attempt + 1}/{self.retry_attempts}): {e}")

            if attempt < self.retry_attempts - 1:
                self.logger.info("Retrying in 3 seconds...")
                time.sleep(3)

        self.logger.error("Failed to fetch data after all retries.")
        return None

    def process_data(self, data: List[Dict]) -> List[Dict]:
        """Process employee data and save to JSON, XML, and Parquet."""
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

        self.save_all_formats(processed_employees, "processed_employees")
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

    def save_all_formats(self, data: List[Dict], base_name: str) -> None:
        """Save data in JSON, XML, and Parquet formats."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_path = f"output/{base_name}_{timestamp}"
            os.makedirs(os.path.dirname(base_path), exist_ok=True)

            # Save JSON
            json_path = f"{base_path}.json"
            with open(json_path, "w", encoding="utf-8") as json_file:
                json.dump(data, json_file, indent=4)
            self.logger.info(f"JSON data saved: {json_path}")

            # Save XML
            xml_path = f"{base_path}.xml"
            root = ET.Element("employees")
            for emp in data:
                emp_elem = ET.SubElement(root, "employee")
                for key, value in emp.items():
                    field = ET.SubElement(emp_elem, key.replace(" ", "_"))
                    field.text = str(value)
            tree = ET.ElementTree(root)
            tree.write(xml_path, encoding="utf-8", xml_declaration=True)
            self.logger.info(f"XML data saved: {xml_path}")

            # Save Parquet
            parquet_path = f"{base_path}.parquet"
            df = pd.DataFrame(data)
            df.to_parquet(parquet_path, index=False)
            self.logger.info(f"Parquet data saved: {parquet_path}")

        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
