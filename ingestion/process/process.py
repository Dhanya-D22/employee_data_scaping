import os
import json
import time
import logging
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Optional

class EmployeeScraper:
    def __init__(self, api_url: str, retry_attempts: int = 3, timeout: int = 30):
        self.api_url = api_url
        self.retry_attempts = retry_attempts
        self.timeout = timeout

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def fetch_data(self) -> Optional[List[Dict]]:
        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(self.api_url, timeout=self.timeout)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, dict) and "users" in data:
                        employees = data["users"]
                    else:
                        employees = data
                    self.save_all_formats(employees, "raw_employees")
                    return employees
                else:
                    self.logger.error(f"HTTP Error: {response.status_code}")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Attempt {attempt + 1}: {e}")
                time.sleep(3)
        self.logger.error("Failed to fetch data after retries.")
        return None

    def process_data(self, data: List[Dict]) -> List[Dict]:
        processed = []
        for emp in data:
            try:
                processed.append(self._process_single_employee(emp))
            except Exception as e:
                self.logger.error(f"Processing error: {e}")
        self.save_all_formats(processed, "processed_employees")
        return processed

    def _process_single_employee(self, emp: Dict) -> Dict:
        full_name = f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
        phone = str(emp.get('phone', ''))
        phone = "Invalid Number" if 'x' in phone else phone

        years_exp = int(emp.get('years_of_experience', 0))
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
        if years_exp < 3:
            return "System Engineer"
        elif 3 <= years_exp <= 5:
            return "Data Engineer"
        elif 5 < years_exp <= 10:
            return "Senior Data Engineer"
        else:
            return "Lead"

    def save_all_formats(self, data: List[Dict], base_name: str) -> None:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_path = f"output/{base_name}_{timestamp}"
            os.makedirs("output", exist_ok=True)

            with open(f"{base_path}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)

            root = ET.Element("employees")
            for emp in data:
                emp_elem = ET.SubElement(root, "employee")
                for key, value in emp.items():
                    ET.SubElement(emp_elem, key.replace(" ", "_")).text = str(value)
            ET.ElementTree(root).write(f"{base_path}.xml", encoding="utf-8", xml_declaration=True)

            pd.DataFrame(data).to_parquet(f"{base_path}.parquet", index=False)
        except Exception as e:
            self.logger.error(f"Saving error: {e}")
