import unittest
import os
import json
from ingestion.process.process import EmployeeScraper

class TestEmployeeScraper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("ingestion/run_scraper.json") as f:
            config = json.load(f)[0]
        cls.scraper = EmployeeScraper(
            api_url=config["api_url"],
            retry_attempts=config["retry_attempts"],
            timeout=config["timeout"]
        )

    def test_1_json_download(self):
        """Test Case 1: Verify JSON File Download"""
        data = self.scraper.fetch_data()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)

    def test_2_json_structure(self):
        """Test Case 2: Verify JSON File Extraction"""
        data = self.scraper.fetch_data()
        sample = data[0]
        self.assertIn("first_name", sample)
        self.assertIn("last_name", sample)
        self.assertIn("email", sample)

    def test_3_processed_format(self):
        """Test Case 3: Validate File Type and Format"""
        data = self.scraper.fetch_data()
        processed = self.scraper.process_data(data)
        sample = processed[0]
        expected_keys = {
            "Full Name", "email", "phone", "gender", "age",
            "job_title", "years_of_experience", "salary", "department", "designation"
        }
        self.assertTrue(set(sample.keys()).issuperset(expected_keys))

    def test_4_invalid_phone(self):
        """Test Case 4: Handle Missing or Invalid Data"""
        test_record = {
            "first_name": "A", "last_name": "B", "phone": "123x456", "email": "", 
            "gender": "", "age": 0, "job_title": "", "years_of_experience": 0, 
            "salary": 0, "department": ""
        }
        result = self.scraper._process_single_employee(test_record)
        self.assertEqual(result["phone"], "Invalid Number")

    def test_5_designation_mapping(self):
        """Test Case 5: Validate Data Structure"""
        mapping = {
            2: "System Engineer",
            4: "Data Engineer",
            7: "Senior Data Engineer",
            12: "Lead"
        }
        for exp, expected in mapping.items():
            self.assertEqual(self.scraper._get_designation(exp), expected)

if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromTestCase(TestEmployeeScraper))
