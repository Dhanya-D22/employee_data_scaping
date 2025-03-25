import unittest
from process.process import EmployeeScraper

class TestEmployeeScraper(unittest.TestCase):

    def setUp(self):
        self.scraper = EmployeeScraper("https://api.slingacademy.com/v1/sample-data/files/employees.json")

    def test_process_data(self):
        sample_data = {
            "employees": [
                {
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "john.doe@example.com",
                    "phone": "1234567890",
                    "gender": "Male",
                    "age": 30,
                    "job_title": "Software Engineer",
                    "years_of_experience": 4,
                    "salary": 60000,
                    "department": "IT"
                },
                {
                    "first_name": "Jane",
                    "last_name": "Smith",
                    "email": "jane.smith@example.com",
                    "phone": "987654321x",
                    "gender": "Female",
                    "age": 28,
                    "job_title": "Data Analyst",
                    "years_of_experience": 6,
                    "salary": 70000,
                    "department": "Data Science"
                }
            ]
        }
        processed = self.scraper.process_data(sample_data)
        self.assertEqual(processed[0]["Full Name"], "John Doe")
        self.assertEqual(processed[1]["phone"], "Invalid Number")
        self.assertEqual(processed[0]["designation"], "Data Engineer")
        self.assertEqual(processed[1]["designation"], "Senior Data Engineer")

if __name__ == '__main__':
    unittest.main()
