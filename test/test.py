import unittest
import json
import os
import pandas as pd
from employee import fetch_emp_data, process_emp_data

class TestEmployeeData(unittest.TestCase):
    def setUp(self):
        self.run_id = None
        
    def test_json_file_download(self):
        """Test Case 1: Verify JSON File Download"""
        data = fetch_emp_data()
        self.assertIsNotNone(data)
        self.assertIsInstance(data, list)

    def test_json_file_extraction(self):
        """Test Case 2: Verify JSON File Extraction"""
        data = fetch_emp_data()
        self.assertTrue(len(data) > 0)
        self.assertIsInstance(data[0], dict)

    def test_file_type_and_format(self):
        """Test Case 3: Validate File Type and Format"""
        self.run_id = process_emp_data()
        self.assertIsNotNone(self.run_id)
        
        csv_file = f"emp_{self.run_id}.csv"
        self.assertTrue(os.path.exists(csv_file))
        self.assertTrue(csv_file.endswith('.csv'))
        
        df = pd.read_csv(csv_file)
        self.assertIsInstance(df, pd.DataFrame)

    def test_data_structure(self):
        """Test Case 4: Validate Data Structure"""
        self.run_id = process_emp_data()
        df = pd.read_csv(f"emp_{self.run_id}.csv")
        
        self.assertIn('Full Name', df.columns)
        self.assertEqual(df['Full Name'].dtype, 'object')

    def test_missing_invalid_data(self):
        """Test Case 5: Handle Missing or Invalid Data"""
        self.run_id = process_emp_data()
        df = pd.read_csv(f"emp_{self.run_id}.csv")
        
        self.assertFalse(df['Full Name'].isnull().any())
        self.assertTrue(all(isinstance(name, str) for name in df['Full Name']))

if __name__ == '__main__':
    unittest.main()
