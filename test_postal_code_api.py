from flask import Flask
import unittest
from unittest.mock import patch, MagicMock
import json
from app import get_postal_code, store_result_in_db, process_row

app = Flask(__name__)


class TestPostalCodeAPI(unittest.TestCase):

    @patch("app.requests.request")
    def test_get_postal_code_success(self, mock_request):
        mock_response = MagicMock()
        mock_response.text = json.dumps({"result": [{"postcode": "SW1A 1AA"}]})
        mock_request.return_value = mock_response

        postal_code, error = get_postal_code(51.5014, -0.1419)
        self.assertEqual(postal_code, json.dumps({"postcode": "SW1A 1AA"}))
        self.assertIsNone(error)

    @patch("app.requests.request")
    def test_get_postal_code_error(self, mock_request):
        mock_response = MagicMock()
        mock_response.text = json.dumps({"error": {"message": "Invalid coordinates"}})
        mock_request.return_value = mock_response

        postal_code, error = get_postal_code(999, 999)
        self.assertIsNone(postal_code)
        self.assertEqual(error, "Error: Invalid coordinates")

    @patch("app.get_db_connection")
    def test_store_result_in_db(self, mock_get_db_connection):
        mock_conn = MagicMock()
        mock_get_db_connection.return_value = mock_conn

        store_result_in_db(51.5014, -0.1419, "SW1A 1AA", None)
        mock_conn.cursor().execute.assert_called_once()

    @patch("app.get_postal_code")
    @patch("app.store_result_in_db")
    def test_process_row_success(self, mock_store_result_in_db, mock_get_postal_code):
        mock_get_postal_code.return_value = (json.dumps({"postcode": "SW1A 1AA"}), None)
        result = process_row({"lat": 51.5014, "lon": -0.1419})

        self.assertEqual(
            result,
            {
                "latitude": 51.5014,
                "longitude": -0.1419,
                "postal_code": "Data loaded into DB",
            },
        )
        mock_store_result_in_db.assert_called_once()

    @patch("app.get_postal_code")
    @patch("app.store_result_in_db")
    def test_process_row_error(self, mock_store_result_in_db, mock_get_postal_code):
        mock_get_postal_code.return_value = (None, "Error: Invalid coordinates")
        result = process_row({"lat": 999, "lon": 999})

        self.assertEqual(
            result,
            {
                "latitude": 999,
                "longitude": 999,
                "error": "Error: Invalid coordinates",
            },
        )
        mock_store_result_in_db.assert_called_once()


if __name__ == "__main__":
    unittest.main()
