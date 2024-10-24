from flask import Flask, request, jsonify
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2 import sql
from ratelimit import limits, sleep_and_retry
import json

app = Flask(__name__)

API_LIMIT = 50
ONE_MINUTE = 1
MAX_THREADS = 10


# Conexión a PostgreSQL
def get_db_connection():
    conn = psycopg2.connect(
        host="db",
        database="api_test",
        user="postgres",
        password="postgres",
    )
    return conn


@sleep_and_retry
@limits(calls=API_LIMIT, period=ONE_MINUTE)
def get_postal_code(lat, lon):
    url = f"https://api.postcodes.io/postcodes?lon={lon}&lat={lat}"
    headersList = {"Accept": "*/*"}
    payload = ""
    response = requests.request("GET", url, data=payload, headers=headersList)
    data = json.loads(response.text)

    if "error" in data:
        return None, f"Error: {data['error']['message']}"

    if data["result"] is None:
        load_data = None
        return load_data, "Not find data for this coordinates"
    elif len(data["result"]) > 0 and data["result"][0] is not None:
        load_data = data["result"][0]
        load_data = json.dumps(load_data)
        return load_data, None


def store_result_in_db(latitude, longitude, postal_code, error):
    conn = get_db_connection()
    cur = conn.cursor()
    query = sql.SQL(
        """
        INSERT INTO public.coordinates (latitude, longitude, postal_code, error)
        VALUES (%s, %s, %s, %s)
    """
    )

    cur.execute(query, (latitude, longitude, postal_code, error))
    conn.commit()

    cur.close()
    conn.close()


def process_row(row):
    try:
        postal_code, error = get_postal_code(row["lat"], row["lon"])
        store_result_in_db(row["lat"], row["lon"], postal_code, error)
        if error:
            return {
                "latitude": row["lat"],
                "longitude": row["lon"],
                "error": error,
            }
        return {
            "latitude": row["lat"],
            "longitude": row["lon"],
            "postal_code": "Data loaded into DB",
        }
    except Exception as e:
        store_result_in_db(row["lat"], row["lon"], None, str(e))
        return {
            "latitude": row["lat"],
            "longitude": row["lon"],
            "error": str(e),
        }


@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    try:

        def remove_quotes(val):
            return val.strip("'")

        df = pd.read_csv(
            file,
            sep="|",
            decimal=",",
            quotechar="'",
            converters={"lat": remove_quotes, "lon": remove_quotes},
        )
        df = df.replace(",", ".", regex=True)
        if "lat" not in df.columns or "lon" not in df.columns:
            return jsonify({"error": "Missing latitude or longitude columns"}), 400

        results = []
        errors = []

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_row, row) for _, row in df.iterrows()]

            for future in as_completed(futures):
                result = future.result()
                if "error" in result:
                    errors.append(result)
                else:
                    results.append(result)

        response = {"results": {"success": len(results), "errors": len(errors)}}

        return jsonify(response)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
