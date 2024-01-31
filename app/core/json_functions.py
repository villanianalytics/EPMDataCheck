import csv
import io

from app.core.logging_engine import *


def json_to_csv(json_data):
    try:
        logging.info("Inside json_to_csv function")
        output = io.StringIO()
        csv_writer = csv.writer(output)

        # Determine the number of empty cells needed in the header
        first_row_headers = json_data.get('rows', [])[0].get('headers', [])
        empty_cells = len(first_row_headers)

        # Write the Column headers
        for col in json_data.get('columns', []):
            csv_writer.writerow([''] * empty_cells + col)

        # Write Rows
        for row in json_data.get('rows', []):
            headers = row.get('headers', [])
            data = row.get('data', [])
            csv_writer.writerow(headers + data)

        return output.getvalue()
    except Exception as e:
        logging.error(f"Error in json_to_csv: {e}")
        raise e