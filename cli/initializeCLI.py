import json

from app.core.logging_engine import *
from app.core.json_functions import *
from app.core.csv_functions import *
import argparse


def run_cli():
    # Create the top-level parser
    parser = argparse.ArgumentParser(description="CLI for managing operations")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Create the parser for the "count-keys" command
    parser_json_to_csv = subparsers.add_parser('json-to-csv', help='Convert EPM JSON Data to CSV format')
    parser_json_to_csv.add_argument('--json_data', help='JSON data as a string', type=str)
    parser_json_to_csv.add_argument('--file', help='Path to a file with JSON data in it', type=str)

    parser_csv_to_json = subparsers.add_parser('csv-to-json', help='Convert CSV to EPM JSON Format')
    parser_csv_to_json.add_argument('--file', help='Path to a file with csv data in it', type=str)
    parser_csv_to_json.add_argument('--pov_dimensions', help='Path to a file with csv data in it', type=str)
    parser_csv_to_json.add_argument('--pov_dimension_members', help='Path to a file with csv data in it', type=str)
    parser_csv_to_json.add_argument('--col_dimensions', help='Path to a file with csv data in it', type=str)
    parser_csv_to_json.add_argument('--row_dimensions', help='Path to a file with csv data in it', type=str)

    # Parse the arguments
    args = parser.parse_args()

    # Handle each command
    if args.command == 'json-to-csv':
        json_data = None
        if args.json_data:
            try:
                json_data = json.loads(args.json_data)
                json_to_csv(json_data)
                print(f"JSON data converted to CSV")
            except json.JSONDecodeError:
                print("Invalid JSON data provided.")
            except Exception as e:
                print(f"An error occurred: {str(e)}")
        elif args.file:
            try:
                with open(args.file, 'r') as file:
                    json_data = json.load(file)
            except Exception as e:
                print(f"An error occurred while reading the file: {str(e)}")
                return

        if json_data is not None:
            try:
                csv_data = json_to_csv(json_data)
                print(f"JSON data converted to CSV:\n{csv_data}")
            except Exception as e:
                print(f"An error occurred during conversion: {str(e)}")
        else:
            print("No JSON data provided.")

    elif args.command == 'csv-to-json':
        csv_data = None
        csv_to_json(args.file,
                    args.pov_dimensions,
                    args.pov_dimension_members,
                    args.col_dimensions,
                    args.row_dimensions)

    else:
        parser.print_help()