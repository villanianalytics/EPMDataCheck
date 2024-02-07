import json

from app.core.logging_engine import *
from app.core.json_functions import *
from app.core.csv_functions import *
from app.core.xlsx_functions import *
from app.core.epm_functions import *
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

    parser_save_to_excel = subparsers.add_parser('save-to-excel', help='Create Excel comparison file')
    parser_save_to_excel.add_argument('--source', help='Path to source file with csv data in it', type=str)
    parser_save_to_excel.add_argument('--target', help='Path to target file with csv data in it', type=str)
    parser_save_to_excel.add_argument('--excel-destination', help='Path to save Excel file', type=str)
    parser_save_to_excel.add_argument('--srcHeaders', help='Number of Source Header Rows', type=str)
    parser_save_to_excel.add_argument('--tgtHeaders', help='Number of Target Header Rows', type=str)
    parser_save_to_excel.add_argument('--rowDims', help='Comma separated list of Row Dimension names', type=str)

    parser_export_slice = subparsers.add_parser('export-data-slice', help='Exports data from an Oracle EPM application')
    parser_export_slice.add_argument('--base_url', help='base application URL', type=str)
    parser_export_slice.add_argument('--username', help='username to log into application', type=str)
    parser_export_slice.add_argument('--password', help='password for application', type=str)
    parser_export_slice.add_argument('--app_name', help='application to export from', type=str)
    parser_export_slice.add_argument('--api_version', help='API version', type=str)
    parser_export_slice.add_argument('--plan_type_name', help='name of plan type to pull from', type=str)
    parser_export_slice.add_argument('--payload', help='Path to json export payload or json', type=str)

    parser_import_slice = subparsers.add_parser('import-data-slice', help='Imports data to an Oracle EPM application')
    parser_import_slice.add_argument('--base_url', help='base application URL', type=str)
    parser_import_slice.add_argument('--username', help='username to log into application', type=str)
    parser_import_slice.add_argument('--password', help='password for application', type=str)
    parser_import_slice.add_argument('--app_name', help='application to import to', type=str)
    parser_import_slice.add_argument('--api_version', help='API version', type=str)
    parser_import_slice.add_argument('--plan_type_name', help='name of plan type to import to', type=str)
    parser_import_slice.add_argument('--payload', help='Path to json import payload or json', type=str)

    parser_run_epm_job = subparsers.add_parser('run-epm-job', help='Runs an EPM Job')
    parser_run_epm_job.add_argument('--base_url', help='base application URL', type=str)
    parser_run_epm_job.add_argument('--username', help='username to log into application', type=str)
    parser_run_epm_job.add_argument('--password', help='password for application', type=str)
    parser_run_epm_job.add_argument('--app_name', help='application to import to', type=str)
    parser_run_epm_job.add_argument('--api_version', help='API version', type=str)
    parser_run_epm_job.add_argument('--job_type', help='name of plan type to import to', type=str)
    parser_run_epm_job.add_argument('--job_name', help='name of plan type to import to', type=str)
    parser_run_epm_job.add_argument('--parameters', help='Path to json import payload or json', type=str)
    parser_run_epm_job.add_argument('--poll_interval', help='Path to json import payload or json', type=str)
    parser_run_epm_job.add_argument('--max_retries', help='Path to json import payload or json', type=str)

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

    elif args.command == 'save-to-excel':
        if args.rowDims:
            row_dimensions = args.rowDims.split(',')
        else:
            row_dimensions = []
        save_to_excel_with_hash_check(args.source,
                                      args.target,
                                      args.excel_destination,
                                      args.srcHeaders,
                                      args.tgtHeaders,
                                      row_dimensions)

    elif args.command == 'export-data-slice':
        export_data_slice_json(
            args.base_url,
            args.username,
            args.password,
            args.app_name,
            args.api_version,
            args.plan_type_name,
            args.payload)

    elif args.command == 'import-data-slice':
        import_data_slice_json(
            args.base_url,
            args.username,
            args.password,
            args.app_name,
            args.api_version,
            args.plan_type_name,
            args.payload)

    elif args.command == 'run-epm-job':
        run_job(
            base_url=args.base_url,
            api_version=args.api_version,
            application=args.app_name,
            job_type=args.job_type,
            job_name=args.job_name,
            username=args.username,
            password=args.password,
            parameters=args.parameters,
            poll_interval=args.poll_interval,
            max_retries=args.max_retries)

    else:
        parser.print_help()
