import hashlib
from io import StringIO

import pandas as pd
from openpyxl.utils import get_column_letter

from app.core.logging_engine import *


def save_to_excel(data_pull_csv, comparison_csv, excel_path):
    try:
        # Read CSV data into pandas DataFrames
        data_pull_df = pd.read_csv(StringIO(data_pull_csv)).apply(pd.to_numeric, errors='ignore')
        comparison_df = pd.read_csv(StringIO(comparison_csv)).apply(pd.to_numeric, errors='ignore')

        # Check if both DataFrames are identical
        hash_pull = hashlib.sha256(data_pull_df.to_string().encode()).hexdigest()
        hash_comp = hashlib.sha256(comparison_df.to_string().encode()).hexdigest()

        match = hash_pull == hash_comp

        # Replace 'Unnamed' columns with empty string
        data_pull_df.columns = ['' if 'Unnamed' in str(col) else col for col in data_pull_df.columns]
        comparison_df.columns = ['' if 'Unnamed' in str(col) else col for col in comparison_df.columns]

        # Create an empty DataFrame for the variance with the same columns and indices
        variance_df = pd.DataFrame(index=comparison_df.index, columns=comparison_df.columns)

        # Populate the variance DataFrame with Excel formula placeholders
        for col in range(len(comparison_df.columns)):
            for row in comparison_df.index:
                col_letter = get_column_letter(col + 1)
                row_number = str(row + 2)
                variance_df.iat[row, col] = f"='Data Pull'!{col_letter}{row_number} = 'Comparison'!{col_letter}{row_number}"

        # Save to Excel
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            data_pull_df.to_excel(writer, sheet_name='Data Pull', index=False)
            comparison_df.to_excel(writer, sheet_name='Comparison', index=False)
            variance_df.to_excel(writer, sheet_name='Variance', index=False)

        return 200 if match else 412

    except Exception as e:
        logging.error(f"Error in save_to_excel: {e}")
        return 500