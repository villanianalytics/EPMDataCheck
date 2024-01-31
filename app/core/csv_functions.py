import csv
import io

from starlette.responses import JSONResponse

from app.core.logging_engine import *


def csv_to_json(
        file: str,
        pov_dimensions: str ,
        pov_dimension_members: str ,
        col_dimensions: str ,
        row_dimensions: str ):

    # Read the uploaded CSV file into a DataFrame
    content = await file.read()
    csv_content = io.StringIO(content.decode())
    df = pd.read_csv(csv_content, header=None)

    # Replace NaNs and Infs
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.fillna("", inplace=True)

    pov_dimensions_list = pov_dimensions.split(", ")
    pov_dimension_members_list = pov_dimension_members.split(", ")
    col_dimensions_list = col_dimensions.split(", ")
    row_dimensions_list = row_dimensions.split(", ")

    # Read column members
    col_members = []
    for col_index in range(len(row_dimensions_list), df.shape[1]):
        members_for_col = []
        for row_index in range(len(col_dimensions_list)):
            members_for_col.append([df.iloc[row_index, col_index]])
        col_members.append({"dimensions": col_dimensions_list, "members": members_for_col})

    # Read row members
    row_members = []
    for row_index in range(len(col_dimensions_list), df.shape[0]):
        members_for_row = []
        for col_index in range(len(row_dimensions_list)):
            members_for_row.append([df.iloc[row_index, col_index]])
        row_members.append({"dimensions": row_dimensions_list, "members": members_for_row})

    # Prepare the payload
    payload = {
        "exportPlanningData": False,
        "gridDefinition": {
            "suppressMissingBlocks": True,
            "suppressMissingRows": False,
            "suppressMissingColumns": False,
            "pov": {
                "dimensions": pov_dimensions_list,
                "members": [[m] for m in pov_dimension_members_list]
            },
            "columns": col_members,
            "rows": row_members
        }
    }

    return JSONResponse(content=payload)
