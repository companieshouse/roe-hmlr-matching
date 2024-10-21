# -*- coding: utf-8 -*-
"""
Rework of HMLR Script to use monthly Land Registry Extracts

Original script called helpers written by Omolara Ajayi and James Gough

Created on Tue Sep  3 10:00:29 2024
@author: wburkett
"""


# %%
import json
import os
import re
from datetime import datetime
from pathlib import Path

import cx_Oracle
import pandas as pd
from cleanco.clean import basename
from sqlalchemy import create_engine


def get_roe_data(config_file: Path) -> pd.DataFrame:
    """
    Connects to the oracle database and returns the ROE data
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file {config_file} not found.")

    with open(config_file, "r") as f:
        config = json.load(f)

    dsn_tns = cx_Oracle.makedsn(config["host"], config["port"], config["sid"])
    conn = f'oracle+cx_oracle://{config["user"]}:{config["password"]}@{dsn_tns}'

    engine = create_engine(conn, pool_recycle=10, pool_size=50, echo=False)

    # Runs a query on the CHIPS database to extract the full list of ROE
    # companies on the register.
    # - corporate_body_type_id = 37 is for ROE companies
    # - action_code_type_id < 9000 is for live companies
    query = """
                SELECT
                    incorporation_number,
                    corporate_body_name,
                    incorporation_date
                FROM
                    corporate_body cb
                WHERE
                    corporate_body_type_id = 37
                AND 
                    action_code_type_id < 9000  
                """
    roe_df = pd.read_sql_query(query, engine)
    roe_df.columns = roe_df.columns.str.lower()

    return roe_df


def clean_company_name(company_name: str):
    """
    Converts string to lower case, removes anything not a word, number or space, and removes various suffixes via
    the cleanco package.
    :param company_name:
    """
    company_name = str(company_name).lower()
    company_name = re.sub(r"[^\w\d\s]", "", company_name)
    company_name = basename(company_name)
    company_name = re.sub(" ", "", company_name)
    company_name = re.sub(
        r"\ss\w\srl$", "", company_name
    )  # This is to remove SRL suffix in particular.
    return company_name


def get_newest_hmlr_file(folder_path: Path) -> pd.DataFrame:
    """
    Reads the most recent HMLR Excel file from a folder based on the date in the
    filename.

    The function looks for files following the naming convention
    'RXN_DD_MMM_YYYY.xlsx'. It ignores any other files and returns the contents
    of the most recent file as a pandas DataFrame.
    """
    pattern = re.compile(r"RXN_(\d{2})_(\w{3})_(\d{4})\.xlsx")
    month_map = {
        month: index
        for index, month in enumerate(
            [
                "JAN",
                "FEB",
                "MAR",
                "APR",
                "MAY",
                "JUN",
                "JUL",
                "AUG",
                "SEP",
                "OCT",
                "NOV",
                "DEC",
            ],
            start=1,
        )
    }
    folder = Path(folder_path)

    files_with_dates = [
        (file, datetime(int(year), month_map[month_str.upper()], int(day)))
        for file in folder.iterdir()
        if file.is_file() and (match := pattern.match(file.name))
        for day, month_str, year in [match.groups()]
    ]

    if not files_with_dates:
        raise FileNotFoundError(
            "No valid files found in the folder following the RXN_DD_MMM_YYYY.xlsx naming convention."
        )

    newest_file = max(files_with_dates, key=lambda x: x[1])[0]

    df = pd.read_excel(newest_file)
    df.columns = df.columns.str.lower()
    return df


def get_newest_exclusion_list(folder_path: Path) -> pd.DataFrame:
    """
    Reads the most recent Exclusion Excel file from a folder based on the date in the
    filename.

    The function looks for files following the naming convention
    'YYYY-MM-DD-exclusions.xlsx'. It ignores any other files and returns the contents
    of the most recent file as a pandas DataFrame.
    """
    pattern = re.compile(r"(\d{4})-(\d{2})-(\d{2})-exclusions\.xlsx")

    folder = Path(folder_path)

    files_with_dates = [
        (file, datetime(int(year), int(month), int(day)))
        for file in folder.iterdir()
        if file.is_file() and (match := pattern.match(file.name))
        for year, month, day in [match.groups()]
    ]

    if not files_with_dates:
        raise FileNotFoundError(
            "No valid files found in the folder following the YYYY-MM-DD-exclusions.xlsx naming convention."
        )

    newest_file = max(files_with_dates, key=lambda x: x[1])[0]

    df = pd.read_excel(newest_file)
    df.columns = df.columns.str.lower()
    return df


def reshape_hmlr_proprietors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshapes the HMLR data into a long format.

    In the original file, multiple proprietors are stored per row, so this
    function splits those proprietors so each has its own row.
    """

    NUM_PROPRIETORS = 4
    df_melted = pd.DataFrame()

    for i in range(1, NUM_PROPRIETORS + 1):

        temp_df = df[
            [
                "title_number",
                "tenure",
                "property_address",
                "district",
                "county",
                "region",
                "price_paid",
                f"proprietor_name_{i}",
                f"proprietor_{i}_address_1",
                f"proprietor_{i}_address_2",
                f"proprietor_{i}_address_3",
                "date_proprieter_added_updated",
                "extract_date",
            ]
        ].copy()

        temp_df.columns = [
            "title_number",
            "tenure",
            "property_address",
            "district",
            "county",
            "region",
            "price_paid",
            "proprietor_name",
            "proprietor_address_1",
            "proprietor_address_2",
            "proprietor_address_3",
            "date_proprieter_added_updated",
            "extract_date",
        ]

        df_melted = pd.concat([df_melted, temp_df], ignore_index=True)

    # Remove rows where proprietor_name is blank or NaN
    df_melted = df_melted.dropna(subset=["proprietor_name"])

    return df_melted


# %%
def main():
    """
    This function combines the functions above to run the entire pipeline.
    Two files with the unmatched HMLR and ROE companies will be output in the
    files subfolder of this script.
    """

    # Get data -----------------------------------------------------------------

    hmlr_df = get_newest_hmlr_file(folder_path="inputs/hmlr-data")
    hmlr_df = reshape_hmlr_proprietors(hmlr_df)
    hmlr_df["clean_proprietor_name"] = (
        hmlr_df["proprietor_name"].astype(str).apply(clean_company_name)
    )

    roe_df = get_roe_data(config_file="config.json")
    roe_df["clean_company_name"] = (
        roe_df["corporate_body_name"].astype(str).apply(clean_company_name)
    )

    exclusions_df = get_newest_exclusion_list(folder_path="inputs")
    exclusions_df["clean_entity_name"] = (
        exclusions_df["entity name (from hmlr datasets)"].astype(str).apply(clean_company_name)
    )

    roe_df["excluded_bool"] = roe_df["clean_company_name"].isin(
        exclusions_df["clean_entity_name"]
    )

    hmlr_df["excluded_bool"] = hmlr_df["clean_proprietor_name"].isin(
        exclusions_df["clean_entity_name"]
    )

    # Stores the date today in a YYYY-MM-DD format variable for use when saving the unmatched dataframes

    date_today = datetime.today().strftime("%Y-%m-%d")

    # Save the unmatched HMLR holdings -----------------------------------------

    hmlr_unmatched_in_roe_df = hmlr_df[
        ~hmlr_df["clean_proprietor_name"].isin(roe_df["clean_company_name"]) & ~hmlr_df["excluded_bool"]
    ].sort_values(by=["clean_proprietor_name"]).drop("excluded_bool", axis=1)

    hmlr_unmatched_in_roe_df.to_excel(
        f"./outputs/{date_today}-HMLR-unmatched.xlsx", index=False
    )

    # Save the unmatched ROE entities ------------------------------------------

    roe_unmatched_in_hmlr_df = roe_df[
        ~roe_df["clean_company_name"].isin(hmlr_df["clean_proprietor_name"]) & ~roe_df["excluded_bool"]
    ].sort_values(by=["clean_company_name"]).drop("excluded_bool", axis=1)

    roe_unmatched_in_hmlr_df.to_excel(
        f"./outputs/{date_today}-ROE-unmatched.xlsx", index=False
    )

    hmlr_df_unique_proprietors = hmlr_df.drop_duplicates(
        subset=["clean_proprietor_name"],
        keep="first",
    )

    # Statistics ---------------------------------------------------------------

    # Transforming to a set to get a count of the unique company names in the
    # HMLR dataset.
    hmlr_unique_proprietors_count = len(
        hmlr_df_unique_proprietors["clean_proprietor_name"]
    )
    hmlr_excluded_proprietors_count = sum(
        hmlr_df_unique_proprietors["excluded_bool"]
    )
    # Getting the count for how many unique hmlr companies we have in ans not in
    # our ROE database.
    hmlr_unmatched_roe_count = len(
        hmlr_unmatched_in_roe_df["clean_proprietor_name"].unique()
    )
    hmlr_matched_roe_count = hmlr_unique_proprietors_count - hmlr_unmatched_roe_count - hmlr_excluded_proprietors_count

    # Getting the percentage of HMLR companies that we have in the database.
    matched_roe_percentage = (
        hmlr_matched_roe_count / (hmlr_unique_proprietors_count) * 100
    )

    print(
        f"The number of unique hmlr proprietors on the list is: {hmlr_unique_proprietors_count}."
    )
    print(
        f"The number of hmlr proprietors matched in ROE is: {hmlr_matched_roe_count}."
    )
    print(
        f"The number of hmlr proprietors excluded is: {hmlr_excluded_proprietors_count}"
    )
    print(
        f"The number of hmlr proprietors not matched or excluded in ROE is: {hmlr_unmatched_roe_count}."
    )

    print(
        f"The proportion of proprietors on the ROE register is: {matched_roe_percentage:.2f}%."
    )
    print(
        f"The number of overseas entities on the ROE register is: {len(roe_df)}"
    )


if __name__ == "__main__":
    main()

# %%
