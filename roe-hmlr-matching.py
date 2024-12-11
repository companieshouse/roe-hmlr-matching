# -*- coding: utf-8 -*-
"""
Rework of HMLR Script to use monthly Land Registry Extracts

Based on a script called helpers written by Omolara Ajayi and James Gough

Created on Tue Sep  3 10:00:29 2024
@author: wburkett
"""


# %%
import json
import os
import re
from datetime import datetime
from pathlib import Path
from unidecode import unidecode
import cx_Oracle
import pandas as pd
from cleanco.clean import basename
from sqlalchemy import create_engine
import rapidfuzz
from rapidfuzz import process


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
    # this corrects some common variances that have been found to cause missed matches
    # if the script need to adjust more it could be an option to include these in a txt that is imported instead
    company_name = re.sub("&", "and", company_name)
    company_name = re.sub("street", "st", company_name)
    company_name = re.sub("invesment", "investment", company_name)
    company_name = re.sub("investments", "investment", company_name)
    # this converts accented characters to the closest equivalent letter
    company_name = unidecode(company_name)
    company_name = re.sub(r"[^\w\d\s]", "", company_name)
    company_name = basename(company_name)
    # this is a list of suffixes that have been found that are not
    # correctly removed using the basename function in cleanco
    # similar to the adjustments above we could potentially add these to a txt file to allow others to update external to the script
    suffix = [
        "sa rl",
        "s a r l",
        "pty",
        "holdings",
        "holding",
        "s a",
        "limitada",
        "cy",
        "sb",
        "dac",
        "public",
        "sdnbhd",
        "coltd",
        "designated activity",
        "properties",
        "lp",
        "pteltd",
        "b v",
        "s s",
    ]
    for word in suffix:
        company_name = re.sub(rf"\b{word}\b", "", company_name)
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
    Reshapes the HMLR data from a wide format into a long format (each proprietor has their own row).

    In the original file, up to 5 proprietors are stored per row, so this
    function splits those proprietors so each has its own row containing
    their name and address against the associated title_number and property.
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
                f"country_incorporated_{i}",
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
            "country_incorporated",
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
    outputs subfolder of this script.
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

    exclusions_df = get_newest_exclusion_list(folder_path="inputs/exclusions")
    exclusions_df["clean_entity_name"] = (
        exclusions_df["entity name (from hmlr datasets)"]
        .astype(str)
        .apply(clean_company_name)
    )

    # checks whether company is found on the exclusion list and adds a TRUE/FALSE boolean column
    roe_df["excluded_bool"] = roe_df["clean_company_name"].isin(
        exclusions_df["clean_entity_name"]
    )

    # checks whether proprietor is found on the exclusion list and adds a TRUE/FALSE boolean column
    hmlr_df["excluded_bool"] = hmlr_df["clean_proprietor_name"].isin(
        exclusions_df["clean_entity_name"]
    )

    # Stores the date today in a YYYY-MM-DD format variable for use when saving the unmatched dataframes
    date_today = datetime.today().strftime("%Y-%m-%d")

    # Creates the unmatched HMLR holdings -----------------------------------------
    # This compares the HMLR dataset against the ROE dataset and
    # returns a dataframe of the HMLR proprietors that do not have a match
    hmlr_unmatched_in_roe_df = (
        hmlr_df[
            ~hmlr_df["clean_proprietor_name"].isin(roe_df["clean_company_name"])
            & ~hmlr_df["excluded_bool"]
        ]
        .sort_values(by=["clean_proprietor_name"])
        .drop("excluded_bool", axis=1)
    )

    # Finds the closest match in the ROE dataframe and adds 3 columns
    # closest_match_in_roe stores the clean_company_name that is the closest match
    # accuracy_ratio is the ratio of how close a match is to the HMLR clean_proprietor_name
    # index is the index of the row the match was found (this is dropped immediately after)
    hmlr_unmatched_in_roe_df[
        ["closest_match_in_roe", "accuracy_ratio", "index"]
    ] = hmlr_unmatched_in_roe_df["clean_proprietor_name"].apply(
        lambda x: pd.Series(
            process.extractOne(
                x, roe_df["clean_company_name"], scorer=rapidfuzz.fuzz.ratio
            )
        )
    )

    # sorts the dataframe by the accuracy_ratio of the match and drops the index column
    hmlr_unmatched_in_roe_df = hmlr_unmatched_in_roe_df.sort_values(
        by=["accuracy_ratio"], ascending=False
    ).drop("index", axis=1)

    # Saves the unmatched holdings
    hmlr_unmatched_in_roe_df.to_excel(
        f"./outputs/{date_today}-HMLR-unmatched.xlsx", index=False
    )

    # Creates a list of unique hmlr proprietors
    hmlr_df_unique_proprietors = hmlr_df.drop_duplicates(
        subset=["clean_proprietor_name"],
        keep="first",
    )

    # Creates the unmatched ROE entities ------------------------------------------
    # This compares the ROE dataset against the HMLR dataset and
    # returns a dataframe of the ROE entities that do not have a match
    roe_unmatched_in_hmlr_df = (
        roe_df[
            ~roe_df["clean_company_name"].isin(hmlr_df["clean_proprietor_name"])
            & ~roe_df["excluded_bool"]
        ]
        .sort_values(by=["clean_company_name"])
        .drop("excluded_bool", axis=1)
    )

    # Finds the closest match in the HMLR dataframe and adds 3 columns
    # closest_match_in_hmlr stores the clean_proprietor_name that is the closest match
    # accuracy_ratio is the ratio of how close a match is to the ROE clean_company_name
    # index is the index of the row the match was found (this is dropped immediately after)
    roe_unmatched_in_hmlr_df[
        ["closest_match_in_hmlr", "accuracy_ratio", "index"]
    ] = roe_unmatched_in_hmlr_df["clean_company_name"].apply(
        lambda x: pd.Series(
            process.extractOne(
                x,
                hmlr_df_unique_proprietors["clean_proprietor_name"],
                scorer=rapidfuzz.fuzz.ratio,
            )
        )
    )

    # sorts the dataframe by the accuracy_ratio of the match and drops the index column
    roe_unmatched_in_hmlr_df = roe_unmatched_in_hmlr_df.sort_values(
        by=["accuracy_ratio"], ascending=False
    ).drop("index", axis=1)

    # Saves the unmatched holdings
    roe_unmatched_in_hmlr_df.to_excel(
        f"./outputs/{date_today}-ROE-unmatched.xlsx", index=False
    )

    # Statistics ---------------------------------------------------------------

    # Transforming to a set to get a count of the unique company names in the
    # HMLR dataset.
    hmlr_unique_proprietors_count = len(
        hmlr_df_unique_proprietors["clean_proprietor_name"]
    )
    hmlr_excluded_proprietors_count = sum(hmlr_df_unique_proprietors["excluded_bool"])
    # Getting the count for how many unique hmlr companies we have in ans not in
    # our ROE database.
    hmlr_unmatched_roe_count = len(
        hmlr_unmatched_in_roe_df["clean_proprietor_name"].unique()
    )
    hmlr_matched_roe_count = (
        hmlr_unique_proprietors_count
        - hmlr_unmatched_roe_count
        - hmlr_excluded_proprietors_count
    )

    # Getting the percentage of HMLR companies that we have in the database.
    matched_roe_percentage = (
        hmlr_matched_roe_count / (hmlr_unique_proprietors_count) * 100
    )

    # writes the statistics to a txt file
    with open(f"./outputs/{date_today}-ROE-HMLR-Statistics.txt", "w") as txt_file:
        print(
            f"The number of unique hmlr proprietors on the list is: {hmlr_unique_proprietors_count}.",
            file=txt_file,
            end="\n",
        )
        print(
            f"The number of hmlr proprietors matched in ROE is: {hmlr_matched_roe_count}.",
            file=txt_file,
            end="\n",
        )
        print(
            f"The number of hmlr proprietors excluded is: {hmlr_excluded_proprietors_count}",
            file=txt_file,
            end="\n",
        )
        print(
            f"The number of hmlr proprietors not matched or excluded in ROE is: {hmlr_unmatched_roe_count}.",
            file=txt_file,
            end="\n",
        )
        print(
            f"The proportion of proprietors on the ROE register is: {matched_roe_percentage:.2f}%.",
            file=txt_file,
            end="\n",
        )
        print(
            f"The number of overseas entities on the ROE register is: {len(roe_df)}",
            file=txt_file,
            end="\n",
        )


if __name__ == "__main__":
    main()

# %%
