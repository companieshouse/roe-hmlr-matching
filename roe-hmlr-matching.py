# -*- coding: utf-8 -*-
"""
Rework of HMLR Script to use monthly Land Registry Extracts

Original script called helpers written by Omolara Ajayi and James Gough

Created on Tue Sep  3 10:00:29 2024
@author: wburkett
"""
# %%
import os
import json
import cx_Oracle
import pandas as pd
import re
from cleanco.clean import basename
from sqlalchemy import create_engine
from datetime import datetime


def get_roe_data(config_file_path: str) -> pd.DataFrame:
    """
    Connects to the oracle database and returns the ROE data
    """
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"Config file {config_file_path} not found.")

    with open(config_file_path, "r") as f:
        config = json.load(f)

    dsn_tns = cx_Oracle.makedsn(config["host"], config["port"], config["sid"])
    conn = f'oracle+cx_oracle://{config["user"]}:{config["password"]}@{dsn_tns}'

    engine = create_engine(conn, pool_recycle=10, pool_size=50, echo=False)
    # Runs a query on the CHIPS database to extract the full list of ROE companies on the register
    # (corporate_body_type_id = 37 is for ROE companies); (action_code_type_id < 9000 is for live companies)
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
    company_name = re.sub(
        r"\ss\w\srl$", "", company_name
    )  # This is to remove SRL suffix in particular.
    return company_name


def get_roe_cleaned(config_file_path: str) -> pd.DataFrame:
    """
    Applies clean_company_name to the corporate_body_name column. Returns a cleaned dataframe of the ROE data queried from the database.
    """
    df = get_roe_data(config_file_path)
    df["cleaned_company_name"] = (
        df["corporate_body_name"].astype(str).apply(clean_company_name)
    )
    return df


def clean_column_names(df):
    """
    Replaces
    """
    df.columns = [c.replace(" ", "_").replace("?", "") for c in df.columns]


def split_alternate_proprietors(df):
    """
    Converts a wide list of proprietors into a longer list with each proprietor recorded on their own row.
    """
    all_dfs = []
    for i in range(1, 5):
        # checks whether PROPRIETOR_NAME_{i} is not blank
        df_address_2 = df[df[f"PROPRIETOR_NAME_{i}"] != ""]
        # checks whether PROPRIETOR_NAME_{i} isna
        df_address_2 = df_address_2[~pd.isna(df_address_2[f"PROPRIETOR_NAME_{i}"])]
        # TODO: check whether removing this line makes a difference
        df_address_2 = df_address_2.rename(
            columns={
                f"COUNTRY_INCORPORATED_{i}": "COUNTRY_INCORPORATED",
                f"PROPRIETOR_NAME_{i}": "PROPRIETOR_NAME",
                f"PROPRIETOR_{i}_ADDRESS_1": "PROPRIETOR_ADDRESS_1",
                f"PROPRIETOR_{i}_ADDRESS_2": "PROPRIETOR_ADDRESS_2",
                f"PROPRIETOR_{i}_ADDRESS_3": "PROPRIETOR_ADDRESS_3",
            }
        )

        df_address_2 = df_address_2[
            [
                "TITLE_NUMBER",
                "TENURE",
                "DATE_PROPERTY_WAS_PURCHASED",
                "COUNTRY_INCORPORATED",
                "PROPRIETOR_NAME",
                "PROPRIETOR_ADDRESS_1",
                "PROPRIETOR_ADDRESS_2",
                "PROPRIETOR_ADDRESS_3",
            ]
        ]
        all_dfs.append(df_address_2)
    return pd.concat(all_dfs, ignore_index=True)


# References OneDrive locations
def import_spreadsheets(path: str) -> pd.DataFrame:
    files = os.listdir(path)
    paths = [os.path.join(path, file) for file in files]
    latest_file = max(paths, key=os.path.getctime)
    latest_hmlr_df = pd.read_excel(latest_file)
    clean_column_names(latest_hmlr_df)
    return latest_hmlr_df


def get_hmlr_data(file_path: str, dedupe: bool = True) -> pd.DataFrame:
    """
    Returns a dataframe with the deduplicated proprietor name and address
    """
    base_df = import_spreadsheets(file_path)
    base_df = split_alternate_proprietors(base_df)
    base_df["cleaned_proprietor_name"] = (
        base_df["PROPRIETOR_NAME"].astype(str).apply(clean_company_name)
    )
    base_df["cleaned_proprietor_address"] = (
        base_df["PROPRIETOR_ADDRESS_1"].astype(str).apply(clean_company_name)
    )

    if dedupe:
        df_final = base_df.drop_duplicates(
            subset=["cleaned_proprietor_name", "cleaned_proprietor_address"],
            keep="first",
        )

    else:
        df_final = base_df

    return df_final


# %%
def full_hmlr_roe_processing_pipeline():
    """
    This function combines the functions above to run the entire pipeline. All that is needed is the hmlr file's path
    location and a SQLAlchemy database connection object to retrieve the data. (see
    data_engineering.db_connection.db_connect.py for how to
    establish a DB connection securely).
    Two files with the unmatched HMLR and ROE companies will be output in the files subfolder of this script.
    """

    # Getting data as a dictionary with the company name processed for both our ROE data and the HMLR dataset.
    hmlr_df = get_hmlr_data("./inputs", dedupe=True)
    roe_df = get_roe_cleaned("config.json")

    # hmlr_df_distinct_proprietors = hmlr_df.drop_duplicates(
    #     subset=["cleaned_proprietor_name"],
    #     keep="first",
    # )

    # Save the unmatched HMLR holdings
    hmlr_unmatched_in_roe_df = hmlr_df[
        ~hmlr_df["cleaned_proprietor_name"].isin(roe_df["cleaned_company_name"])
    ]
    hmlr_unmatched_in_roe_df = hmlr_unmatched_in_roe_df.sort_values(
        by=["cleaned_proprietor_name"]
    )
    date_today = datetime.today().strftime("%Y-%m-%d")
    hmlr_unmatched_in_roe_df.to_excel(
        f"./outputs/{date_today}-HMLR-unmatched.xlsx", index=False
    )

    # Save the unmatched ROE Companies
    roe_unmatched_in_hmlr_df = roe_df[
        ~roe_df["cleaned_company_name"].isin(hmlr_df["cleaned_proprietor_name"])
    ]
    roe_unmatched_in_hmlr_df = roe_unmatched_in_hmlr_df.sort_values(
        by=["cleaned_company_name"]
    )
    date_today = datetime.today().strftime("%Y-%m-%d")
    roe_unmatched_in_hmlr_df.to_excel(
        f"./outputs/{date_today}-ROE-unmatched.xlsx", index=False
    )

    hmlr_df_unique_proprietors = hmlr_df.drop_duplicates(
        subset=["cleaned_proprietor_name"],
        keep="first",
    )

    # Transforming to a set to get a count of the _unique_ company names in the HMLR dataset.
    hmlr_unique_proprietors_count = len(
        hmlr_df_unique_proprietors["cleaned_proprietor_name"]
    )

    # Getting the count for how many unique hmlr companies we have in ans not in our ROE database.
    hmlr_unmatched_roe_count = len(
        hmlr_unmatched_in_roe_df["cleaned_proprietor_name"].unique()
    )
    hmlr_matched_roe_count = hmlr_unique_proprietors_count - hmlr_unmatched_roe_count

    # Getting the percentage of HMLR companies that we have in the database.
    matched_roe_percentage = (
        hmlr_matched_roe_count / (hmlr_unique_proprietors_count) * 100
    )

    # Formatting these stats into easily digestible sentences.
    # =============================================================================
    print(
        f"The number of unique hmlr proprietors on the list is: {hmlr_unique_proprietors_count}."
    )
    print(
        f"The number of hmlr proprietors matched in ROE is: {hmlr_matched_roe_count}."
    )
    print(
        f"The number of hmlr proprietors not matched in ROE is: {hmlr_unmatched_roe_count}."
    )
    print(
        f"The proportion of proprietors on the ROE register is: {matched_roe_percentage:.2f}%."
    )
    print(f"The number of overseas entities on the ROE register is: {len(roe_df)}")
    # =============================================================================


# Runs the functions above to produce the output
full_hmlr_roe_processing_pipeline()
