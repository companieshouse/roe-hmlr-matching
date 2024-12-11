# -*- coding: utf-8 -*-
"""
Script to compile a list of unique proprietors from the available HMLR extracts.
Using the list of unique proprietors the script will identify the first and last 
extracts that the proprietor was found in 

@author: wburkett
"""


# %%
import os
import re
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np


def clean_datetime(datetime: str):
    """
    Converts the datetime columns into a useable format
    :param datetime:
    """

    # the datetime format found in the HMLR extracts have been inconsistent and
    # makes the conversion into a datetime type fail unless the formatting is corrected

    # some of the timestamps have . between the hours, minutes and seconds
    datetime = re.sub("\.", ":", datetime)  # this line converts it to :
    # some datetimes include an additional : between the date and the time components
    datetime = re.sub(
        ":\d{2}:\d{2}:\d{2}", "", datetime
    )  # this line removes the timestamp

    # the following line removes the timestamp when there is no additional :
    # as some timestamps immediately follow the date without a space
    datetime = re.sub("\d{2}:\d{2}:\d{2}", "", datetime)
    # this function converts the datetime into formats that are recognised by the "mixed" format used later
    return datetime


def get_newest_hmlr_files(folder_path: Path) -> pd.DataFrame:
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

    # combines all HMLR extracts found within the folder into a single dataframe
    combined_hmlr_df = pd.DataFrame()
    for file in files_with_dates:
        combined_hmlr_df = pd.concat(
            [combined_hmlr_df, pd.read_excel(file[0])], ignore_index=True
        )

    # converts the column headers to lower case
    combined_hmlr_df.columns = combined_hmlr_df.columns.str.lower()

    # applies the clean_datetime function to extract_date and then stores just the date component
    combined_hmlr_df["extract_date"] = pd.to_datetime(
        (combined_hmlr_df["extract_date"].astype(str).apply(clean_datetime)),
        format="mixed",
    ).dt.date

    # applies the clean_datetime function to date_proprieter_added_updated and then stores just the date component
    combined_hmlr_df["date_proprieter_added_updated"] = pd.to_datetime(
        (
            combined_hmlr_df["date_proprieter_added_updated"]
            .astype(str)
            .apply(clean_datetime)
        ),
        format="mixed",
    ).dt.date

    # returns the combined dataframe with formatted date columns
    return combined_hmlr_df


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
    # import all HMLR extracts as a combined dataframe
    combined_hmlr_df = get_newest_hmlr_files(folder_path="inputs/hmlr-data")
    # expand the numbered proprietor columns into seperate rows
    combined_hmlr_df = reshape_hmlr_proprietors(combined_hmlr_df)

    # finds the first and last extract that each proprietor is found on
    hmlr_proprietor_profile = combined_hmlr_df.groupby(["proprietor_name"]).agg(
        first_date_on_hmlr_extract=("extract_date", "min"),
        last_date_on_hmlr_extract=("extract_date", "max"),
    )

    # takes todays date to use when saving the output
    date_today = datetime.today().strftime("%Y-%m-%d")

    # saves the list of proprietors to an excel file
    hmlr_proprietor_profile.to_excel(
        f"./outputs/{date_today}-HMLR-profile.xlsx", index=True
    )


if __name__ == "__main__":
    main()

# %%
