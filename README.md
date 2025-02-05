roe-hmlr-matching.py
------------------------------------------------------------------------------------------------

In order to be able to run this script you need to use the the correct folder structure found in the git repository and a config file for database access. You will also need a copy of the latest HMLR extract and exclusion list.

The pyproject.toml and roe-hmlr-matching.py will be placed in the effective root folder and you will need the inputs and outputs subfolders within the root folder. You will also need to create a config.json file to store your login details in order to access the database and extract the ROE data from CHIPS.

The schema for the config file is shown below
```json
{
    "host": "host-name",
    "port": 0000,
    "sid": "sid",
    "user": "user-name",
    "password": "password"
}
```
In the inputs folder you will have 2 sub-folders called hmlr-data and exclusions. 

In the hmlr-data folder you will need to save the HMLR extract(s) to be used in the script. The naming convention for the files is RXN_DD_MMM_YYYY.xlsx, if they are not in this format the script will ignore the files when searching for the latest HMLR extract.

In the exclusions sub-folder you will need to save the exclusions list(s) to be used by the script. The naming convention for the files is YYYY-MM-DD-exclusions.xlsx, if they are not in this format the script will ignore the files when searching for the latest exclusions list.

The 2 unmatched lists will be saved into the outputs folders in the format YYYY-MM-DD-ROE-unmatched.xlsx and YYYY-MM-DD-HMLR-unmatched.xlsx and will each contain a list of the unmatched entities from either the ROE or HMLR datasets. The script will also add 2 columns which shows the closest match in either the ROE or HMLR datasets and the accuracy ratio so that we can identify those that have been misspelt or worded slightly differently.

The project.toml file contains a list of the dependencies used by the script and these will need to be installed in the virtual environment in order to run the script.

NOTE:
This script uses exact matching so there may be some companies that are not matched due to spelling or slight variations in the naming. If we were to use fuzzy matching, we could incorrectly match companies that should not be.

------------------------------------------------------------------------------------------------


hmlr-disposals-profiling.py
------------------------------------------------------------------------------------------------

In order to be able to run this script you need to use the the correct folder structure found in the git repository.

The pyproject.toml and hmlr-disposals-profiling.py will be placed in the effective root folder and you will need the inputs/hmlr-data and outputs subfolders within the root folder. (You do not need the inputs/exclusions subfolder or a config file as this script does not require them)

In the hmlr-data folder you will need to save the HMLR extracts to be used in the script. The script requires at least 2 extracts to function, however it can compare any number of additional extracts. The naming convention for the files is RXN_DD_MMM_YYYY.xlsx, if they are not in this format the script will ignore the files when searching for the latest HMLR extract.

The disposals lists will be saved into the outputs folders in the format YYYY-MM-DD-HMLR-profile.xlsx and will contain a list of the entities that appeared on any of the previous HMLR extracts but that did not appear on the most recent. The script will add 2 which shows the date of the first and last extracts that the entities appears on and filters out any that still appear on the most recent list.

The project.toml file contains a list of the dependencies used by the script and these will need to be installed in the virtual environment in order to run the script.