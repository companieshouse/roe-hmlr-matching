In order to be able to run this script you need to set up the the correct folder structure and a config file for database access. You will also need a copy of the latest HMLR extract and exclusion list.

The pyproject.toml and roe-hmlr-matching.py will be place in the effective root folder and you will need to create inputs and outputs subfolders within the root folder. You will also need to create a config.json file to store your login details in order to access the database and extract the ROE data from CHIPS.

The schema for the config file is shown below

{
    "host": "host-name",
    "port": 0000,
    "sid": "sid",
    "user": "user-name",
    "password": "password"
}

In the inputs folder you will create 2 sub-folders called hmlr-data and exclusions. 

In the hmlr-data folder you will need to save the HMLR extract(s) to be used in the script. The naming convention for the files is RXN_DD_MMM_YYYY.xlsx, if they are not in this format the script will ignore the files when searching for the latest HMLR extract.

In the exclusions sub-folder you will need to save the exclusions list(s) to be used by the script. The naming convention for the files is YYYY-MM-DD-exclusions.xlsx, if they are not in this format the script will ignore the files when searching for the latest exclusions list.

The 2 unmatched lists will be saved into the outputs folders in the format YYYY-MM-DD-ROE-unmatched.xlsx and YYYY-MM-DD-HMLR-unmatched.xlsx and will each contain a list of the unmatched entities from either the ROE or HMLR datasets.

The project.toml file contains a list of the dependencies used by the script and these will need to be installed in the virtual environment in order to run the script.