# Automated Cost Uploader

## Background
The following project was designed to automate a manual process carried out on a daily basis, which entailed the following:
  - Review spend and performance data within two marketing channel platforms (Criteo & Kelkoo)
  - Copy tables from each website's reporting dashboard page to a Google sheet
  - Add recent currency exchange data to a reference tab to calculate relevant billing cost conversions
  - Remove unecessary columns, and manually populate new columns ('engine', 'majormarket', 'channel')
  - Reformat data and column titles to match table requirements for upload
  - Join all tables together
  - Copy and paste to a second Google sheet for upload to database platform

## What The Script Does
  - Queries analytics APIs for both marketing platforms:
    - Generates marketing performance data reports for the previous x days - segmented by market, date and device
    - Ensures the correct reporting timezone is selected based on current UK timezone
  - Criteo:
    - Generates separate CSV reports for GBP/EUR/USD data 
    - Uses pandas to reorganise the data, including creation of billing and local cost columns
    - Merges the dataframes together, then populates relevant columns with missing data
    - Renames columns and cleans data attributes
    - Groups columns to remove duplicate/null values
  - Kelkoo:
    - Fixer API call to generate current GBP > USD FX rate
    - Pulls default JSON report of marketing data
    - Uses pandas to remove unecessary columns
    - Groups by shared data points to remove duplicate rows
    - Uses FX rate to populate USD column
    - Cleans titles/data 
  - Combines dataframes together
  - Prepares the final Google sheet used for upload:
    - Opens the target sheet and clears contents of relevant rows
  - Pushes final combined dataframe to sheet
