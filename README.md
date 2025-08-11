
# Google Sheet Report Automation

This script automates the process of pulling data from Google BigQuery, processing it with Pandas, and exporting the results to Google Sheets.

## Overview

- Input:  
  Reads a list of merchant IDs (MID) from a specified Google Sheet (Sheet15).

- Processing:  
  Runs multiple SQL queries against BigQuery to fetch:
  - Merchant details
  - Voucher listings
  - Transaction counts and values for different date ranges
  - Cancellation stats
  - Wallet redemption data

  Merges all query outputs into a single DataFrame.

- Output:  
  The final dataset is written back to a target Google Sheet (currently commented out in the script).

## Key Features

- Connects securely to BigQuery using a service account.
- Reads and writes Google Sheets data using the gspread API.
- Cleans, filters, and merges multiple datasets.
- Prepares a consolidated report for merchant performance monitoring.

## Technologies Used

- Python  
- Pandas  
- Google BigQuery (via pandas-gbq and google-auth)  
- Google Sheets API (gspread)  
