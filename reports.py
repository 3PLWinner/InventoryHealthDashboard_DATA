import requests
from dotenv import set_key, load_dotenv
import os
import pandas as pd
import numpy as np
import urllib
import time
import logging
import sys
from datetime import datetime
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.auth.authentication_context import AuthenticationContext
from pathlib import Path

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 0)
pd.set_option("display.max_colwidth", None)

CSV_FOLDER = os.path.join(os.getcwd(), "csvs")
ARCHIVE_FOLDER = os.path.join(os.getcwd(), "archive")
OUTPUT_FOLDER = os.path.join(os.getcwd(), f"output_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(CSV_FOLDER, exist_ok=True)
os.makedirs(ARCHIVE_FOLDER, exist_ok=True)

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SYSTEM_ID = os.getenv("SYSTEM_ID")
TOKEN = os.getenv("W_TOKEN")
SHAREPOINT_URL = os.getenv("SHAREPOINT_URL")
SHAREPOINT_USERNAME=os.getenv("SHAREPOINT_USERNAME")
SHAREPOINT_FOLDER=os.getenv("SHAREPOINT_FOLDER")
SHAREPOINT_CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SHAREPOINT_CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")
SHAREPOINT_PASSWORD=os.getenv("SHAREPOINT_PASSWORD")

#Set up logging
def setup_logging():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Create file handler with UTF-8 encoding
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)
    
    # Create console handler with UTF-8 encoding for Windows
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Configure the root logger
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger

logger = setup_logging()
    
# Just add these debug lines to your existing archive_existing_csvs function
def archive_existing_csvs(ctx, relative_folder_url, archive_folder_url):
    try:
        # Add this debug logging at the start
        logger.info(f"ARCHIVE DEBUG: Starting archive process")
        logger.info(f"ARCHIVE DEBUG: Source folder: {relative_folder_url}")
        logger.info(f"ARCHIVE DEBUG: Archive folder: {archive_folder_url}")
        
        # Load the target folder
        target_folder = ctx.web.get_folder_by_server_relative_url(relative_folder_url)
        ctx.load(target_folder.files)
        ctx.execute_query()
        
        # Add this debug logging
        all_files = [f.properties["Name"] for f in target_folder.files]
        logger.info(f"ARCHIVE DEBUG: All files found: {all_files}")
        
        csv_files = [f for f in target_folder.files if f.properties["Name"].lower().endswith(".csv")]
        csv_names = [f.properties["Name"] for f in csv_files]
        logger.info(f"ARCHIVE DEBUG: CSV files to archive: {csv_names}")
        
        if not csv_files:
            logger.info(f"No CSV files to archive in folder: {relative_folder_url}")
            return True
        
        # Check if archive folder exists, create if not
        try:
            archive_folder = ctx.web.get_folder_by_server_relative_url(archive_folder_url)
            ctx.load(archive_folder)
            ctx.execute_query()
            logger.info(f"ARCHIVE DEBUG: Archive folder exists")
        except Exception:
            logger.info(f"ARCHIVE DEBUG: Archive folder not found, creating: {archive_folder_url}")
            target_folder.folders.add(archive_folder_url.split('/')[-1])  # Create last part of path
            ctx.execute_query()
            archive_folder = ctx.web.get_folder_by_server_relative_url(archive_folder_url)

        date_str = datetime.now().strftime("%Y%m%d")
        
        # Add counter for debugging
        archived_count = 0
        for f in csv_files:
            try:
                original_name = f.properties["Name"]
                archive_name = f"{Path(original_name).stem}_{date_str}{Path(original_name).suffix}"
                archive_path = f"{archive_folder_url}/{archive_name}"
                
                logger.info(f"ARCHIVE DEBUG: Archiving {original_name} to {archive_path}")
                f.move_to(archive_path, 1)  # overwrite if exists
                ctx.execute_query()
                archived_count += 1
                logger.info(f"ARCHIVE DEBUG: Successfully archived {original_name}")
            except Exception as file_error:
                logger.error(f"ARCHIVE DEBUG: Failed to archive {original_name}: {file_error}")
        
        logger.info(f"ARCHIVE DEBUG: Archived {archived_count} of {len(csv_files)} files to {archive_folder_url}")
        return True

    except Exception as e:
        logger.error(f"ARCHIVE DEBUG: Error archiving CSV files: {e}")
        return False



# Enhanced upload function with better SharePoint path handling
def upload_to_sharepoint(local_file_path, sharepoint_filename):
    try:
        logger.info(f"Starting SharePoint upload process...")
        logger.info(f"Local file: {local_file_path}")
        logger.info(f"SharePoint filename: {sharepoint_filename}")
        logger.info(f"SharePoint URL: {SHAREPOINT_URL}")
        logger.info(f"SharePoint folder: {SHAREPOINT_FOLDER}")
        
        # Connect to SharePoint
        ctx_auth = AuthenticationContext(SHAREPOINT_URL)
        if not ctx_auth.acquire_token_for_user(SHAREPOINT_USERNAME, SHAREPOINT_PASSWORD):
            logger.error("Authentication failed for SharePoint")
            return False
        
        ctx = ClientContext(SHAREPOINT_URL, ctx_auth)
        logger.info("SharePoint authentication successful")
        
        # Ensure SharePoint folder path is properly formatted
        # Remove leading/trailing slashes and ensure proper format
        clean_folder_path = SHAREPOINT_FOLDER.strip('/')
        if not clean_folder_path.startswith('/'):
            clean_folder_path = '/' + clean_folder_path
            
        archive_folder_path = f"{clean_folder_path}/Archive"
        
        logger.info(f"Clean folder path: {clean_folder_path}")
        logger.info(f"Archive folder path: {archive_folder_path}")
        
        # Archive existing CSV files before uploading new ones
        logger.info("Starting archiving process...")
        archive_success = archive_existing_csvs(ctx, clean_folder_path, archive_folder_path)
        
        if archive_success:
            logger.info("Archiving completed successfully")
        else:
            logger.warning("Archiving had issues, but continuing with upload...")
        
        # Get target folder for upload
        target_folder = ctx.web.get_folder_by_server_relative_url(clean_folder_path)
        ctx.load(target_folder)
        ctx.execute_query()
        
        # Upload the new file
        logger.info(f"Uploading file: {sharepoint_filename}")
        with open(local_file_path, "rb") as content_file:
            file_content = content_file.read()
            target_folder.upload_file(sharepoint_filename, file_content)
            ctx.execute_query()

        logger.info(f"Successfully uploaded: {sharepoint_filename}")
        logger.info(f"SharePoint URL: {SHAREPOINT_URL}{clean_folder_path}/{sharepoint_filename}")
        return True

    except Exception as e:
        logger.error(f"Error uploading to SharePoint: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False
    
# Get authorization token from VeraCore API
def get_token():
    logger.info("Attempting to get authorization token from VeraCore API")
    logger.info(f"USERNAME: {'SET' if USERNAME else 'NOT SET'}")
    logger.info(f"PASSWORD: {'SET' if PASSWORD else 'NOT SET'}")  
    logger.info(f"SYSTEM_ID: {'SET' if SYSTEM_ID else 'NOT SET'}")
    logger.info(f"W_TOKEN: {'SET' if TOKEN else 'NOT SET'}")
    if USERNAME:
        logger.info(f"USERNAME value: {USERNAME}")
    if SYSTEM_ID:
        logger.info(f"SYSTEM_ID value: {SYSTEM_ID}")
    if TOKEN:
        logger.info("Attempting direct token authentication...")
        auth_header = {"Authorization": f"bearer {TOKEN}"}

    # Test the token with a simple API call

    test_url = "https://wms.3plwinner.com/VeraCore/Public.Api/api/reports"

    try:
        logger.info(f"Testing direct token against: {test_url}")
        test_response = requests.get(test_url, headers=auth_header, timeout=30)
        logger.info(f"Direct token test - Status Code: {test_response.status_code}")
        logger.info(f"Direct token test - Response Headers: {dict(test_response.headers)}")
            
        if test_response.status_code == 200:
            logger.info("✓ Direct token authentication successful!")
            return auth_header
        else:
            logger.warning(f"✗ Direct token failed - Response: {test_response.text[:500]}")
    except Exception as e:
        logger.warning(f"✗ Direct token test error: {str(e)}")




    endpoint = 'https://wms.3plwinner.com/VeraCore/Public.Api/api/Login'

    body = {
        "userName" : USERNAME,
        "password" : PASSWORD,
        "systemId" : SYSTEM_ID
    }
    try:
        response = requests.post(endpoint, data=body, timeout=30)
        if response.status_code != 200:
            logger.error("Login Failed:", response.status_code, response.text)
            return None
        
        token = response.json()["Token"]

        auth_header = {
            "Authorization" : "bearer "+ token
        }

        logger.info("Authentication Successful.")
        return auth_header
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        return None


def start_report_task(report_name, filters, auth_header):
    url = "https://wms.3plwinner.com/VeraCore/Public.Api/api/reports"

    payload = {
        "reportName": report_name,
        "filters": filters
    }
    try:
        response = requests.post(url, json=payload, headers=auth_header, timeout=30)
        if response.status_code == 200:
            response_data = response.json()
            task_id = response_data["TaskId"]
            logger.info("Task started. Task ID: %s", task_id)
            return task_id
        else:
            logger.error("Error starting report task: %s %s", response.status_code, response.text)
            return None
    except Exception as e:
        logger.error("Exception starting report task: %s", str(e))
        return None
    

def run_report_task(report_name, filters, auth_header, output_csv_name):
    logger.info(f"Processing report: {report_name}")
    task_id = start_report_task(report_name, filters, auth_header)
    if not task_id:
        print("Failed to start report task.")
        return False
    
    status_url = f"https://wms.3plwinner.com/VeraCore/Public.Api/api/reports/{task_id}/status"
    max_attempts = 20
    for attempt in range(max_attempts):
        try:
            status_response = requests.get(status_url, headers=auth_header, timeout=30)
            if status_response.status_code == 200:
                status = status_response.json().get("Status")
                if status == "Done":
                    logger.info(f"Report Completed")
                    break
                elif status == "Request too Large":
                    logger.error("Report Request too large: %s %s", status_response.status_code, status_response.text)
                    return False
                else:
                    if attempt % 5 == 0:
                        logger.info(f"Report status: {status} (attempt {attempt + 1})")
                    time.sleep(3)
            else:
                logger.error(f"Status Check Failed: {status_response.status_code} {status_response.text}")
                return False
                time.sleep(3)
        except Exception as e:
            logger.error(f"Exception checking report status: {str(e)}")
            return False
    else:
        logger.error("Report timeout - did not complete within 60 seconds")
        return False
    
    try:
        report_url = f"https://wms.3plwinner.com/VeraCore/Public.Api/api/reports/{task_id}"
        report_response = requests.get(report_url, headers=auth_header, timeout=60)
        if report_response.status_code == 200:
            report_data = report_response.json()["Data"]
            df = pd.DataFrame(report_data)
            output_path = os.path.join(OUTPUT_FOLDER, output_csv_name)
            df.to_csv(output_path, index=False)
            logger.info(f"Report data saved to {output_csv_name}")
            basename = Path(output_csv_name).stem
            timestamped_filename = f"{basename}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"


            upload_success = upload_to_sharepoint(output_path, timestamped_filename)
            if os.path.exists(output_csv_name):
                os.remove(output_csv_name)
                logger.info(f"Cleaned up local file")
            if upload_success:
                logger.info(f"Successfully uploaded {output_csv_name} to SharePoint")
                return True
            else:
                logger.error(f"Failed to upload {output_csv_name} to SharePoint")
                return False

        else:
            logger.error("Error retrieving report data: %s %s", report_response.status_code, report_response.text)
            return False

    except Exception as e:
        logger.error(f"Exception getting report data: {str(e)}")
        return False

# Get data from APi endpoint
def get_dataframe_from_api(endpoint, auth_header, name):
    try:
        logger.info(f"Fetching data from API endpoint: {endpoint}")
        response = requests.get(endpoint, headers=auth_header)

        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                df = pd.DataFrame(data)
                filename = f"{name}.csv"
                output_path = os.path.join(OUTPUT_FOLDER, filename)
                df.to_csv(output_path, index=False)

                if name == "available_reports_endpoint":
                    logger.info(f"Skipping SharePoint upload for {filename}")
                    if os.path.exists(output_path):
                        os.remove(output_path)
                        logger.info(f"Deleted local file: {output_path}")
                    return True
                
                upload_success = upload_to_sharepoint(output_path, filename)
                if os.path.exists(filename):
                    os.remove(filename)
                if upload_success:
                    logger.info(f"Successfully uploaded {filename} to SharePoint")
                    return True
                else:
                    logger.error(f"Failed to upload {filename} to SharePoint")
                    return False
            else:
                logger.error(f"Unexpected data format: {name}")
                return False
        else:
            logger.error(f"API Error for {name}: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Exception fetching data from {name}: {str(e)}")
        return False


def main():
    logger.info("=" * 50)
    logger.info("Starting Veracore Data Pipeline")
    logger.info(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)

    required_vars = {
        "USERNAME": USERNAME,
        "PASSWORD": PASSWORD,
        "SYSTEM_ID": SYSTEM_ID,
        "SharePoint Client ID": SHAREPOINT_CLIENT_ID,
        "SharePoint Client Secret": SHAREPOINT_CLIENT_SECRET,
        "W_TOKEN": TOKEN
    }
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        logger.error("Make sure all GitHub Secrets are properly configured.")
        return False
    logger.info("All required environment variables are set.")


    auth_header = get_token()
    if auth_header:
        print("Authorization header obtained successfully.")
    else:
        logger.error("Failed to obtain authorization header.")
        return False
    
    endpoints = {
    "available_reports_endpoint": "https://wms.3plwinner.com/VeraCore/Public.Api/api/reports", # GETS available reports
    }

    for name, url in endpoints.items():
        get_dataframe_from_api(url, auth_header, name)


    # List of reports to run
    reports_to_run = [
        {
            "report_name": "unit-details",
            "filters": [
                {"filterColumnName": "Owner"},
                {"filterColumnName": "Product ID"},
                {
                    "filterColumnName": "Receipt Date",
                    "startDate": "01/01/2025 12:00:00 AM",
                    "endDate": "07/30/2025 11:59:59 PM"
                },
                {"filterColumnName": "Unit"},
                {"filterColumnName": "Location"},
                {"filterColumnName": "On Hand Total"},
            ],
            "output_csv": "unit_details_with_current_balance.csv"
        },

        {
            "report_name": "expected",
            "filters": [],
            "output_csv": "expected_arrivals.csv"
        },
        {
            "report_name": "Unit Billing",
            "filters": [],
            "output_csv": "unit_billing.csv"
        },
        {
            "report_name": "WarehouseLocations",
            "filters": [],
            "output_csv": "warehouse_locations.csv"
        },
        {
            "report_name": "Shipping Report",
            "filters": [],
            "output_csv": "shipping_report.csv"
        },
        {
            "report_name": "Pull Manifest report",
            "filters": [],
            "output_csv": "pull_manifest_report.csv"
        }
    ]

    successful_reports = 0
    total_reports = len(reports_to_run)

    for i, report in enumerate(reports_to_run, 1):
        logger.info(f"Processing report {i}/{total_reports}: {report['report_name']}")
        success = run_report_task(
            report["report_name"],
            report["filters"],
            auth_header,
            report["output_csv"]
        )
        if success:
            successful_reports += 1

    logger.info("=" * 50)
    logger.info(f"Pipeline Summary:")
    logger.info(f"Successful reports: {successful_reports} / {total_reports}")
    logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)
    return successful_reports == total_reports


if __name__ == "__main__":
    try:
        success = main()
        if success:
            logger.info("Pipeline Completed Successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline completed with errors!")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
        sys.exit(1)