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

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 0)
pd.set_option("display.max_colwidth", None)


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SYSTEM_ID = os.getenv("SYSTEM_ID")
TOKEN = os.getenv("W_TOKEN")
SHAREPOINT_CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SHAREPOINT_CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")

#Set up logging for GitHub Actions Environment
def setup_logging():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger

logger = setup_logging()
    

# Uploads a file to SharePoint
def upload_to_sharepoint(local_path, sharepoint_filename):
    logger.info(f"Starting SharePoint upload for {sharepoint_filename} from {local_path}")

    site_url="https://3plwinner.sharepoint.com"
    relative_folder_url="/Shared Documents/InventoryHealthDashboard"

    if not SHAREPOINT_CLIENT_ID or not SHAREPOINT_CLIENT_SECRET:
        logger.error("SharePoint client ID or secret not set in environment variables.")
        return False
    try:
        ctx = ClientContext(site_url).with_credentials(ClientCredential(SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET))
        web = ctx.web.load()
        ctx.execute_query()
        logger.info(f"Connected to SharePoint site: {web.properties.get('Title', 'Unknown')}")
        target_folder = ctx.web.get_folder_by_server_relative_url(relative_folder_url)

        with open(local_path, 'rb') as file_obj:
            file_content = file_obj.read()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_filename = f"{sharepoint_filename}_{timestamp}"
        uploaded_file = target_folder.upload_file(timestamped_filename, file_content)
        ctx.execute_query()
        logger.info(f"Successfully uploaded: {timestamped_filename}")
        logger.info(f"SharePoint URL: {site_url}{relative_folder_url}/{timestamped_filename}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload file to SharePoint: {str(e)}")
        return False
    
# Get authorization token from VeraCore API
def get_token():
    logger.info("Attempting to get authorization token from VeraCore API")
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
            df.to_csv(output_csv_name, index=False)
            logger.info(f"Report data saved to {output_csv_name}")
            upload_success = upload_to_sharepoint(output_csv_name, report_name.replace(" ", "_"))
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
                df.to_csv(filename, index=False)
                upload_success = upload_to_sharepoint(filename, filename)
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
            "report_name": "Unit Details (by owner) with Current Balance",
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
            "report_name": "expected arrivals",
            "filters": [],
            "output_csv": "expected_arrivals.csv"
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
            report["output.csv"],
            auth_header
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