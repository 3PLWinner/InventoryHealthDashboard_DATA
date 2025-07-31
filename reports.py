import requests
from dotenv import set_key, load_dotenv
import os
import pandas as pd
import numpy as np
import urllib
import time

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

def get_token():
    endpoint = 'https://wms.3plwinner.com/VeraCore/Public.Api/api/Login'

    body = {
        "userName" : USERNAME,
        "password" : PASSWORD,
        "systemId" : SYSTEM_ID
    }

    response = requests.post(endpoint, data=body)

    if response.status_code != 200:
        print("Login Failed:", response.status_code, response.text)
        return None

    token = response.json()["Token"]

    os.environ["TOKEN"] = token

    set_key(dotenv_path, "W_TOKEN", token)

    auth_header = {
        "Authorization" : "bearer "+ token
    }

    return auth_header

# Check/get authorization token and create header
def check_token(local_token):

    auth_header = None

    if local_token is None:
        return get_token()
    else:
        auth_str = "bearer " + local_token

        check_endpoint = 'https://wms.3plwinner.com/VeraCore/Public.Api/api/token'

        auth_header = {
            "Authorization" : auth_str
        }

        token_response = requests.get(check_endpoint, headers=auth_header)

        if not(token_response.status_code == 200):
            print("Failed to authenticate:", token_response.status_code, token_response.text)
            return get_token()
    
    return auth_header

def start_report_task(report_name, filters, auth_header):
    url = "https://wms.3plwinner.com/VeraCore/Public.Api/api/reports"

    payload = {
        "reportName": report_name,
        "filters": filters
    }
    response = requests.post(url, json=payload, headers=auth_header)
    if response.status_code == 200:
        response_data = response.json()
        task_id = response_data["TaskId"]
        print("Task started. Task ID:", task_id)
        return task_id
    else:
        print("Error starting report task:", response.status_code, response.text)
        return None
    

def run_report_task(report_name, filters, auth_header, output_csv_name):
    print(f"Starting report: {report_name}")
    task_id = start_report_task(report_name, filters, auth_header)
    if not task_id:
        print("Failed to start report task.")
        return None
    
    status_url = f"https://wms.3plwinner.com/VeraCore/Public.Api/api/reports/{task_id}/status"

    for _ in range(10):
        status_response = requests.get(status_url, headers=auth_header)
        if status_response.status_code == 200:
            status = status_response.json()
            if status.get("Status") == "Done":
                break
            elif status.get("Status") == "Request too Large":
                print("Report Request too large:", status_response.status_code, status_response.text)
                return None
            else:
                time.sleep(3)
        else:
            print("Error checking report status:", status_response.status_code, status_response.text)
            return None
        
    else:
        print("Report task did not complete in time.")
        return None
    
    report_url = f"https://wms.3plwinner.com/VeraCore/Public.Api/api/reports/{task_id}"
    report_response = requests.get(report_url, headers=auth_header)
    if report_response.status_code == 200:
        report_data = report_response.json()["Data"]
        df = pd.DataFrame(report_data)
        df.to_csv(output_csv_name, index=False)
        return df
    else:
        print("Error retrieving report data:", report_response.status_code, report_response.text)
        return None




def get_dataframe_from_api(endpoint, auth_header):
    response = requests.get(endpoint, headers=auth_header)

    if response.status_code == 200:
        try:
            data = response.json()
            if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                # Convert list of dictionaries to DataFrame
                df = pd.DataFrame(data)
                return df
            else:
                print("Unexpected data format:", data)
                return None
        except ValueError as e:
            print("Error parsing JSON:", e)
    else:
        print("Error fetching data:", response.status_code, response.text)
        return None


if __name__ == "__main__":
    auth_header = check_token(TOKEN)
    if auth_header:
        print("Authorization header obtained successfully.")
    else:
        print("Failed to obtain authorization header.")
        exit()
    
    endpoints = {
    "available_reports_endpoint": "https://wms.3plwinner.com/VeraCore/Public.Api/api/reports", # GETS available reports
    }

    for name, url in endpoints.items():
        df = get_dataframe_from_api(url, auth_header)
        if df is not None:
            df.to_csv(f"{name}.csv", index=False)
            print(f"Saved {name}.csv successfully.")
        else:
            print(f"Failed to retrieve {name}.")


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
            "report_name": "Warehouse Locations - 2",
            "filters": [],
            "output_csv": "warehouse_locations.csv"
        },
        {
            "report_name": "Shipping Report",
            "filters": [],
            "output_csv": "shipping_report.csv"
        }
    ]

    for report in reports_to_run:
        df = run_report_task(
            report_name=report["report_name"],
            filters=report["filters"],
            auth_header=auth_header,
            output_csv_name=report["output_csv"]
        )

        if df is not None:
            print("DataFrame obtained successfully:")
            print(df.head())
        else:
            print("Failed to obtain DataFrame.")
