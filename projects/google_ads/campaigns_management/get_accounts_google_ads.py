import logging

import pandas as pd
import requests
from prefect import task


@task(name="[GOOGLE ADS] Get accounts")
def get_accounts_google_ads(mcc_id, access_token, developer_token):
    """
    Get all accounts Google Ads available.

    Args:
        mcc_id (str): MCC ID provided by Google Ads.
        access_token (str): Access Token provided by previous step.
        developer_token (str): Developer Token provided by Google Ads Interface.

    Returns:
        df (DataFrame): DataFrame with all accounts available in Google Ads.
    """

    customers = "customers/" + mcc_id
    url = "https://googleads.googleapis.com/v14/" + customers + "/googleAds:search"

    headers = {
        "Authorization": "Bearer " + access_token,
        "developer-token": developer_token,
        "Content-Type": "application/json",
    }

    response = requests.post(
        url=url,
        headers=headers,
        json={
            "query": """SELECT customer.descriptive_name,
                               customer_client.client_customer,
                               customer_client.descriptive_name,
                               customer_client.currency_code
                               FROM customer_client"""
        },
    )
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data["results"])
        return df
    else:
        logging.info("Process Error")
