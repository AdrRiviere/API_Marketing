import logging

import pandas as pd
import requests
from prefect import task


@task(name="[APPLE SEARCH] Get all campaigns ID")
def get_all_campaign_apple_search(access_token):
    """
    Get all campaigns id from Apple Search.

    Args:
        access_token (str): Access Token provided by previous step.

    Returns:
        access_token (str): Valid Access Token for Apple Ads.
    """

    url = "https://api.searchads.apple.com/api/v4/campaigns"
    headers = {
        "Authorization": "Bearer " + access_token,
        "X-AP-Context": "orgId=3160990",
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        campaigns_data = response.json()
        master_table_df = pd.DataFrame()
        no_campaign_global_info = len(campaigns_data["data"])
        for campaign in range(0, no_campaign_global_info):
            unique_result = campaigns_data["data"][campaign]
            tmp_dict_ = {
                "CampaignId": unique_result["id"],
                "CampaignName": unique_result["name"],
                "startTime": unique_result["startTime"],
                "endTime": unique_result["endTime"],
                "CampaignStatus": unique_result["status"],
            }

            tmp_df_ = pd.DataFrame.from_dict(tmp_dict_, orient="index").transpose()
            master_table_df = pd.concat([master_table_df, tmp_df_], axis=0).reset_index(drop=True)

        master_table_df["startTime"] = pd.to_datetime(master_table_df["startTime"]).dt.date

        return master_table_df
    else:
        logging.info("[APPLE SEARCH] Get all campaigns ID - Failed")
