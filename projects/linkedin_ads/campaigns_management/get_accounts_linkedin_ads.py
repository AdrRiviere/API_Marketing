import json
import time

import pandas as pd
import requests
from prefect import task


@task(name="[LINKEDIN ADS] Get accounts")
def get_accounts_linkedin_ads(access_token):
    """
    Get all accounts LinkedIn Ads available.

    Args:
        access_token (str): Access Token provided by previous step.

    Returns:
        df (DataFrame): DataFrame with all accounts available in Google Ads.
    """
    time.sleep(60)
    url = "https://api.linkedin.com/v2/adCampaignGroupsV2?q=search"
    headers = {"Authorization": "Bearer " + access_token}
    response = requests.get(url=url, headers=headers)

    campaign_group_linkedin_df = pd.DataFrame()
    if response.status_code == 200:
        for group_campaign in json.loads(response.text)["elements"]:

            # Get CampaignGroupName / CampaignGroupId / StatusGroup
            campaign_group_linkedin_dict = {
                "CampaignGroupName": group_campaign.get("name", ""),
                "CampaignGroupId": group_campaign.get("id", ""),
                "CampaignGroupStatus": group_campaign.get("status", ""),
            }
            tmp_df_ = pd.DataFrame.from_dict(
                campaign_group_linkedin_dict, orient="index"
            ).transpose()
            campaign_group_linkedin_df = pd.concat(
                [campaign_group_linkedin_df, tmp_df_], axis=0
            ).reset_index(drop=True)

    return campaign_group_linkedin_df
