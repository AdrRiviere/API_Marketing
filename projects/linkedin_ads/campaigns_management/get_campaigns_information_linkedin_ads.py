import json
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from prefect import task


@task(name="[LINKEDIN ADS] Get campaigns information")
def get_campaign_information_linkedin_ads(access_token):
    """
    Get campaigns information from LinkedIn Ads.

    Args:
        access_token (str): Access Token provided by previous step.

    Returns:
        linkedin_df (DataFrame): DataFrame with all campaigns on LinkedIn Ads.
    """

    url = "https://api.linkedin.com/v2/adCampaignsV2?q=search"
    headers = {"Authorization": "Bearer " + access_token}
    response = requests.get(url=url, headers=headers)
    linkedin_df = pd.DataFrame()
    if response.status_code == 200:
        for campaign in json.loads(response.text)["elements"]:

            campaign_linkedin_dict = {
                "CampaignName": campaign.get("name", np.nan),
                "CampaignId": campaign.get("id", np.nan),
                "CampaignGroupId": campaign.get("campaignGroup", np.nan),
                "CampaignStatus": campaign.get("status", np.nan),
                "DateStart": campaign["runSchedule"].get("start", np.nan),
                "DateEnd": campaign["runSchedule"].get("end", np.nan),
                "OptimizationTargetType": campaign.get("optimizationTargetType", np.nan),
                "Language": campaign["locale"].get("language", np.nan),
            }
            tmp_df_ = pd.DataFrame.from_dict(campaign_linkedin_dict, orient="index").transpose()
            linkedin_df = pd.concat([linkedin_df, tmp_df_], axis=0).reset_index(drop=True)

    if len(linkedin_df) > 0:
        linkedin_df["DateStart"] = linkedin_df["DateStart"].apply(
            lambda x: datetime.fromtimestamp(x / 1000).date()
        )
        linkedin_df["DateEnd"] = linkedin_df["DateEnd"].apply(
            lambda x: datetime.fromtimestamp(x / 1000).date()
            if not np.isnan(x)
            else datetime.today().date()
        )
        linkedin_df["CampaignGroupId"] = linkedin_df["CampaignGroupId"].apply(
            lambda x: x.replace("urn:li:sponsoredCampaignGroup:", "")
        )
    return linkedin_df
