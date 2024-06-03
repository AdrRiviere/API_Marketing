import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from prefect import task


@task(name="[LINKEDIN ADS] Get daily insights by campaign")
def get_all_insights_daily_linkedin_ads(
    campaign_linkedin_df,
    access_token,
    campaign_ingestion_settings,
    start_date_settings,
    end_date_settings,
):
    """
    Get daily insights by campaign on LinkedIn Ads.

    Args:
        campaign_linkedin_df (DataFrame): DataFrame with all campaigns provided at the
        previous step.
        access_token (str): Access Token provided by previous step.
        campaign_ingestion_settings (str): Daily or Custom.
        start_date_settings (str): Start Date to request API Google Ads
        end_date_settings (str): End Date to request API Google Ads

    Returns:
        df (DataFrame): DataFrame with all accounts insights.
    """

    global_kpi_linkedin_df = pd.DataFrame()
    for campaign_id in campaign_linkedin_df["CampaignId"].unique().tolist():
        # Get Date Start and Date End
        if campaign_ingestion_settings == "Daily":
            start_date = datetime.now() - timedelta(days=1)
            end_date = datetime.now()
        else:
            campaign_start_date = campaign_linkedin_df.loc[
                campaign_linkedin_df["CampaignId"] == campaign_id, "DateStart"
            ].values[0]
            campaign_end_date = campaign_linkedin_df.loc[
                campaign_linkedin_df["CampaignId"] == campaign_id, "DateEnd"
            ].values[0]

            if campaign_start_date >= start_date_settings:
                start_date = campaign_start_date
            else:
                start_date = start_date_settings

            if campaign_end_date <= end_date_settings:
                end_date = campaign_end_date
            else:
                end_date = end_date_settings

        # Generate URL :
        url = create_url_linkedin(campaign_id, start_date, end_date)
        # Create Request :
        headers = {"Authorization": "Bearer " + access_token}
        response = requests.get(url=url, headers=headers, timeout=100)

        if response.status_code == 200:
            data = json.loads(response.text)
            result_kpi_marketing_tmp_ = get_insight_daily_campaign(data)
            result_kpi_marketing_tmp_["CampaignId"] = campaign_id

            global_kpi_linkedin_df = pd.concat(
                [global_kpi_linkedin_df, result_kpi_marketing_tmp_], axis=0
            )

    return global_kpi_linkedin_df


def create_url_linkedin(campaign_id, date_start, date_end):
    """
    Create URL to request LinkedIn Ads API.

    Args:
        campaign_id (str): Campaign ID.
        date_start (str): Start date of the campaign.
        date_end (str): End date of the campaign.

    Returns:
        url (str): URL to request API LinkedIn Ads.
    """

    url = "https://api.linkedin.com/v2/adAnalyticsV2?q=analytics"
    url = url + "&pivot=CAMPAIGN&campaigns[0]=urn:li:sponsoredCampaign:"
    url = url + str(campaign_id)
    url = url + "&dateRange.start.day=" + str(date_start.day)
    url = url + "&dateRange.start.month=" + str(date_start.month)
    url = url + "&dateRange.start.year=" + str(date_start.year)
    url = url + "&dateRange.end.day=" + str(date_end.day)
    url = url + "&dateRange.end.month=" + str(date_end.month)
    url = url + "&dateRange.end.year=" + str(date_end.year)
    url = url + "&timeGranularity=DAILY&fields="
    url = url + "dateRange,impressions,likes,clicks,costInLocalCurrency"
    return url


def get_insight_daily_campaign(data):
    """
    Get insights from a LinkedIn Ads campaign.

    Args:
        data (json): Daily insights in Json format.

    Returns:
        kpi_marketing_linkedin_df (DataFrame): Daily insight for one campaign LinkedIn Ads.
    """
    kpi_marketing_linkedin_df = pd.DataFrame()
    for row in data["elements"]:

        generated_date = datetime(
            int(row["dateRange"]["start"].get("year", 1)),
            int(row["dateRange"]["start"].get("month", 1)),
            int(row["dateRange"]["start"].get("day", 1)),
        ).date()

        tmp_df_ = pd.DataFrame.from_dict(
            {
                "Spend": row.get("costInLocalCurrency", np.nan),
                "Impressions": row.get("impressions", np.nan),
                "Clicks": row.get("clicks", np.nan),
                "Likes": row.get("likes", np.nan),
                "Date": generated_date,
            },
            orient="index",
        ).transpose()
        kpi_marketing_linkedin_df = pd.concat(
            [kpi_marketing_linkedin_df, tmp_df_], axis=0
        ).reset_index(drop=True)

    return kpi_marketing_linkedin_df
