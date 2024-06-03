from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from prefect import task


@task(name="[APPLE SEARCH] Get daily statistics by campaign")
def get_full_insights_process_by_campaign(
    access_token,
    campaign_group_apple_search_df,
    campaign_ingestion_settings,
    start_date_settings,
    end_date_settings,
):
    """
    Get all daily insights by campaign from Apple Search.

    Args:
        access_token (str): Access Token provided by previous step.
        campaign_group_apple_search_df (DataFrame): DataFrame with all
            campaigns id provided by previous step.
        campaign_ingestion_settings (str): Daily or Custom.
        start_date_settings (str): Start Date to request API Apple Search.
        end_date_settings (str): End Date to request API Apple Search.

    Returns:
        apple_search_all_campaigns_df (DataFrame): Full DataFrame with all campaigns
        and full insights daily.
    """

    apple_search_all_campaigns_df = pd.DataFrame()
    index = 0
    for campaign_id in campaign_group_apple_search_df["CampaignId"].unique().tolist():
        # Get Start Date and End Date by campaign

        campaign_start_date = campaign_group_apple_search_df.loc[
            campaign_group_apple_search_df["CampaignId"] == campaign_id, "startTime"
        ][index]

        if campaign_ingestion_settings == "Daily":
            start_date = datetime.now() - timedelta(days=1)
            end_date = datetime.now()
        else:
            if campaign_start_date >= start_date_settings:
                start_date = campaign_start_date
                end_date = end_date_settings
            else:
                start_date = start_date_settings
                end_date = end_date_settings

        campaign_name = campaign_group_apple_search_df.loc[
            campaign_group_apple_search_df["CampaignId"] == campaign_id, "CampaignName"
        ][index]

        campaign_status = campaign_group_apple_search_df.loc[
            campaign_group_apple_search_df["CampaignId"] == campaign_id, "CampaignStatus"
        ][index]

        index = index + 1
        # Get insights for all days
        apple_search_one_campaign_all_days_df = get_insights_all_days_by_campaign(
            access_token=access_token,
            campaign_id=campaign_id,
            start_date=start_date,
            end_date=end_date,
        )
        if len(apple_search_one_campaign_all_days_df) > 0:
            apple_search_one_campaign_all_days_df["CampaignName"] = campaign_name
            apple_search_one_campaign_all_days_df["CampaignStatus"] = campaign_status

            apple_search_all_campaigns_df = pd.concat(
                [apple_search_all_campaigns_df, apple_search_one_campaign_all_days_df], axis=0
            )

    apple_search_all_campaigns_df["Installs"] = apple_search_all_campaigns_df["Installs"].astype(
        int
    )
    apple_search_all_campaigns_df["Spend"] = apple_search_all_campaigns_df["Spend"].astype(float)
    apple_search_all_campaigns_df["Installs"] = apple_search_all_campaigns_df["Installs"].fillna(0)
    apple_search_all_campaigns_df["Impressions"] = apple_search_all_campaigns_df[
        "Impressions"
    ].fillna(0)

    apple_search_all_campaigns_df["CTR"] = np.where(
        apple_search_all_campaigns_df["Impressions"] == 0,
        0,
        apple_search_all_campaigns_df["Installs"] / apple_search_all_campaigns_df["Impressions"],
    )
    apple_search_all_campaigns_df["CPC"] = np.where(
        apple_search_all_campaigns_df["Installs"] == 0,
        apple_search_all_campaigns_df["Spend"],
        apple_search_all_campaigns_df["Spend"] / apple_search_all_campaigns_df["Installs"],
    )

    return apple_search_all_campaigns_df


def get_insights_by_campaign(access_token, campaign_id, date_start, date_end):
    """
    Get daily insights by campaign from Apple Search.

    Args:
        access_token (str): Access Token provided by previous step.
        campaign_id (str): Campaign ID.
        date_start (date): Start Date of the campaign.
        date_end (date): End Date of the campaign.

    Returns:
        access_token (str): Valid Access Token for Apple Ads.
    """
    url = f"https://api.searchads.apple.com/api/v4/reports/campaigns/{campaign_id}/adgroups"

    headers = {
        "Authorization": "Bearer " + access_token,
        "X-AP-Context": "orgId=3160990",
    }

    data = {
        "granularity": "DAILY",
        "timeZone": "UTC",
        "startTime": date_start,
        "selector": {
            "orderBy": [{"field": "localSpend", "sortOrder": "ASCENDING"}],
            "pagination": {"offset": 0, "limit": 1000},
        },
        "endTime": date_end,
        "returnRecordsWithNoMetrics": "false",
    }

    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        daily_insights_campaign = response.json()
        return daily_insights_campaign
    else:
        return None


def get_insights_all_days_by_campaign(access_token, campaign_id, start_date, end_date):
    """
    Process to get all days insights by campaign from Apple Search.
    Limit 90 days provided by Apple Search.

    Args:
        access_token (str): Access Token provided by previous step.
        campaign_id (str): Campaign ID.
        start_date (date): Start Date of the campaign.
        end_date (date): End Date of the campaign.

    Returns:
        access_token (str): Valid Access Token for Apple Ads.
    """
    current_date = start_date
    next_date = start_date

    final_table_df = pd.DataFrame()
    while current_date <= end_date:
        # Process Between Two Dates - Limit 90 days provided by Apple Search
        next_date += timedelta(days=3 * 30)
        apple_search_api_result_json = get_insights_by_campaign(
            access_token=access_token,
            campaign_id=campaign_id,
            date_start=str(current_date),
            date_end=str(next_date),
        )
        if apple_search_api_result_json is not None:
            apple_search_df = build_metrics_dataframe(result_json=apple_search_api_result_json)
            final_table_df = pd.concat([final_table_df, apple_search_df], axis=0)

        current_date += timedelta(days=3 * 30)

    if len(final_table_df) > 0:
        final_table_df = final_table_df.drop_duplicates()
        final_table_df = final_table_df[final_table_df["Spend"] > 0]

    return final_table_df


def build_metrics_dataframe(result_json):
    """
    Build final DataFrame with final format for output.

    Args:
        result_json (json): Return from Apple Search API.

    Returns:
        campaign_daily_df (DataFrame): Daily metrics by campaign from Apple Search.
    """
    campaign_daily_df = pd.DataFrame()
    max_adgroup = len(result_json["data"]["reportingDataResponse"]["row"])
    for adgroup in range(0, max_adgroup):
        daily_results = result_json["data"]["reportingDataResponse"]["row"][adgroup]
        for daily_insight in daily_results["granularity"]:
            tmp_dict_ = {
                "Date": daily_insight["date"],
                "CampaignId": daily_results["metadata"]["campaignId"],
                "CampaignGroupName": daily_results["metadata"]["adGroupName"],
                "Impressions": int(daily_insight["impressions"]),
                "Taps": int(daily_insight["taps"]),
                "Installs": int(daily_insight["installs"]),
                "newDownloads": int(daily_insight["newDownloads"]),
                "Spend": float(daily_insight["localSpend"]["amount"]),
                "Currency": daily_insight["localSpend"]["currency"],
            }

            tmp_df_ = pd.DataFrame.from_dict(tmp_dict_, orient="index").transpose()
            campaign_daily_df = pd.concat([campaign_daily_df, tmp_df_], axis=0).reset_index(
                drop=True
            )
    return campaign_daily_df
