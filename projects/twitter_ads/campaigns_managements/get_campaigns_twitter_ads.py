import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from prefect import task
from twitter_ads.campaign import Campaign
from twitter_ads.error import RateLimit

import projects.twitter_ads.settings.settings_twitter_ads as ps


@task(name="[TWITTER ADS] Get all campaigns")
def get_all_campaigns_twitter_ads(
    account_twitter_ads, campaign_ingestion_settings, start_date_settings, end_date_settings
):
    """
    Get all campaigns from Twitter Ads.

    Args:
        account_twitter_ads (str): Account Connection provided by previous date.
        campaign_ingestion_settings (str): Daily or Custom.
        start_date_settings (str): Start Date to request API Twitter Ads
        end_date_settings (str): End Date to request API Twitter Ads

    Returns:
        df (DataFrame): DataFrame with all campaigns and insights from Twitter Ads.
    """

    campaigns = Campaign.all(account_twitter_ads)
    df = pd.DataFrame()
    for campaign in campaigns:
        if campaign.name in ps.mapping_historic_campaign_twitter.keys():
            currency = campaign.currency
            if campaign_ingestion_settings == "Custom":
                start_date_settings = ps.mapping_historic_campaign_twitter[campaign.name][0]
                end_date_settings = ps.mapping_historic_campaign_twitter[campaign.name][1]
            if campaign_ingestion_settings == "Daily":
                start_date_settings = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                end_date_settings = datetime.now().strftime("%Y-%m-%d")

            current_date = datetime.strptime(start_date_settings, "%Y-%m-%d")

            while current_date <= datetime.strptime(end_date_settings, "%Y-%m-%d"):
                try:
                    campaign_statistics_df = get_campaign_statistics(
                        campaign=campaign,
                        metrics=["BILLING", "ENGAGEMENT"],
                        date_start=current_date,
                        date_end=current_date + timedelta(days=7),
                        granularity="DAY",
                    )

                except RateLimit:
                    time.sleep(15 * 60)
                    campaign_statistics_df = get_campaign_statistics(
                        campaign=campaign,
                        metrics=["BILLING", "ENGAGEMENT"],
                        date_start=current_date,
                        date_end=current_date + timedelta(days=7),
                        granularity="DAY",
                    )

                campaign_statistics_df["CampaignName"] = campaign.name
                campaign_statistics_df["CampaignId"] = campaign.id
                campaign_statistics_df["Currency"] = currency
                df = pd.concat([df, campaign_statistics_df], axis=0)
                current_date = current_date + timedelta(days=7)

    df["CampaignGroupName"] = df["CampaignName"]
    df = df[df["Spend"] > 0].reset_index(drop=True)
    df["Impressions"] = df["Impressions"].fillna(0)
    df["Likes"] = df["Likes"].fillna(0)
    df["Engagements"] = df["Engagements"].fillna(0)
    df["Clicks"] = df["Clicks"].fillna(0)
    df["Spend"] = df["Spend"].fillna(0)
    df["Retweets"] = df["Retweets"].fillna(0)
    df["Follows"] = df["Follows"].fillna(0)
    df["Unfollows"] = df["Unfollows"].fillna(0)
    df["CTR"] = np.where(
        df["Impressions"] == 0,
        0,
        df["Clicks"] / df["Impressions"],
    )
    df["CPC"] = np.where(
        df["Clicks"] == 0,
        df["Spend"],
        df["Spend"] / df["Clicks"],
    )

    return df


def get_campaign_statistics(campaign, metrics, date_start, date_end, granularity="DAY"):
    """
    Get all insights from a campaign Twitter Ads.

    Args:
        campaign (str): Campaign Twitter.
        metrics (List): List of metrics to get.
        date_start (date): Start Date of the campaign.
        date_end (date): End Date of the campaign.
        granularity (str): Define granularity to get insights by campaign.

    Returns:
        df (DataFrame): DataFrame with all campaigns and insights from Twitter Ads.
    """
    campaign_statistics = campaign.stats(
        metrics=metrics, start_time=date_start, end_time=date_end, granularity=granularity
    )

    if campaign_statistics[0]["id_data"][0]["metrics"]["billed_charge_local_micro"] is not None:
        campaign_statistics_df = pd.DataFrame(campaign_statistics[0]["id_data"][0]["metrics"])

        campaign_statistics_df = campaign_statistics_df[
            [
                "impressions",
                "likes",
                "engagements",
                "clicks",
                "billed_charge_local_micro",
                "retweets",
                "follows",
                "unfollows",
            ]
        ].rename(
            columns={
                "billed_charge_local_micro": "Spend",
                "impressions": "Impressions",
                "likes": "Likes",
                "engagements": "Engagements",
                "clicks": "Clicks",
                "retweets": "Retweets",
                "follows": "Follows",
                "unfollows": "Unfollows",
            }
        )

        campaign_statistics_df["Spend"] = campaign_statistics_df["Spend"] / 1000000

        date_index = pd.DataFrame(
            pd.date_range(start=date_start, end=date_end - timedelta(days=1), freq="D"),
            columns=["Date"],
        )

        campaign_statistics_df = pd.concat([date_index, campaign_statistics_df], axis=1)
    else:
        campaign_statistics_df = pd.DataFrame()
    return campaign_statistics_df
