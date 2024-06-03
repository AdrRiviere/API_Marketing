import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.exceptions import FacebookRequestError
from prefect import task

from projects.meta_ads.campaigns_management.get_insights_campaigns_meta_ads import (
    get_insights_by_campaign_meta_ads,
)


def get_all_campaigns_by_account_meta_ads(account_id):
    """
    Get all campaigns available by account.

    Args:
        account_id (str): Account ID in Meta Ads.
    Returns:
        campaigns (List): List with all campaigns available in Meta Ads.
        account_name (str): Name of the account in Meta Ads.
    """

    ad_account = AdAccount(account_id)
    account_name = ad_account.api_get(fields=[AdAccount.Field.name])["name"]
    # Get all campaigns in Meta Account
    campaigns = ad_account.get_campaigns(
        fields=[
            Campaign.Field.id,
            Campaign.Field.source_campaign_id,
            Campaign.Field.name,
            Campaign.Field.effective_status,
            Campaign.Field.objective,
            Campaign.Field.bid_strategy,
            Campaign.Field.daily_budget,
            Campaign.Field.source_campaign,
            Campaign.Field.budget_remaining,
            Campaign.Field.start_time,
            Campaign.Field.stop_time,
        ]
    )

    return campaigns, account_name


def get_campaign_information_meta_ads(campaign, account_id, account_name):

    campaign_information_meta_ads = {
        "CampaignGroupId": account_id,
        "CampaignGroupName": account_name,
        "CampaignName": campaign.get("name", np.nan),
        "CampaignId": campaign.get("id", np.nan),
        "CampaignStatus": campaign.get("effective_status", np.nan),
        "Objective": campaign.get("objective", np.nan),
        "BidStrategy": campaign.get("bid_strategy", np.nan),
        "DailyBudget": campaign.get("daily_budget", np.nan),
        "BudgetRemaining": campaign.get("budget_remaining", np.nan),
        "DateStart": campaign.get("start_time", np.nan),
        "DateEnd": campaign.get("stop_time", np.nan),
    }

    return campaign_information_meta_ads


@task(name="[Meta ADS] Get all campaigns")
def get_all_campaigns_all_accounts_meta_ads(
    meta_ads_accounts_df, campaign_ingestion_settings, start_date_settings, end_date_settings
):

    all_campaigns_with_insights = pd.DataFrame()
    for account_id in meta_ads_accounts_df:
        # Step 1 : Get All Campaigns available by account
        campaigns, account_name = get_all_campaigns_by_account_meta_ads(account_id=account_id)
        print("[META ADS] Process of the Account : " + str(account_name))
        # Step 2 : Get Insights by Campaign
        for campaign in campaigns:
            try:
                campaign_insights_df = get_all_campaigns_with_insights_meta_ads(
                    campaign=campaign,
                    account_id=account_id,
                    account_name=account_name,
                    campaign_ingestion_settings=campaign_ingestion_settings,
                    start_date_settings=start_date_settings,
                    end_date_settings=end_date_settings,
                )

                all_campaigns_with_insights = pd.concat(
                    [all_campaigns_with_insights, campaign_insights_df], axis=0
                )
            except FacebookRequestError:
                print("[META ADS] Max API Call - Wait 1 hour")
                time.sleep(3600)
                print("[META ADS] Process of the Account : " + str(account_name))
                campaign_insights_df = get_all_campaigns_with_insights_meta_ads(
                    campaign=campaign,
                    account_id=account_id,
                    account_name=account_name,
                    campaign_ingestion_settings=campaign_ingestion_settings,
                    start_date_settings=start_date_settings,
                    end_date_settings=end_date_settings,
                )

                all_campaigns_with_insights = pd.concat(
                    [all_campaigns_with_insights, campaign_insights_df], axis=0
                )

    return all_campaigns_with_insights


def get_all_campaigns_with_insights_meta_ads(
    campaign,
    account_id,
    account_name,
    campaign_ingestion_settings,
    start_date_settings,
    end_date_settings,
):

    # Get Campaign Information
    campaign_information = get_campaign_information_meta_ads(
        campaign=campaign, account_id=account_id, account_name=account_name
    )

    # Check Date
    if campaign_ingestion_settings == "Daily":
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
    else:
        start_date = start_date_settings
        end_date = end_date_settings

    # Get Campaign Insights
    campaign_insights_df = get_insights_by_campaign_meta_ads(
        campaign_id=campaign_information["CampaignId"],
        campaign_ingestion_settings=campaign_ingestion_settings,
        start_date_settings=start_date,
        end_date_settings=end_date,
    )

    if len(campaign_insights_df) > 0:
        tmp_df = pd.DataFrame.from_dict(campaign_information, orient="index").transpose()
        campaign_insights_df = pd.concat(
            [
                pd.concat([tmp_df] * len(campaign_insights_df), ignore_index=True),
                campaign_insights_df,
            ],
            axis=1,
        ).reset_index(drop=True)

    return campaign_insights_df
