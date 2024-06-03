from datetime import datetime, timedelta

import pandas as pd
import requests
from prefect import task


def get_campaigns_google_ads(
    access_token, developer_token, mcc_id, customer, start_date, end_date, next_page_token=None
):
    """
    Get all campaigns Google Ads by customer.

    Args:
        access_token (str): Access Token provided by previous step.
        developer_token (str): Developer Token provided by Google Ads Interface.
        mcc_id (str): MCC ID provided by Google Ads Interface.
        customer (str): Customer Google Ads provided by previous step.
        start_date (str): Start Date to request API Google Ads
            (if frequency = daily then start_date will be date today).
        end_date (str): End Date to request API Google Ads
            (if frequency = daily then start_date will be date today).
        next_page_token (str): Token to access next page.

    Returns:
        data (json): API return in JSON.
    """
    url = "https://googleads.googleapis.com/v14/" + customer + "/googleAds:search"
    headers = {
        "Authorization": "Bearer " + access_token,
        "developer-token": developer_token,
        "Content-Type": "application/json",
        "login-customer-id": mcc_id,
    }
    if next_page_token is None:
        pass
    else:
        url = f"{url}?pageToken={next_page_token}"
    response = requests.post(
        url=url,
        headers=headers,
        json={
            "query": """SELECT segments.date, campaign.id, campaign.name,
                              campaign_budget.amount_micros , campaign.status ,
                              campaign.optimization_score , campaign.advertising_channel_type,
                              metrics.clicks, metrics.impressions, metrics.ctr,
                              metrics.average_cpc, metrics.cost_micros,
                              campaign.bidding_strategy_type
                              FROM campaign
                              WHERE segments.date BETWEEN '"""
            + start_date
            + "' AND '"
            + end_date
            + "'"
        },
    )

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return None


@task(name="[GOOGLE ADS] Get all campaigns")
def get_all_campaigns_google_ads(
    google_ads_accounts_df,
    access_token,
    developer_token,
    mcc_id,
    campaign_ingestion_settings,
    start_date_settings,
    end_date_settings,
):
    """
    Get all campaigns Google Ads by customer.

    Args:
        google_ads_accounts_df (DataFrame): DataFrame with all accounts available in Google Ads.
        access_token (str): Access Token provided by previous step.
        developer_token (str): Developer Token provided by Google Ads Interface.
        mcc_id (str): MCC ID provided by Google Ads Interface.
        campaign_ingestion_settings (str): Daily or Custom.
        start_date_settings (str): Start Date to request API Google Ads
        end_date_settings (str): End Date to request API Google Ads

    Returns:
        df (DataFrame): DataFrame with all campaigns.
    """

    df = pd.DataFrame()
    for customer in google_ads_accounts_df["customerClient.clientCustomer"].unique():
        process = True
        next_page_token = None
        name = google_ads_accounts_df.loc[
            google_ads_accounts_df["customerClient.clientCustomer"] == customer,
            "customerClient.descriptiveName",
        ].values[0]
        currency = google_ads_accounts_df.loc[
            google_ads_accounts_df["customerClient.clientCustomer"] == customer,
            "customerClient.currencyCode",
        ].values[0]

        if campaign_ingestion_settings == "Daily":
            start_date_settings = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            end_date_settings = datetime.now().strftime("%Y-%m-%d")

        while process:
            data = get_campaigns_google_ads(
                access_token=access_token,
                developer_token=developer_token,
                mcc_id=mcc_id,
                customer=customer,
                start_date=start_date_settings,
                end_date=end_date_settings,
                next_page_token=next_page_token,
            )
            if data is not None and "results" in data:
                tmp_df = pd.json_normalize(data["results"])
                tmp_df["customerClient.descriptiveName"] = name
                tmp_df["customerClient.Currency"] = currency

                df = pd.concat([df, tmp_df], axis=0)
                if data.get("nextPageToken"):
                    next_page_token = data["nextPageToken"]
                else:
                    process = False
            else:
                process = False

    if len(df) > 0:
        df["metrics.CostInDollar"] = df["metrics.costMicros"].astype(float) / 1000000
        df = df.rename(
            columns={
                "campaign.status": "CampaignStatus",
                "campaign.advertisingChannelType": "AdvertisingChannelType",
                "campaign.biddingStrategyType": "BiddingStrategyType",
                "campaign.name": "CampaignName",
                "campaign.id": "CampaignId",
                "metrics.clicks": "Clicks",
                "metrics.ctr": "CTR",
                "metrics.averageCpc": "CPC",
                "metrics.impressions": "Impressions",
                "segments.date": "Date",
                "customerClient.descriptiveName": "CampaignGroupName",
                "customerClient.Currency": "Currency",
                "campaign.optimizationScore": "OptimizationScore",
                "metrics.CostInDollar": "Spend",
            }
        )

    return df
