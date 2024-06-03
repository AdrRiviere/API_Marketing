#!/usr/bin/env python
import os
import warnings
from datetime import date, datetime

from dotenv import load_dotenv
from prefect import flow

import projects.common.global_settings.mapping_campaign as m
import projects.twitter_ads.settings.settings_twitter_ads as ps
from projects.common.data_manager.upload_outputs import upload_outputs
from projects.common.features_engineering.create_dimension_by_campaign import (
    create_channel_acquisition,
    create_language_dimension,
    create_market_dimension,
)
from projects.common.global_settings.global_settings import CampaignIngestion
from projects.common.utils import create_hash_id
from projects.twitter_ads.campaigns_managements.get_campaigns_twitter_ads import (
    get_all_campaigns_twitter_ads,
)
from projects.twitter_ads.utils.get_access_token_twitter_ads import connect_account_twitter_ads

warnings.filterwarnings("ignore")


@flow(
    name="[iTrust][Marketing] Campaign Tracking",
    log_prints=True,
    flow_run_name="twitter-ads-"
    + ps.environment
    + " "
    + str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
)
def main_tracking_campaign_twitter_ads(
    campaign_ingestion_settings: CampaignIngestion = CampaignIngestion.Daily,
    start_date_settings: date = datetime.now().date(),
    end_date_settings: date = datetime.now().date(),
):
    """
    Main code to process the tracking of all campaigns
    proceed on Google Ads by Flowbank.
    Args:
        campaign_ingestion_settings (str): Daily or Custom.
        start_date_settings (str): Start Date to request API Google Ads
                          (if frequency = daily then start_date will be date today).
        end_date_settings (str): End Date to request API Google Ads
                        (if frequency = daily then start_date will be date today).
    """

    dict_data_to_export = {}
    load_dotenv()

    account_twitter_ads = connect_account_twitter_ads(
        account_id=os.environ.get("account_id_twitter_ads"),
        consumer_key=os.environ.get("consumer_key_twitter_ads"),
        consumer_secret=os.environ.get("consumer_secret_twitter_ads"),
        access_token=os.environ.get("access_token_twitter_ads"),
        access_token_secret=os.environ.get("access_token_secret_twitter_ads"),
    )

    campaign_twitter_ads_df = get_all_campaigns_twitter_ads(
        account_twitter_ads=account_twitter_ads,
        campaign_ingestion_settings=campaign_ingestion_settings.value,
        start_date_settings=start_date_settings.strftime("%Y-%m-%d"),
        end_date_settings=end_date_settings.strftime("%Y-%m-%d"),
    )

    if len(campaign_twitter_ads_df) > 0:

        campaign_twitter_ads_df["CampaignName"] = campaign_twitter_ads_df["CampaignName"].apply(
            lambda x: x.replace("zzz", "")
        )

        campaign_twitter_ads_df = create_channel_acquisition(
            marketing_df=campaign_twitter_ads_df,
            variable_campaign="CampaignName",
            mapping_channel=m.mapping_channel,
            default_channel="Twitter",
        )

        campaign_twitter_ads_df = create_market_dimension(
            marketing_df=campaign_twitter_ads_df,
            variable_campaign="CampaignName",
            mapping_market=m.mapping_market,
        )

        campaign_twitter_ads_df = create_language_dimension(
            marketing_df=campaign_twitter_ads_df,
            variable_campaign="CampaignName",
            mapping_language=m.mapping_language,
            default_channel="Twitter",
        )

        campaign_twitter_ads_df = create_hash_id(
            df=campaign_twitter_ads_df, columns=["CampaignId", "Date"]
        )

        dict_data_to_export["campaign_twitter_ads_df"] = campaign_twitter_ads_df[ps.columns_output]

        print("Number of rows to insert :" + str(len(campaign_twitter_ads_df)))

        upload_outputs(
            dict_dataset=dict_data_to_export,
            database=ps.database,
            name_table=ps.name_table_output,
            export_option=ps.option,
            environment=ps.environment,
            query=ps.query_twitter_ads_to_export,
        )
    else:
        print("No Data to insert for these dates")
