#!/usr/bin/env python
import os
import warnings
from datetime import date, datetime

from dotenv import load_dotenv
from prefect import flow

import projects.common.global_settings.mapping_campaign as m
import projects.google_ads.settings.settings_google_ads as ps
from projects.common.data_manager.upload_outputs import upload_outputs
from projects.common.features_engineering.create_dimension_by_campaign import (
    create_channel_acquisition,
    create_language_dimension,
    create_market_dimension,
)
from projects.common.global_settings.global_settings import CampaignIngestion
from projects.common.utils import create_hash_id
from projects.google_ads.campaigns_management.get_accounts_google_ads import get_accounts_google_ads
from projects.google_ads.campaigns_management.get_campaigns_google_ads import (
    get_all_campaigns_google_ads,
)
from projects.google_ads.utils.cleaning_campaign_name_google_ads import specific_cleaning_google_ads
from projects.google_ads.utils.get_access_token_google_ads import token_negotiation_google_ads

warnings.filterwarnings("ignore")


@flow(
    name="[iTrust][Marketing] Campaign Tracking",
    log_prints=True,
    flow_run_name="google-ads-"
    + ps.environment
    + " "
    + str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
)
def main_tracking_campaign_google_ads(
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

    access_token = token_negotiation_google_ads(
        client_id=os.environ.get("client_id_google_ads"),
        client_secret=os.environ.get("client_secret_google_ads"),
        refresh_token=os.environ.get("refresh_token_google_ads"),
    )

    google_ads_accounts_df = get_accounts_google_ads(
        mcc_id=os.environ.get("mcc_id_google_ads"),
        access_token=access_token,
        developer_token=os.environ.get("developer_token_google_ads"),
    )

    google_ads_campaigns_df = get_all_campaigns_google_ads(
        google_ads_accounts_df=google_ads_accounts_df,
        access_token=access_token,
        developer_token=os.environ.get("developer_token_google_ads"),
        mcc_id=os.environ.get("mcc_id_google_ads"),
        campaign_ingestion_settings=campaign_ingestion_settings.value,
        start_date_settings=start_date_settings.strftime("%Y-%m-%d"),
        end_date_settings=end_date_settings.strftime("%Y-%m-%d"),
    )

    if len(google_ads_campaigns_df) > 0:

        google_ads_campaigns_df["CampaignName"] = google_ads_campaigns_df["CampaignName"].apply(
            lambda x: x.replace("zzz", "")
        )

        google_ads_campaigns_df = create_channel_acquisition(
            marketing_df=google_ads_campaigns_df,
            variable_campaign="CampaignName",
            mapping_channel=m.mapping_channel,
            default_channel="Google Ads",
        )

        google_ads_campaigns_df = create_market_dimension(
            marketing_df=google_ads_campaigns_df,
            variable_campaign="CampaignName",
            mapping_market=m.mapping_market,
        )

        google_ads_campaigns_df = create_language_dimension(
            marketing_df=google_ads_campaigns_df,
            variable_campaign="CampaignName",
            mapping_language=m.mapping_language,
            default_channel="Google Ads",
        )

        google_ads_campaigns_df = specific_cleaning_google_ads(
            df=google_ads_campaigns_df,
        )

        google_ads_campaigns_df = create_hash_id(
            df=google_ads_campaigns_df, columns=["CampaignId", "Date"]
        )

        dict_data_to_export["google_ads_campaigns_df"] = google_ads_campaigns_df[ps.columns_output]

        print("Number of rows to insert :" + str(len(google_ads_campaigns_df)))

        upload_outputs(
            dict_dataset=dict_data_to_export,
            database=ps.database,
            name_table=ps.name_table_output,
            export_option=ps.option,
            environment=ps.environment,
            query=ps.query_google_ads_to_export,
        )
    else:
        print("No Data to insert for these dates")
