#!/usr/bin/env python
import os
import warnings
from datetime import date, datetime

from dotenv import load_dotenv
from prefect import flow

import projects.apple_search.settings.settings_apple_search as ps
import projects.common.global_settings.mapping_campaign as m
from projects.apple_search.campaigns_management.get_all_campaigns_apple_search import (
    get_all_campaign_apple_search,
)
from projects.apple_search.campaigns_management.get_statistics_by_campaign_apple_search import (
    get_full_insights_process_by_campaign,
)
from projects.apple_search.utils.get_access_token_apple_search import token_negotiation_apple_search
from projects.common.data_manager.upload_outputs import upload_outputs
from projects.common.features_engineering.create_dimension_by_campaign import (
    create_channel_acquisition,
    create_language_dimension,
    create_market_dimension,
)
from projects.common.global_settings.global_settings import CampaignIngestion
from projects.common.utils import create_hash_id

warnings.filterwarnings("ignore")


@flow(
    name="[iTrust][Marketing] Campaign Tracking",
    log_prints=True,
    flow_run_name="apple-search-"
    + ps.environment
    + " "
    + str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
)
def main_tracking_campaign_apple_search(
    campaign_ingestion_settings: CampaignIngestion = CampaignIngestion.Daily,
    start_date_settings: date = datetime.now().date(),
    end_date_settings: date = datetime.now().date(),
):
    """
    Main code to process the tracking of all campaigns
    proceed on Apple Search by Flowbank.
    Args:
        campaign_ingestion_settings (str): Daily or Custom.
        start_date_settings (str): Start Date to request API Google Ads
                          (if frequency = daily then start_date will be date today).
        end_date_settings (str): End Date to request API Google Ads
                        (if frequency = daily then start_date will be date today).
    """
    dict_data_to_export = {}
    load_dotenv()

    access_token = token_negotiation_apple_search(
        client_id=os.environ.get("client_id_apple_search"),
        client_secret=os.environ.get("client_secret_apple_search"),
    )

    campaign_group_apple_search_df = get_all_campaign_apple_search(access_token=access_token)

    campaign_apple_search_df = get_full_insights_process_by_campaign(
        access_token=access_token,
        campaign_group_apple_search_df=campaign_group_apple_search_df,
        campaign_ingestion_settings=campaign_ingestion_settings.value,
        start_date_settings=start_date_settings,
        end_date_settings=end_date_settings,
    )

    if len(campaign_apple_search_df) > 0:

        campaign_apple_search_df["CampaignName"] = campaign_apple_search_df["CampaignName"].apply(
            lambda x: x.replace("zzz", "")
        )

        campaign_apple_search_df = create_channel_acquisition(
            marketing_df=campaign_apple_search_df,
            variable_campaign="CampaignName",
            mapping_channel=m.mapping_channel,
            default_channel="Apple Search Ads",
        )

        campaign_apple_search_df = create_market_dimension(
            marketing_df=campaign_apple_search_df,
            variable_campaign="CampaignName",
            mapping_market=m.mapping_market,
        )

        campaign_apple_search_df = create_language_dimension(
            marketing_df=campaign_apple_search_df,
            variable_campaign="CampaignName",
            mapping_language=m.mapping_language,
            default_channel="Apple Search Ads",
        )

        campaign_apple_search_df = create_hash_id(
            df=campaign_apple_search_df, columns=["CampaignId", "CampaignGroupName", "Date"]
        )

        dict_data_to_export["campaign_apple_search_df"] = campaign_apple_search_df[
            ps.columns_output
        ]

        print("Number of rows to insert :" + str(len(campaign_apple_search_df)))

        upload_outputs(
            dict_dataset=dict_data_to_export,
            database=ps.database,
            name_table=ps.name_table_output,
            export_option=ps.option,
            environment=ps.environment,
            query=ps.query_apple_search_to_export,
        )
    else:
        print("No Data to insert for these dates")
