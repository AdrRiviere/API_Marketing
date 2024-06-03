#!/usr/bin/env python
import os
import warnings
from datetime import date, datetime

from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from prefect import flow

import projects.common.global_settings.mapping_campaign as m
import projects.meta_ads.settings.settings_meta_ads as ps
from projects.common.data_manager.upload_outputs import upload_outputs
from projects.common.features_engineering.create_dimension_by_campaign import (
    create_channel_acquisition,
    create_language_dimension,
    create_market_dimension,
)
from projects.common.global_settings.global_settings import CampaignIngestion
from projects.common.utils import create_hash_id
from projects.meta_ads.campaigns_management.get_accounts_meta_ads import get_accounts_meta_ads
from projects.meta_ads.campaigns_management.get_campaigns_meta_ads import (
    get_all_campaigns_all_accounts_meta_ads,
)
from projects.meta_ads.utils.get_access_token_meta_ads import token_negotiation_meta_ads

warnings.filterwarnings("ignore")


@flow(
    name="[iTrust][Marketing] Campaign Tracking",
    log_prints=True,
    flow_run_name="meta-ads-"
    + ps.environment
    + " "
    + str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
)
def main_tracking_campaign_meta_ads(
    campaign_ingestion_settings: CampaignIngestion = CampaignIngestion.Daily,
    start_date_settings: date = datetime.now().date(),
    end_date_settings: date = datetime.now().date(),
):
    """
    Main code to process the tracking of all campaigns
    proceed on Meta Ads by Flowbank.
    Args:
        campaign_ingestion_settings (str): Daily or Custom.
        start_date_settings (str): Start Date to request API Google Ads
                          (if frequency = daily then start_date will be date today).
        end_date_settings (str): End Date to request API Google Ads
                        (if frequency = daily then start_date will be date today).
    """

    dict_data_to_export = {}
    load_dotenv()

    access_token = token_negotiation_meta_ads(
        app_id=os.environ.get("app_id_meta_ads"),
        app_secret=os.environ.get("app_secret_meta_ads"),
        refresh_token=os.environ.get("refresh_token_meta_ads"),
    )

    FacebookAdsApi.init(
        os.environ.get("app_id_meta_ads"),
        os.environ.get("app_secret_meta_ads"),
        access_token,
    )

    meta_ads_accounts_df = get_accounts_meta_ads()

    all_campaigns_with_insights = get_all_campaigns_all_accounts_meta_ads(
        meta_ads_accounts_df=meta_ads_accounts_df,
        campaign_ingestion_settings=campaign_ingestion_settings,
        start_date_settings=start_date_settings,
        end_date_settings=end_date_settings,
    )

    if len(all_campaigns_with_insights) > 0:
        all_campaigns_with_insights = all_campaigns_with_insights.drop_duplicates(
            ["CampaignId", "Date"]
        )

        all_campaigns_with_insights["CampaignName"] = all_campaigns_with_insights[
            "CampaignName"
        ].apply(lambda x: x.replace("zzz", ""))

        all_campaigns_with_insights = create_channel_acquisition(
            marketing_df=all_campaigns_with_insights,
            variable_campaign="CampaignName",
            mapping_channel=m.mapping_channel,
            default_channel="Facebook / Instagram Organic",
        )

        all_campaigns_with_insights = create_market_dimension(
            marketing_df=all_campaigns_with_insights,
            variable_campaign="CampaignName",
            mapping_market=m.mapping_market,
        )

        all_campaigns_with_insights = create_language_dimension(
            marketing_df=all_campaigns_with_insights,
            variable_campaign="CampaignName",
            mapping_language=m.mapping_language,
            default_channel="Facebook / Instagram Organic",
        )

        all_campaigns_with_insights = create_hash_id(
            df=all_campaigns_with_insights, columns=["CampaignId", "Date"]
        )

        dict_data_to_export["meta_ads_campaigns_df"] = all_campaigns_with_insights[
            ps.columns_output
        ]

        print("Number of rows to insert :" + str(len(all_campaigns_with_insights)))

        upload_outputs(
            dict_dataset=dict_data_to_export,
            database=ps.database,
            name_table=ps.name_table_output,
            export_option=ps.option,
            environment=ps.environment,
            query=ps.query_meta_ads_to_export,
        )
    else:
        print("No Data to insert for these dates")
