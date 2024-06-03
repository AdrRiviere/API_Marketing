#!/usr/bin/env python
import os
import warnings
from datetime import date, datetime

from dotenv import load_dotenv
from prefect import flow

import projects.common.global_settings.mapping_campaign as m
import projects.linkedin_ads.settings.settings_linkedin_ads as ps
from projects.common.data_manager.upload_outputs import upload_outputs
from projects.common.features_engineering.create_dimension_by_campaign import (
    create_channel_acquisition,
    create_language_dimension,
    create_market_dimension,
)
from projects.common.global_settings.global_settings import CampaignIngestion
from projects.common.utils import create_hash_id
from projects.linkedin_ads.campaigns_management.get_accounts_linkedin_ads import (
    get_accounts_linkedin_ads,
)
from projects.linkedin_ads.campaigns_management.get_campaigns_information_linkedin_ads import (
    get_campaign_information_linkedin_ads,
)
from projects.linkedin_ads.campaigns_management.get_insights_by_campaign_linkedin_ads import (
    get_all_insights_daily_linkedin_ads,
)
from projects.linkedin_ads.utils.create_final_output_linkedin_ads import create_final_table_linkedin
from projects.linkedin_ads.utils.get_access_token_linkedin_ads import token_negotiation_linkedin_ads

warnings.filterwarnings("ignore")


@flow(
    name="[iTrust][Marketing] Campaign Tracking",
    log_prints=True,
    flow_run_name="linkedin-ads-"
    + ps.environment
    + " "
    + str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
)
def main_tracking_campaign_linkedin_ads(
    campaign_ingestion_settings: CampaignIngestion = CampaignIngestion.Daily,
    start_date_settings: date = datetime.now().date(),
    end_date_settings: date = datetime.now().date(),
):
    """
    Main code to process the tracking of all campaigns
    proceed on LinkedIn Ads by Flowbank.
    Args:
        campaign_ingestion_settings (str): Daily or Custom.
        start_date_settings (str): Start Date to request API Google Ads
                          (if frequency = daily then start_date will be date today).
        end_date_settings (str): End Date to request API Google Ads
                        (if frequency = daily then start_date will be date today).
    """

    dict_data_to_export = {}
    load_dotenv()

    access_token = token_negotiation_linkedin_ads(
        client_id=os.environ.get("client_id_linkedin_ads"),
        client_secret=os.environ.get("client_secret_linkedin_ads"),
        refresh_token=os.environ.get("refresh_token_linkedin_ads"),
    )

    campaign_group_linkedin_df = get_accounts_linkedin_ads(access_token=access_token)

    campaign_linkedin_df = get_campaign_information_linkedin_ads(access_token=access_token)

    campaign_linkedin_stats_df = get_all_insights_daily_linkedin_ads(
        campaign_linkedin_df=campaign_linkedin_df,
        access_token=access_token,
        campaign_ingestion_settings=campaign_ingestion_settings.value,
        start_date_settings=start_date_settings,
        end_date_settings=end_date_settings,
    )

    campaign_linkedin_df = create_final_table_linkedin(
        campaign_group_linkedin_df=campaign_group_linkedin_df,
        campaign_linkedin_df=campaign_linkedin_df,
        campaign_linkedin_stats_df=campaign_linkedin_stats_df,
    )

    if len(campaign_linkedin_df) > 0:

        campaign_linkedin_df["CampaignName"] = campaign_linkedin_df["CampaignName"].apply(
            lambda x: x.replace("zzz", "")
        )

        campaign_linkedin_df = create_channel_acquisition(
            marketing_df=campaign_linkedin_df,
            variable_campaign="CampaignName",
            mapping_channel=m.mapping_channel,
            default_channel="Linkedin Ads",
        )

        campaign_linkedin_df = create_market_dimension(
            marketing_df=campaign_linkedin_df,
            variable_campaign="CampaignName",
            mapping_market=m.mapping_market,
        )

        campaign_linkedin_df = create_language_dimension(
            marketing_df=campaign_linkedin_df,
            variable_campaign="CampaignName",
            mapping_language=m.mapping_language,
            default_channel="Linkedin Ads",
        )

        campaign_linkedin_df = create_hash_id(
            df=campaign_linkedin_df, columns=["CampaignId", "Date"]
        )

        dict_data_to_export["campaign_linkedin_df"] = campaign_linkedin_df[ps.columns_output]
        print("Number of rows to insert :" + str(len(campaign_linkedin_df)))

        upload_outputs(
            dict_dataset=dict_data_to_export,
            database=ps.database,
            name_table=ps.name_table_output,
            export_option=ps.option,
            environment=ps.environment,
            query=ps.query_linkedin_ads_to_export,
        )
    else:
        print("No Data to insert for these dates")
