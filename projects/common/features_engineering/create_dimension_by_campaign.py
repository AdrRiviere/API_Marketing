import re

import numpy as np
import pandas as pd
from prefect import task


def check_campaign_name(campaign_name):
    """
    Check the format of the campaign name.

    Args:
        campaign_name (str): Namer of the campaign
    Returns:
        Boolean:
    """
    if re.search(
        r"([A-Z]{2}\-[A-Z]{2}\-[a-zA-Z]{2}\-[a-zA-Z]*\-0)|"
        r"([A-Z]{2}\-[A-Z]{2}\-[a-zA-Z]{2}\-[a-zA-Z]*\-0-0)|"
        r"([A-Z]{2}\-[A-Z]{2}\-[a-zA-Z]{2}\-[a-zA-Z]*)",
        campaign_name,
    ):
        return True
    else:
        return False


@task(name="[FEATURES ENGINEERING] Create Market Dimension")
def create_market_dimension(marketing_df: pd.DataFrame, variable_campaign: str, mapping_market):
    """
    Creation of the variable Market based on the first 3-5 characters of the campaign name.

    Args:
        marketing_df (DataFrame) : Campaign DataFrame.
        variable_campaign (DataFrame) : Name of the column with campaigns.
        mapping_market (Dict) : Mapping Market.
    Returns:
        DataFrame (marketing_df): Marketing table with the information of the Market.
    """

    # Step 1 : Automated cleaning
    marketing_df["TemplateCampaignNaming"] = marketing_df[variable_campaign].apply(
        lambda x: check_campaign_name(x)
    )
    marketing_df["MarketCampaign"] = marketing_df[variable_campaign].apply(lambda x: x[3:5])
    marketing_df["MarketCampaign"] = np.where(
        marketing_df["TemplateCampaignNaming"].isin([True]), marketing_df["MarketCampaign"], ""
    )
    marketing_df["MarketCampaign"] = marketing_df["MarketCampaign"].replace(mapping_market)
    marketing_df = marketing_df.drop(["TemplateCampaignNaming"], axis=1)
    return marketing_df


@task(name="[FEATURES ENGINEERING] Create Language Dimension")
def create_language_dimension(
    marketing_df: pd.DataFrame, variable_campaign: str, mapping_language, default_channel
):
    """
    Creation of the variable Language based on the first 6-8 characters of the campaign name.

    Args:
        marketing_df (DataFrame) : Campaign DataFrame.
        variable_campaign (DataFrame) : Name of the column campaign.
        mapping_language (Dict) : Mapping Language.
        default_channel (str) : Default Language.
    Returns:
        DataFrame (marketing_df): Marketing table with the information of the Language.
    """

    # Step 1 : Automated cleaning
    marketing_df["TemplateCampaignNaming"] = marketing_df[variable_campaign].apply(
        lambda x: check_campaign_name(x)
    )
    marketing_df["LanguageCampaign"] = marketing_df[variable_campaign].apply(lambda x: x[6:8])
    if default_channel == "Linkedin Ads":
        marketing_df["LanguageCampaign"] = np.where(
            marketing_df["TemplateCampaignNaming"].isin([True]),
            marketing_df["LanguageCampaign"],
            marketing_df["Language"],
        )
    else:
        marketing_df["LanguageCampaign"] = np.where(
            marketing_df["TemplateCampaignNaming"].isin([True]),
            marketing_df["LanguageCampaign"],
            "",
        )
    marketing_df["LanguageCampaign"] = marketing_df["LanguageCampaign"].replace(mapping_language)
    marketing_df = marketing_df.drop(["TemplateCampaignNaming"], axis=1)
    return marketing_df


@task(name="[FEATURES ENGINEERING] Create Channel Dimension")
def create_channel_acquisition(
    marketing_df: pd.DataFrame, variable_campaign: str, mapping_channel, default_channel: str
):
    """
    Creation of the variable Channel based on the first 2 characters of the campaign name.

    Args:
        marketing_df (DataFrame) : Campaign DataFrame.
        variable_campaign (DataFrame) : Name of the column campaign.
        mapping_channel (Dict) : Mapping Channel.
        default_channel (Str) : Channel by default.
    Returns:
        DataFrame (marketing_df): Marketing table with the information of the Channel.
    """

    # Step 1 : Automated cleaning
    marketing_df["TemplateCampaignNaming"] = marketing_df[variable_campaign].apply(
        lambda x: check_campaign_name(x)
    )
    marketing_df["ChannelAcquisition"] = marketing_df[variable_campaign].apply(lambda x: x[0:2])
    marketing_df["ChannelAcquisition"] = marketing_df["ChannelAcquisition"].replace(mapping_channel)

    marketing_df["ChannelAcquisition"] = np.where(
        marketing_df["TemplateCampaignNaming"].isin([True]),
        marketing_df["ChannelAcquisition"],
        default_channel,
    )
    marketing_df = marketing_df.drop(["TemplateCampaignNaming"], axis=1)
    return marketing_df


def starts_with_prefix(name, looking_for):
    for prefix in looking_for:
        if name.startswith(prefix):
            return True
    return False
