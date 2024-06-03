import numpy as np
import pandas as pd
from prefect import task


@task(name="[LINKEDIN ADS] Configure format table output")
def create_final_table_linkedin(
    campaign_group_linkedin_df, campaign_linkedin_df, campaign_linkedin_stats_df
):
    """
    Create final table of LinkedIn Ads before export in database.

    Args:
        campaign_group_linkedin_df (DataFrame): DataFrame with all campaigns groups.
        campaign_linkedin_df (DataFrame): DataFrame with all campaigns.
        campaign_linkedin_stats_df (DataFrame): DataFrame with all insights.

    Returns:
        final_linkedin_information (DataFrame): DataFrame consolidated.
    """
    campaign_group_linkedin_df["CampaignGroupId"] = campaign_group_linkedin_df[
        "CampaignGroupId"
    ].astype(str)
    campaign_linkedin_df["CampaignGroupId"] = campaign_linkedin_df["CampaignGroupId"].astype(str)

    final_linkedin_information = campaign_group_linkedin_df.merge(
        campaign_linkedin_df[
            [
                "CampaignName",
                "CampaignId",
                "CampaignGroupId",
                "CampaignStatus",
                "OptimizationTargetType",
                "Language",
            ]
        ],
        how="inner",
        on="CampaignGroupId",
    )

    final_linkedin_information["CampaignId"] = final_linkedin_information["CampaignId"].astype(str)
    campaign_linkedin_stats_df["CampaignId"] = campaign_linkedin_stats_df["CampaignId"].astype(str)
    final_linkedin_information = final_linkedin_information.merge(
        campaign_linkedin_stats_df, on="CampaignId", how="left"
    )

    final_linkedin_information = final_linkedin_information[
        final_linkedin_information["CampaignStatus"] != "DRAFT"
    ]

    final_linkedin_information = final_linkedin_information[
        pd.notna(final_linkedin_information["Spend"])
    ]

    final_linkedin_information["Spend"] = final_linkedin_information["Spend"].fillna(0)
    final_linkedin_information["Impressions"] = final_linkedin_information["Impressions"].fillna(0)
    final_linkedin_information["Clicks"] = final_linkedin_information["Clicks"].fillna(0)
    final_linkedin_information["Likes"] = final_linkedin_information["Likes"].fillna(0)

    final_linkedin_information = final_linkedin_information.sort_values(
        ["CampaignGroupName", "CampaignId", "Date"]
    )

    final_linkedin_information["Spend"] = final_linkedin_information["Spend"].astype(float)
    final_linkedin_information["Impressions"] = final_linkedin_information["Impressions"].astype(
        float
    )
    final_linkedin_information["Clicks"] = final_linkedin_information["Clicks"].astype(float)
    final_linkedin_information["Likes"] = final_linkedin_information["Likes"].astype(float)
    final_linkedin_information["CPC"] = np.where(
        final_linkedin_information["Clicks"] == 0,
        final_linkedin_information["Spend"],
        final_linkedin_information["Spend"] / final_linkedin_information["Clicks"],
    )

    final_linkedin_information["CTR"] = np.where(
        final_linkedin_information["Impressions"] == 0,
        0,
        final_linkedin_information["Clicks"] / final_linkedin_information["Impressions"],
    )

    return final_linkedin_information
