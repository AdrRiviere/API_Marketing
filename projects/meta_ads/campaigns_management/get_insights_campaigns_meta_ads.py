import numpy as np
import pandas as pd
from facebook_business.adobjects.campaign import Campaign


def get_insights_by_campaign_meta_ads(
    campaign_id, campaign_ingestion_settings, start_date_settings, end_date_settings
):
    """
    Get all campaigns available by account.

    Args:
        campaign_id : Campaign ID Meta Ads.
        campaign_ingestion_settings (str): Daily or Custom.
        start_date_settings (str): Start Date to request API Google Ads
        end_date_settings (str): End Date to request API Google Ads
    Returns:
    """

    campaign = Campaign(fbid=campaign_id)
    stats_campaign = campaign.get_insights(
        fields=["impressions", "clicks", "spend", "cpc", "cpp", "ctr", "reach"],
        params={
            "time_range": {
                "since": str(start_date_settings),
                "until": str(end_date_settings),
            },
            "level": "campaign",
            "time_increment": 1,
        },
    )

    if len(stats_campaign) > 0:
        stats_campaign_df = pd.DataFrame()
        for daily_insights in stats_campaign:
            campaign_insights_meta_ads = {
                "Date": daily_insights.get("date_start", np.nan),
                "Spend": daily_insights.get("spend", np.nan),
                "Clicks": daily_insights.get("clicks", np.nan),
                "Reach": daily_insights.get("reach", np.nan),
                "Impressions": daily_insights.get("impressions", np.nan),
                "CPC": daily_insights.get("cpc", np.nan),
                "CPP": daily_insights.get("cpp", np.nan),
                "CTR": daily_insights.get("ctr", np.nan),
            }

            tmp_df_ = pd.DataFrame.from_dict(campaign_insights_meta_ads, orient="index").transpose()
            stats_campaign_df = pd.concat([stats_campaign_df, tmp_df_], axis=0).reset_index(
                drop=True
            )

        return stats_campaign_df

    else:
        return pd.DataFrame()
