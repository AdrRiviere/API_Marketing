import numpy as np
from prefect import task

import projects.google_ads.settings.settings_google_ads as m


@task(name="[FEATURES ENGINEERING] Cleaning Campaign Name")
def specific_cleaning_google_ads(df):

    for campaign in m.cleaning_google_ads_campaign.keys():
        df["ChannelAcquisition"] = np.where(
            df["CampaignName"] == campaign,
            m.cleaning_google_ads_campaign[campaign][0],
            df["ChannelAcquisition"],
        )
        df["MarketCampaign"] = np.where(
            df["CampaignName"] == campaign,
            m.cleaning_google_ads_campaign[campaign][1],
            df["MarketCampaign"],
        )
        df["LanguageCampaign"] = np.where(
            df["CampaignName"] == campaign,
            m.cleaning_google_ads_campaign[campaign][2],
            df["LanguageCampaign"],
        )

    return df
