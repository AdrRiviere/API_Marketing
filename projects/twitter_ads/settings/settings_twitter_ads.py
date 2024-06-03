# Mapping Historic Campaign :
# Create Table in Postgres
# CREATE TABLE IF NOT EXISTS marketing_campaign_tracking_twitter_ads (
#    "Id" TEXT PRIMARY KEY,
#    "CampaignGroupName" TEXT,
#    "CampaignId" TEXT,
#    "CampaignName" TEXT,
#    "ChannelAcquisition" TEXT,
#    "MarketCampaign" TEXT,
#    "LanguageCampaign" TEXT,
#    "Date" DATE,
#    "Currency" TEXT,
#    "Spend" FLOAT,
#    "Clicks" INTEGER,
#    "Impressions" INTEGER,
#    "Likes" INTEGER,
#    "Engagements" INTEGER,
#    "Retweets" INTEGER,
#    "Follows" INTEGER,
#    "Unfollows" INTEGER,
#    "CTR" FLOAT,
#    "CPC" FLOAT,
#    "LastRunPipeline" TIMESTAMP
# )

name_table_output = "marketing_campaign_tracking_twitter_ads"
environment = "production"
database = "Postgres"
option = "upsert"

columns_output = [
    "Id",
    "CampaignGroupName",
    "CampaignId",
    "CampaignName",
    "ChannelAcquisition",
    "MarketCampaign",
    "LanguageCampaign",
    "Date",
    "Currency",
    "Spend",
    "Clicks",
    "Impressions",
    "Likes",
    "Engagements",
    "Retweets",
    "Follows",
    "Unfollows",
    "CTR",
    "CPC",
]

query_twitter_ads_to_export = (
    """
    INSERT INTO """
    + name_table_output
    + """ ("Id", "CampaignGroupName", "CampaignId", "CampaignName", "ChannelAcquisition",
    "MarketCampaign", "LanguageCampaign", "Date", "Currency", "Spend",
    "Clicks", "Impressions", "Likes","Engagements", "Retweets", "Follows",
    "Unfollows", "CTR", "CPC",  "LastRunPipeline")
    VALUES %s
    ON CONFLICT ("Id")
    DO UPDATE SET
        "Spend" = EXCLUDED."Spend",
        "Clicks" = EXCLUDED."Clicks",
        "Impressions" = EXCLUDED."Impressions",
        "Likes" = EXCLUDED."Likes",
        "Engagements" = EXCLUDED."Engagements",
        "Retweets" = EXCLUDED."Retweets",
        "Follows" = EXCLUDED."Follows",
        "Unfollows" = EXCLUDED."Unfollows",
        "CTR" = EXCLUDED."CTR",
        "CPC" = EXCLUDED."CPC",
        "LastRunPipeline" = EXCLUDED."LastRunPipeline"
"""
)

mapping_historic_campaign_twitter = {
    "Enig - Awareness Campaign - CH": ["2021-03-01", "2021-04-30"],
    "Enig - Awareness Campaign - UAE": ["2021-03-01", "2021-04-30"],
    "Enig - Webinar April 21 - CHDE": ["2021-03-01", "2021-04-30"],
    "Enig - Webinar April 21 - DE": ["2021-03-01", "2021-04-30"],
    "Enig - CH - FlowBank Awareness - Acquisition": ["2021-04-01", "2021-06-01"],
    "Enig - CH - FlowBank Awareness - Retargeting": ["2021-04-01", "2021-06-01"],
    "Enig - CH - FlowBank App - Acquisition": ["2021-04-01", "2021-06-01"],
    "Enig - CH - FlowBank Pro - Acquisition": ["2021-04-01", "2021-06-01"],
    "Enig - Webinar - 27 April 2021": ["2021-04-01", "2021-06-01"],
    "Enig - Webinar - 29 April 2021": ["2021-04-01", "2021-06-01"],
    "Enig - Webinar - DE": ["2021-04-01", "2021-06-01"],
    "Enig - Webinar - CHDE": ["2021-05-01", "2021-07-01"],
    "Enig - Webinar - CHEN": ["2021-05-01", "2021-07-01"],
    "Morning Live - Setup (end date ad group to change)": ["2021-05-01", "2021-07-01"],
    "Enig - Morning Live - Video Teaser": ["2021-05-01", "2021-07-01"],
    "Enig - Webinar - CHDE - Monthly invoice": ["2021-05-01", "2021-07-01"],
    "Enig - Morning Live - Video Teaser - Monthly invoice": ["2021-06-01", "2021-08-01"],
    "Morning Live - Setup - Monthly invoicing": ["2021-06-01", "2021-08-01"],
    "FlowBank Pro Features video": ["2021-06-01", "2021-08-01"],
    "Enig - Webinar - CHFR - Monthly invoice": ["2021-06-01", "2021-08-01"],
    "Enig - Webinar - FRFR - Monthly invoice": ["2021-06-01", "2021-08-01"],
    "Mediabros - Webinar DE": ["2021-06-01", "2021-12-01"],
    "Join The Big Names": ["2021-09-01", "2021-10-01"],
    "Zurich Opening": ["2021-10-01", "2021-11-01"],
    "Pro MT4": ["2021-10-01", "2021-12-01"],
    "Pro Forex": ["2021-10-01", "2021-12-01"],
    "Mediabros. Webinar FR": ["2021-10-01", "2021-12-01"],
    "Women in Finance 01": ["2021-11-01", "2021-12-31"],
    "Christmas Free Stock": ["2021-11-01", "2021-12-31"],
    "Women in Finance 03": ["2021-12-01", "2021-12-31"],
    "Quick promote · Revue des marchés…": ["2021-12-01", "2021-12-31"],
    "Quick promote · Revue hebdomadaire": ["2021-12-01", "2021-12-31"],
    "Ichimoku": ["2021-12-01", "2021-12-31"],
    "Winter": ["2021-12-01", "2022-01-31"],
    "Boost - Ichimoku - 21.12": ["2021-12-01", "2021-12-31"],
    "Ichimoku 19.01": ["2022-01-01", "2022-01-31"],
    "Ichimoku - 25.01.2022": ["2022-01-01", "2022-02-08"],
    "Cash Bonus": ["2022-02-01", "2022-02-25"],
    "TW-WW-en-zzzIP_Start_Switch_Trade-0": ["2023-02-01", "2023-02-15"],
    "TW-DE-de-zzzIP_Start_Switch_Trade-0": ["2023-02-01", "2023-02-15"],
    "TW-IT-it-zzzIP_Start_Switch_Trade-0": ["2023-02-01", "2023-02-15"],
    "TW-CH-de-IP_Start_Switch_Trade-0": ["2023-02-01", "2023-03-31"],
    "TW-CH-en-IP_Start_Switch_Trade-0": ["2023-02-01", "2023-03-31"],
    "TW-CH-fr-IP_Start_Switch_Trade-0": ["2023-02-01", "2023-03-31"],
    "TW-IT-it-IP_Start_Switch_Trade-0": ["2023-02-01", "2023-02-15"],
    "TW-DE-de-IP_Start_Switch_Trade-0": ["2023-02-01", "2023-02-15"],
    "TW-WW-en-IP_Start_Switch_Trade-0": ["2023-02-01", "2023-02-15"],
}
