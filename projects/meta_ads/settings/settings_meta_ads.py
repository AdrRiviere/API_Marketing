# Create Table in Postgres
# CREATE TABLE IF NOT EXISTS marketing_campaign_tracking_meta_ads (
#    "Id" TEXT PRIMARY KEY,
#    "CampaignGroupId" TEXT,
#    "CampaignGroupName" TEXT,
#    "CampaignId" TEXT,
#    "CampaignName" TEXT,
#    "CampaignStatus" TEXT,
#    "ChannelAcquisition" TEXT,
#    "MarketCampaign" TEXT,
#    "LanguageCampaign" TEXT,
#    "Objective" TEXT,
#    "BidStrategy" TEXT,
#    "Date" DATE,
#    "Spend" FLOAT,
#    "Clicks" INTEGER,
#    "Impressions" INTEGER,
#    "Reach" INTEGER,
#    "CTR" FLOAT,
#    "CPC" FLOAT,
#    "CPP" FLOAT,
#    "LastRunPipeline" TIMESTAMP
# )

name_table_output = "marketing_campaign_tracking_meta_ads"
environment = "production"
database = "Postgres"
option = "upsert"

account_meta = "All"
# act_1139641553127038 : 14 campaigns - 28 April 2021
# act_1504463063049074 : 554 campaigns - 28 October 2020
# act_1481320182261704 : 53 campaigns - 6 January 2022
# act_486356982826506 : 6 campaigns - 6 January 2022

if account_meta == "Custom":
    account_meta_selected = ["act_1504463063049074"]

columns_output = [
    "Id",
    "CampaignGroupId",
    "CampaignGroupName",
    "CampaignId",
    "CampaignName",
    "CampaignStatus",
    "ChannelAcquisition",
    "MarketCampaign",
    "LanguageCampaign",
    "Objective",
    "BidStrategy",
    "Date",
    "Spend",
    "Clicks",
    "Impressions",
    "Reach",
    "CTR",
    "CPC",
    "CPP",
]

query_meta_ads_to_export = (
    """
    INSERT INTO """
    + name_table_output
    + """ ("Id", "CampaignGroupId", "CampaignGroupName", "CampaignId",
    "CampaignName", "CampaignStatus","ChannelAcquisition", "MarketCampaign",
    "LanguageCampaign", "Objective", "BidStrategy","Date", "Spend", "Clicks",
    "Impressions", "Reach", "CTR", "CPC", "CPP", "LastRunPipeline")
    VALUES %s
    ON CONFLICT ("Id")
    DO UPDATE SET
        "CampaignStatus" = EXCLUDED."CampaignStatus",
        "Spend" = EXCLUDED."Spend",
        "Clicks" = EXCLUDED."Clicks",
        "Impressions" = EXCLUDED."Impressions",
        "Reach" = EXCLUDED."Reach",
        "CTR" = EXCLUDED."CTR",
        "CPC" = EXCLUDED."CPC",
        "CPP" = EXCLUDED."CPP",
        "LastRunPipeline" = EXCLUDED."LastRunPipeline"
"""
)
