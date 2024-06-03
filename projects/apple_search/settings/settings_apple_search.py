# Create Table in Postgres
# CREATE TABLE IF NOT EXISTS marketing_campaign_tracking_apple_search (
#    "Id" TEXT PRIMARY KEY,
#    "CampaignGroupName" TEXT,
#    "CampaignId" TEXT,
#    "CampaignName" TEXT,
#    "CampaignStatus" TEXT,
#    "ChannelAcquisition" TEXT,
#    "MarketCampaign" TEXT,
#    "LanguageCampaign" TEXT,
#    "Date" DATE,
#    "Spend" FLOAT,
#    "Impressions" INTEGER,
#    "Taps" INTEGER,
#    "Installs" INTEGER,
#    "newDownloads" INTEGER,
#    "CTR" FLOAT,
#    "CPC" FLOAT,
#    "Currency" TEXT,
#    "LastRunPipeline" TIMESTAMP
# )

name_table_output = "marketing_campaign_tracking_apple_search"
environment = "production"
database = "Postgres"
option = "upsert"


columns_output = [
    "Id",
    "CampaignGroupName",
    "CampaignId",
    "CampaignName",
    "CampaignStatus",
    "ChannelAcquisition",
    "MarketCampaign",
    "LanguageCampaign",
    "Date",
    "Spend",
    "Impressions",
    "Taps",
    "Installs",
    "newDownloads",
    "CTR",
    "CPC",
    "Currency",
]

query_apple_search_to_export = (
    """
    INSERT INTO """
    + name_table_output
    + """ ("Id", "CampaignGroupName", "CampaignId", "CampaignName", "CampaignStatus",
     "ChannelAcquisition", "MarketCampaign", "LanguageCampaign", "Date", "Spend", "Impressions",
     "Taps", "Installs", "newDownloads", "CTR", "CPC", "Currency", "LastRunPipeline")
    VALUES %s
    ON CONFLICT ("Id")
    DO UPDATE SET
        "Spend" = EXCLUDED."Spend",
        "Impressions" = EXCLUDED."Impressions",
        "Taps" = EXCLUDED."Taps",
        "Installs" = EXCLUDED."Installs",
        "newDownloads" = EXCLUDED."newDownloads",
        "CTR" = EXCLUDED."CTR",
        "CPC" = EXCLUDED."CPC",
        "LastRunPipeline" = EXCLUDED."LastRunPipeline"
"""
)
