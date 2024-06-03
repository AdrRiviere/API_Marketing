# Create Table in Postgres
# CREATE TABLE IF NOT EXISTS marketing_campaign_tracking_linkedin_ads (
#    "Id" TEXT PRIMARY KEY,
#    "CampaignGroupId" TEXT,
#    "CampaignGroupName" TEXT,
#    "CampaignGroupStatus" TEXT,
#    "CampaignId" TEXT,
#    "CampaignName" TEXT,
#    "CampaignStatus" TEXT,
#    "ChannelAcquisition" TEXT,
#    "MarketCampaign" TEXT,
#    "LanguageCampaign" TEXT,
#    "OptimizationTargetType" TEXT,
#    "Date" DATE,
#    "Spend" FLOAT,
#    "Clicks" INTEGER,
#    "Impressions" INTEGER,
#    "Likes" INTEGER,
#    "CTR" FLOAT,
#    "CPC" FLOAT,
#    "LastRunPipeline" TIMESTAMP
# )

name_table_output = "marketing_campaign_tracking_linkedin_ads"
environment = "production"
database = "Postgres"
option = "upsert"

columns_output = [
    "Id",
    "CampaignGroupId",
    "CampaignGroupName",
    "CampaignGroupStatus",
    "CampaignId",
    "CampaignName",
    "CampaignStatus",
    "ChannelAcquisition",
    "MarketCampaign",
    "LanguageCampaign",
    "OptimizationTargetType",
    "Date",
    "Spend",
    "Clicks",
    "Impressions",
    "Likes",
    "CTR",
    "CPC",
]

query_linkedin_ads_to_export = (
    """
    INSERT INTO """
    + name_table_output
    + """ ("Id", "CampaignGroupId", "CampaignGroupName", "CampaignGroupStatus",
    "CampaignId", "CampaignName", "CampaignStatus", "ChannelAcquisition",
    "MarketCampaign", "LanguageCampaign",  "OptimizationTargetType",
    "Date", "Spend", "Clicks", "Impressions", "Likes", "CTR", "CPC","LastRunPipeline")
    VALUES %s
    ON CONFLICT ("Id")
    DO UPDATE SET
        "CampaignGroupStatus" = EXCLUDED."CampaignGroupStatus",
        "CampaignStatus" = EXCLUDED."CampaignStatus",
        "Spend" = EXCLUDED."Spend",
        "Clicks" = EXCLUDED."Clicks",
        "Impressions" = EXCLUDED."Impressions",
        "Likes" = EXCLUDED."Likes",
        "CTR" = EXCLUDED."CTR",
        "CPC" = EXCLUDED."CPC",
        "LastRunPipeline" = EXCLUDED."LastRunPipeline"
"""
)
