# Create Table in Postgres
# CREATE TABLE IF NOT EXISTS marketing_campaign_tracking_google_ads (
#    "Id" TEXT PRIMARY KEY,
#    "CampaignGroupName" TEXT,
#    "CampaignId" TEXT,
#    "CampaignName" TEXT,
#    "CampaignStatus" TEXT,
#    "ChannelAcquisition" TEXT,
#    "MarketCampaign" TEXT,
#    "LanguageCampaign" TEXT,
#    "AdvertisingChannelType" TEXT,
#    "BiddingStrategyType" TEXT,
#    "Currency" TEXT,
#    "Date" DATE,
#    "Spend" FLOAT,
#    "Clicks" INTEGER,
#    "Impressions" INTEGER,
#    "CTR" FLOAT,
#    "CPC" FLOAT,
#    "LastRunPipeline" TIMESTAMP
# )

name_table_output = "marketing_campaign_tracking_google_ads"
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
    "AdvertisingChannelType",
    "BiddingStrategyType",
    "Currency",
    "Date",
    "Spend",
    "Clicks",
    "Impressions",
    "CTR",
    "CPC",
]

query_google_ads_to_export = (
    """
    INSERT INTO """
    + name_table_output
    + """ ("Id", "CampaignGroupName", "CampaignId", "CampaignName",
    "CampaignStatus", "ChannelAcquisition","MarketCampaign", "LanguageCampaign",
    "AdvertisingChannelType", "BiddingStrategyType", "Currency", "Date", "Spend", "Clicks",
    "Impressions", "CTR", "CPC","LastRunPipeline")
    VALUES %s
    ON CONFLICT ("Id")
    DO UPDATE SET
        "CampaignStatus" = EXCLUDED."CampaignStatus",
        "Spend" = EXCLUDED."Spend",
        "Clicks" = EXCLUDED."Clicks",
        "Impressions" = EXCLUDED."Impressions",
        "CTR" = EXCLUDED."CTR",
        "CPC" = EXCLUDED."CPC",
        "LastRunPipeline" = EXCLUDED."LastRunPipeline"
"""
)

cleaning_google_ads_campaign = {
    "Beginner_App_IOS_CH_FR": ["Google App Campaigns", "Switzerland", "French"],
    "Beginner_App_iOS_CH_EN": ["Google App Campaigns", "Switzerland", "English"],
    "Beginner_App_iOS_CH_DE": ["Google App Campaigns", "Switzerland", "German"],
    "Search - CH - Product - Forex - EN": ["Google Search", "Switzerland", "English"],
    "Search - CH - Product - CFD - EN": ["Google Search", "Switzerland", "English"],
    "Search - CH - Competitors - EN": ["Google Search", "Switzerland", "English"],
    "GS-CH-Stocks-en": ["Google Search", "Switzerland", "English"],
    "EAU - EN - Brand - Search - 01/21": ["Google Brand", "Switzerland", "English"],
    "CH - EN - Brand - Search  -  01/21": ["Google Brand", "Switzerland", "English"],
    "CH-EN-STRATEO-FRAIS-DE-TRANSFERT": ["Google Search", "Switzerland", "English"],
    "CH-FR-TRANSFER-FEE": ["Google Search", "Switzerland", "French"],
    "Search - CH - Product - Forex - FR": ["Google Search", "Switzerland", "French"],
    "Search - CH - Product - Forex - DE": ["Google Search", "Switzerland", "German"],
    "Search - CH - Brand - EN": ["Google Search", "Switzerland", "English"],
    "Search - CH - Brand - DE": ["Google Search", "Switzerland", "German"],
    "Search - CH - Brand - FR": ["Google Search", "Switzerland", "French"],
    "Search - CH - Generic Bank - DE": ["Google Search", "Switzerland", "German"],
    "Search - CH - Generic Bank - EN": ["Google Search", "Switzerland", "English"],
    "Search - CH - Generic Bank - FR": ["Google Search", "Switzerland", "French"],
    "Search - CH - Generic Trading - EN": ["Google Search", "Switzerland", "English"],
    "Search - CH - Generic Trading - FR": ["Google Search", "Switzerland", "French"],
    "Search - CH - Generic Trading - DE": ["Google Search", "Switzerland", "German"],
    "Search - CH - Product - Fractional Share - EN": [
        "Google Search",
        "Switzerland",
        "English",
    ],
    "Search - CH - Competitors - FR": ["Google Search", "Switzerland", "French"],
    "Search - CH - Competitors - DE": ["Google Search", "Switzerland", "German"],
    "Search - CH - Professional - DE": ["Google Search", "Switzerland", "German"],
    "Search - CH - Professional - FR": ["Google Search", "Switzerland", "French"],
    "Search - CH - Professional - EN": ["Google Search", "Switzerland", "English"],
    "Search - CH - Product - CFD - FR": ["Google Search", "Switzerland", "French"],
    "Search - CH - Product - CFD - DE": ["Google Search", "Switzerland", "German"],
    "Search - CH - Black Friday - EN": ["Google Search", "Switzerland", "English"],
    "GS-CH-Gold-en": ["Google Search", "Switzerland", "English"],
    "Search - CH - Professional - DE Broad match vs Excat/Phrase  Recommendations Trial": [
        "Google Search",
        "Switzerland",
        "German",
    ],
}
