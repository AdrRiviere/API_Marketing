from prefect import task


@task(name="[APPLE SEARCH] Configure format table output")
def configure_output_format_apple_search(df):
    """
    Create final table of Apple Search before export in database.

    Args:
        df (DataFrame): DataFrame with all campaigns and all insights.

    Returns:
        fg (DataFrame): DataFrame prepared.
    """
    # Format Columns
    df["CampaignId"] = df["CampaignId"].astype(str)
    df["adGroupName"] = df["adGroupName"].astype(str)
    df["Impressions"] = df["Impressions"].astype(int)
    df["Taps"] = df["Taps"].astype(int)
    df["Installs"] = df["Installs"].astype(int)
    df["newDownloads"] = df["newDownloads"].astype(int)
    df["Spend"] = df["Spend"].astype(float)
    df["Currency"] = df["Currency"].astype(str)

    return df
