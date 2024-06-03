import logging

import requests
from prefect import task


@task(name="[LINKEDIN ADS] Token Negotiation")
def token_negotiation_linkedin_ads(client_id, client_secret, refresh_token):
    """
    Token Negotiation with LinkedIn Ads.

    Args:
        client_id (str): Client ID provided by LinkedIn.
        client_secret (str): Client Secret provided by LinkedIn.
        refresh_token (str): Refresh Token provided by LinkedIn.

    Returns:
        access_token (str): Valid Access Token for LinkedIn Ads.
    """

    # Define access
    response = requests.post(
        "https://www.linkedin.com/oauth/v2/accessToken",
        params={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )

    if response.status_code == 200:
        logging.info("[LINKEDIN ADS] Token Negotiation LinkedIn Ads Succeed\n")
        refreshed_token_data = response.json()
        access_token = refreshed_token_data["access_token"]
        return access_token
    else:
        logging.info("[LINKEDIN ADS] Token Negotiation LinkedIn Ads Failed\n")
        return "Failed"
