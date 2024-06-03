import logging

import requests
from prefect import task


@task(name="[GOOGLE ADS] Token Negotiation")
def token_negotiation_google_ads(client_id, client_secret, refresh_token):
    """
    Token Negotiation with Google Ads.

    Args:
        client_id (str): Client ID provided by Google Developer.
        client_secret (str): Client Secret provided by Google Developer.
        refresh_token (str): Refresh Token provided by Google Developer.

    Returns:
        access_token (str): Valid Access Token for Google Ads.
    """

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }

    response = requests.post("https://accounts.google.com/o/oauth2/token", data=data)

    if response.status_code == 200:
        refreshed_token_data = response.json()
        access_token = refreshed_token_data["access_token"]
        return access_token
    else:
        logging.info("[GOOGLE ADS] Token Negotiation Google Ads Failed\n")
        return "Failed"
