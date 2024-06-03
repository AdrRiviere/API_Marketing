import logging

import requests
from prefect import task


@task(name="[META ADS] Token Negotiation")
def token_negotiation_meta_ads(app_id, app_secret, refresh_token):
    """
    Token Negotiation with Meta Ads.

    Args:
        app_id (str): App ID provided by Meta Developer.
        app_secret (str): App Secret provided by Meta Developer.
        refresh_token (str): Token provided by Meta Developer.

    Returns:
        access_token (str): Valid Access Token for Meta Ads.
    """

    url = "https://graph.facebook.com/v16.0/oauth/access_token"
    settings = {
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": refresh_token,
    }
    response = requests.get(url, params=settings)

    if response.status_code == 200:
        access_token = response.json()["access_token"]
        return access_token
    else:
        logging.info("[META ADS] Token Negotiation Meta Ads Failed\n")
        return "Failed"
