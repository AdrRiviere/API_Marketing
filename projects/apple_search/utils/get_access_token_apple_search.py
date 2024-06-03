import logging

import requests
from prefect import task


@task(name="[APPLE SEARCH] Token Negotiation")
def token_negotiation_apple_search(client_id, client_secret):
    """
    Token Negotiation with Apple Search.

    Args:
        client_id (str): Client ID provided by Apple Ads.
        client_secret (str): Client Secret provided by Apple Ads.

    Returns:
        access_token (str): Valid Access Token for Apple Ads.
    """

    url = (
        """https://appleid.apple.com/auth/oauth2/token?grant_type=client_credentials&client_id="""
        + client_id
        + """&client_secret="""
        + client_secret
        + """ &scope=searchadsorg"""
    )

    headers = {"Host": "appleid.apple.com"}

    response = requests.post(url, headers=headers)

    if response.status_code == 200:
        refreshed_token_data = response.json()
        access_token = refreshed_token_data["access_token"]
        return access_token
    else:
        logging.info("[APPLE SEARCH] Token Negotiation Apple Search Failed\n")
        return "Failed"
