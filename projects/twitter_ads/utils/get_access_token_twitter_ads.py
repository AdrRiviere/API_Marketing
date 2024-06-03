from prefect import task
from twitter_ads.client import Client


@task(name="[TWITTER ADS] Connect Account")
def connect_account_twitter_ads(
    account_id, consumer_key, consumer_secret, access_token, access_token_secret
):
    """
    Account connection with Twitter Ads.

    Args:
        account_id (str): Account Id provided by Twitter Ads.
        consumer_key (str): Consumer Key provided by Twitter Ads.
        consumer_secret (str): Consumer Secret provided by Twitter Ads.
        access_token (str): Access Token provided by Twitter Ads.
        access_token_secret (str): Access Token Secret provided by Twitter Ads.

    Returns:
        access_token (str): Valid Access Token for Google Ads.
    """

    # Initialize the client
    client = Client(consumer_key, consumer_secret, access_token, access_token_secret)

    # load the advertiser account instance
    account = client.accounts(account_id)

    return account
