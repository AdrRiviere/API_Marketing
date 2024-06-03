from facebook_business.adobjects.user import User
from prefect import task

import projects.meta_ads.settings.settings_meta_ads as psm


@task(name="[META ADS] Get accounts")
def get_accounts_meta_ads():
    """
    Get all accounts Meta Ads available.

    Args:

    Returns:
        list_accounts_meta_campaign (List): List with all accounts available in Meta Ads.
    """

    if psm.account_meta == "All":
        me = User(fbid="me")
        my_ad_accounts = me.get_ad_accounts()

        list_accounts_meta_campaign = []
        for account in my_ad_accounts:
            list_accounts_meta_campaign.append(account["id"])
    else:
        list_accounts_meta_campaign = psm.account_meta_selected

    return list_accounts_meta_campaign
