"""Criteo/Kelkoo Cost Uploader

This script downloads cost/click/impression data from the
Criteo & Kelkoo APIs. It reads returned data as dataframes,
generates necessary data points, restructures and reformats.
It then clears the target Google Sheet (Manual Cost Uploader)
and uploads the merged dataframes to the sheet.

This script requires installation of 'gspread', 'oauth2client'
and 'pandas'.
"""

import datetime as dt
import json
from io import StringIO
import sys

import gspread
import pandas as pd
import requests
from oauth2client.service_account import ServiceAccountCredentials

import settings


def prep_sheet():
    """Google sheet authorisation."""
    gsheet_creds = json.loads((settings.GSHEET), strict=False)
    scope = ["https://www.googleapis.com/auth/drive"]

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(gsheet_creds, scope)

    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(settings.GSHEET_KEY)
    worksheet = spreadsheet.get_worksheet(0)
    worksheet.batch_clear(["A:J"])
    return worksheet



def __criteo_get_auth():
    """Criteo API authorisation"""
    auth_url = "https://api.criteo.com/oauth2/token"

    auth_payload = (
        f"grant_type=client_credentials&client_id={settings.CRITEO_CLIENT_ID}&"
        f"client_secret={settings.CRITEO_SECRET}")
    auth_headers = {
        "Accept": "text/plain",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    try:
        auth_response = requests.post(auth_url, data=auth_payload,
                                      headers=auth_headers, timeout=120)
        json_auth = json.loads(auth_response.text)
        return json_auth["access_token"]
    except Exception as e:
        print('Auto Cost Uploader has failed - Criteo API connection error.')
        raise e


def __criteo_get_csv(token, payload_edits):
    """Generate data from Criteo API request:

    * Set endpoint parameters
    * Set template paylod with edit options
    * Generate CSV report
    """
    start_date = (dt.date.today() - dt.timedelta(days=2)).isoformat()
    end_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    timezone = dt.datetime.now(dt.timezone.utc).astimezone().tzinfo

    # STATS REPORT - CRITEO
    url = "https://api.criteo.com/2022-04/statistics/report"

    headers = {
        "Accept": "text/plain",
        "Content-Type": "application/*+json",
        "Authorization": f"Bearer {token}"
    }

    payload = {
        "dimensions": [
            "Advertiser",
            "Day",
            "Device"
        ],
        "metrics": [
            "Displays",
            "Clicks",
            "AdvertiserCost"
        ],
        "timezone": f"{timezone}",
        "format": "CSV",
        "startDate": f"{start_date}",
        "endDate": f"{end_date}",
        **payload_edits
    }

    response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=120)
    return response.content.decode('utf-8-sig')


def __criteo_create_dataframes(stats, cols):
    """Build initial dataframe from CSV data."""
    criteo_df = pd.read_csv(StringIO(stats), sep=';').drop(columns=['AdvertiserId'])
    criteo_df = criteo_df[criteo_df['Device'].notna()]
    criteo_df.rename(
        columns=cols,
        inplace=True
    )
    return criteo_df


def __criteo_merge_and_format_dataframes(df_usd, df_eur, df_gbp):
    """Merge, reformat and restructure 3 dataframes"""
    df_merged = pd.merge(df_usd, df_eur,
                         on=['Advertiser', 'Day', 'Device'], how='outer')
    df_merged = pd.merge(df_merged, df_gbp,
                         on=['Advertiser', 'Day', 'Device'], how='outer')

    if df_merged['Advertiser'].dropna().empty:
        return None

    df_merged['billingcurrency'] = df_merged['billingcurrency_gbp'].fillna(
        df_merged['billingcurrency_eur']).fillna(df_merged['billingcurrency'])
    df_merged['billingcost'] = df_merged['billingcost_gbp'].fillna(
        df_merged['billingcost_eur']).fillna(df_merged['costusd'])

    df_merged['engine'] = 'Criteo'
    df_merged['channel'] = 'Retargeting'

    df_merged.rename(columns={
        'Day': 'date',
        'Device': 'device',
        'Displays': 'impressions',
        'Clicks': 'clicks',
        'Advertiser': 'majormarket'
    }, inplace=True)
    df_merged['majormarket'].replace(settings.CRITEO_MARKET_REPLACEMENTS, inplace=True)

    df_merged['device'] = df_merged['device'].replace({
        'Desktop': 'desktop',
        'Tablet': 'tablet',
        'CTV': 'unknown',
        'Other': 'unknown',
        'Smartphone': 'mobile',
        'Unknown': 'unknown'
    })
    final_df = df_merged.groupby(['date',
                                  'device',
                                  'majormarket',
                                  'channel'],
                                 as_index=False
                                 ).agg({'impressions': 'sum',
                                        'clicks': 'sum',
                                        'billingcost': 'sum',
                                        'billingcurrency': 'max',
                                        'costusd': 'sum',
                                        'engine': 'max'}
                                       )
    final_df = final_df[['date',
                         'device',
                         'impressions',
                         'clicks',
                         'billingcost',
                         'billingcurrency',
                         'costusd',
                         'engine',
                         'majormarket',
                         'channel']
                        ].round(decimals=2)
    return final_df


def criteo_build_final_dataframe():
    """Build final Criteo dataframe for upload:

    * Generate authorisation token
    * Create payloads for each currency
    * Build dataframes for each currency
    * Merge dataframes together and reformat
    """
    token = __criteo_get_auth()

    usd_edits = {
        "metrics": ["Displays", "Clicks", "AdvertiserCost"],
        "currency": "USD"}
    stats_usd = __criteo_get_csv(token, usd_edits)

    gbp_edits = {
        "advertiserIds": settings.CRITEO_GBP_IDS,
        "metrics": ["AdvertiserCost"],
        "currency": "GBP"}
    stats_gbp = __criteo_get_csv(token, gbp_edits)

    eur_edits = {
        "advertiserIds": settings.CRITEO_EUR_IDS,
        "metrics": ["AdvertiserCost"],
        "currency": "EUR"}
    stats_eur = __criteo_get_csv(token, eur_edits)

    # CREATE DATAFRAMES
    columns_usd = {
        'AdvertiserCost': 'costusd',
        'Currency': 'billingcurrency',
    }
    df_usd = __criteo_create_dataframes(stats_usd, columns_usd)

    columns_eur = {
        'AdvertiserCost': 'billingcost_eur',
        'Currency': 'billingcurrency_eur',
    }
    df_eur = __criteo_create_dataframes(stats_eur, columns_eur)

    columns_gbp = {
        'AdvertiserCost': 'billingcost_gbp',
        'Currency': 'billingcurrency_gbp',
    }
    df_gbp = __criteo_create_dataframes(stats_gbp, columns_gbp)
    return __criteo_merge_and_format_dataframes(df_usd, df_eur, df_gbp)


def __fixer_get_conversion_rate():
    """Generate latest GBP > USD conversion rate."""
    url_fx = "https://api.apilayer.com/fixer/latest?symbols=USD&base=GBP"

    payload_fx = {}
    headers_fx = {
        "apikey": f"{settings.FIXER_KEY}"
    }

    response_fx = requests.request("GET", url_fx, headers=headers_fx, data=payload_fx, timeout=120)

    result = response_fx.json()
    return result['rates']['USD']


def __kelkoo_get_json():
    """Generate data from Kelkoo API request:

    * Set endpoint parameters
    * Generate JSON report
    """
    start_date = (dt.date.today() - dt.timedelta(days=2)).isoformat()
    end_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    campaign_id = settings.KELKOO_CAMPAIGN_ID

    url = ("https://api.kelkoogroup.net/merchant/statistics/v1/category/"
           f"{campaign_id}?startDate={start_date}&endDate={end_date}")
    token = f"{settings.KELKOO_TOKEN}"
    headers = {
        "Accept": "text/plain",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers, timeout=120)
    if response.status_code != 200:
        print("Auto Cost Uploader has failed - Kelkoo API connection error")
        sys.exit()
    else:
        return response.text


def __kelkoo_create_dataframe():
    """Build initial dataframe from CSV data."""
    kelkoo_json_data = __kelkoo_get_json()
    if kelkoo_json_data == '[]':
        return None
    kelkoo_df = pd.read_json(kelkoo_json_data)
    kelkoo_df['date'] = kelkoo_df['date'].astype(str)
    return kelkoo_df


def __kelkoo_format_and_group_dataframe(usd_rate):
    """Reformat and group data:

    * Rename columns
    * Drop unwanted columns
    * Standardise device labels for grouping
    * Group data by date & device
    * Create new columns
    """
    kelkoo_df = __kelkoo_create_dataframe()
    if kelkoo_df is None:
        return None

    kelkoo_df.rename(
        columns={
            'cost': 'billingcost',
            'currency': 'billingcurrency',
            'deviceType': 'device'},
        inplace=True)
    kelkoo_df.drop(
        columns=[
            'catId',
            'catName',
            'sales',
            'orderValue',
            'trackedLeads',
            'costTrackedLeads'],
        inplace=True)
    kelkoo_df['device'] = kelkoo_df['device'].replace({
        'Computer': 'desktop',
        'Desktop': 'desktop',
        'Mobile': 'mobile',
        'Tablet': 'tablet',
        'CTV': 'unknown',
        'Other': 'unknown',
        'Smartphone': 'mobile',
        'Unknown': 'unknown'
    })

    df_grouped = kelkoo_df.groupby(['date', 'device']).agg(
        {'billingcurrency': 'max', 'clicks': 'sum', 'billingcost': 'sum'})
    df_grouped = df_grouped.reset_index()

    df_grouped['impressions'] = 0
    df_grouped['costusd'] = df_grouped['billingcost'] * usd_rate
    df_grouped = df_grouped[['date', 'device', 'impressions', 'clicks',
                            'billingcost', 'billingcurrency', 'costusd']].round(decimals=2)
    df_grouped['engine'] = 'Kelkoo'
    df_grouped['majormarket'] = 'UK'
    df_grouped['channel'] = 'Affiliate'
    return df_grouped


def kelkoo_build_final_dataframe():
    """Generate USD rate; build final grouped dataframe."""
    usd_conversion_rate = __fixer_get_conversion_rate()
    kelkoo_df = __kelkoo_format_and_group_dataframe(usd_conversion_rate)
    if kelkoo_df is None:
        return None
    return kelkoo_df


def merge_dataframes(criteo_df, kelkoo_df):
    """Concat Criteo & Kelkoo dataframes, rename devices."""
    if criteo_df is None and kelkoo_df is None:
        print("Auto Cost Uploader has failed - no data provided.")
        sys.exit()
    if criteo_df is None:
        return kelkoo_df
    if kelkoo_df is None:
        return criteo_df
    return pd.concat([criteo_df, kelkoo_df])


def gsheet_upload(worksheet, merged_dataframe):
    """Update Google sheet with merged dataframe values"""
    worksheet.update(
        [merged_dataframe.columns.values.tolist()] + merged_dataframe.values.tolist(), value_input_option="USER_ENTERED"
    )


def sheet_upload():
    """Run all functions:

    * Generate final dataframes
    * Merge dataframes together
    * Clear target sheet
    * Upload merged dataframe to sheet
    """
    criteo_df = criteo_build_final_dataframe()
    kelkoo_df = kelkoo_build_final_dataframe()
    merged_df = merge_dataframes(criteo_df, kelkoo_df)
    worksheet = prep_sheet()
    gsheet_upload(worksheet, merged_df)
    print("Finished - check sheet.")


sheet_upload()
