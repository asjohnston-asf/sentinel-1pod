import re
from datetime import datetime, timedelta
from operator import itemgetter
from os import environ
from typing import List

import boto3
from boto3.dynamodb.conditions import Attr, Key

DYNAMODB = boto3.resource('dynamodb')


def parse_s1_name(name: str) -> dict:
    platform = r'S1[AB]'
    beam_mode = r'(IW|EW|WV|S[1-6])'
    product_type = r'(GRD|SLC|RAW|OCN)[_HMF]'
    level = r'[0-2]S'
    polarization = r'(SV|SH|DV|DH|VV|HH|VH|HV)'
    date = r'\d{8}T\d{6}'
    orbit = r'\d{6}'
    data_take = '[0-9A-F]{6}'
    unique_id = r'[0-9A-F]{4}'

    regex = fr'(?P<platform>{platform})_{beam_mode}_{product_type}_{level}{polarization}_(?P<start>{date})_(?P<end>{date})_{orbit}_{data_take}_{unique_id}$'
    match = re.match(regex, name)

    if not match:
        raise ValueError(f'{name} is not a valid Sentinel-1 product')

    return match.groupdict()


def get_window_start(start):
    date_format = '%Y%m%dT%H%M%S'
    start_datetime = datetime.strptime(start, date_format)
    earlier_datetime = start_datetime - timedelta(days=3)
    window_start = earlier_datetime.strftime(date_format)
    return window_start


def get_orbits(platform: str, start: str, end: str) -> List[dict]:
    window_start = get_window_start(start)
    table = DYNAMODB.Table(environ['TABLE_NAME'])
    response = table.query(
        IndexName='platform',
        KeyConditionExpression=Key('platform').eq(platform) & Key('validity_start').between(window_start, start),
        FilterExpression=Attr('validity_end').gte(end),
    )
    items = response['Items']
    items.sort(key=itemgetter('ingestion_date'), reverse=True)
    items.sort(key=itemgetter('product_type'))
    return items


def lambda_handler(event, context):
    path = event['requestContext']['http']['path']
    granule = path.lstrip('/')
    try:
        info = parse_s1_name(granule)
    except ValueError as e:
        return {
            'cookies': [],
            'isBase64Encoded': False,
            'statusCode': 400,
            'headers': {'content-type': 'text/plain'},
            'body': str(e),
        }
    orbits = get_orbits(info['platform'], info['start'], info['end'])
    return orbits
