from os import environ

import boto3

DYNAMODB = boto3.resource('dynamodb')


def parse_orbit(orbit_file):
    platform, _, _, product_type, _, ingestion_date, validity_start, validity_end = orbit_file.split('_')
    validity_start = validity_start.lstrip('V')
    validity_end = validity_end.split('.')[0]
    return {
        'orbit_file': orbit_file,
        'platform': platform,
        'product_type': product_type,
        'ingestion_date': ingestion_date,
        'validity_start': validity_start,
        'validity_end': validity_end,
        'url': f'https://{environ["BUCKET_NAME"]}.s3.us-west-2.amazonaws.com/{product_type}/{orbit_file}',
    }


def create_record(orbit_file):
    table = DYNAMODB.Table(environ['TABLE_NAME'])
    item = parse_orbit(orbit_file)
    table.put_item(Item=item)
