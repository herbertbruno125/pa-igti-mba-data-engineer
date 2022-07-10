import io

import pandas as pd
from pandas import DataFrame
from io import BytesIO, StringIO
import boto3

s3_client = boto3.client('s3',
                         aws_access_key_id='',
                         aws_secret_access_key='',
                         region_name='us-east-2')


def __add_input() -> DataFrame:
    print('Add input file to filter')
    resp = s3_client.get_object(Bucket='dcb-staging-zone', Key='de-para-ibge/municipality.json')
    df = pd.read_json(io.BytesIO(resp['Body'].read()), lines=True)
    return df


def __extract_initials():
    resp = s3_client.get_object(Bucket='dcb-staging-zone', Key='estados_atendidos/estados_atendidos.json')
    df = pd.read_json(io.BytesIO(resp['Body'].read()), lines=True)
    res = list(df['sigla'])
    return res


def __filter_rules(df, states):
    print('Filter city codes')
    df = df[df['uf'].isin(states)]
    return df


def __write_records(df):
    print('Write File')
    df = df[['code', 'uf', 'name']]
    json_buffer = io.StringIO()
    df.to_json(json_buffer, orient='records')
    s3_client.put_object(Body=json_buffer.getvalue(),
                         Bucket='dcb-trusted-zone',
                         Key='cidades-estados/atendidos/municipality.json')


def lambda_handler(event, context):
    df = __add_input()
    filter_df: DataFrame = __filter_rules(df, __extract_initials())
    __write_records(filter_df)
