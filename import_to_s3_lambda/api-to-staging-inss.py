import datetime
import io
import time

import boto3
import dateutil.relativedelta
import pandas as pd
import requests

TOKEN = ''

s3_client = boto3.client('s3',
                         aws_access_key_id='',
                         aws_secret_access_key='',
                         region_name='us-east-2')

reference_month = datetime.date.today() - dateutil.relativedelta.relativedelta(months=9)
reference_month = reference_month.replace(day=1)
reference_month_str = reference_month.strftime('%Y%m')

name_write = f'bpc-inss-{reference_month}.json'

URL = "https://api.portaldatransparencia.gov.br/api-de-dados/bpc-por-municipio?codigoIbge={}&mesAno={}&pagina=1"
headers = {'chave-api-dados': "{}".format(TOKEN)}
records_list = list()


def cities_served():
    resp = s3_client.get_object(Bucket='dcb-trusted-zone', Key='cidades-estados/atendidos/municipality.json')
    df = pd.read_json(io.BytesIO(resp['Body'].read()))
    codes = list(df['code'])
    return codes


def lambda_handler(event, context):
    codes = cities_served()
    count = 0
    time.sleep(60)
    for cod_city in codes:
        count += 1
        print(f'run for {cod_city} number {count} from {len(codes)}')
        _URL = URL.format(cod_city, reference_month_str)
        response = requests.get(_URL, headers=headers)
        data = response.json()

        if len(data):
            records_list.append(data[0])

        if count == 600:
            time.sleep(60)

    df = pd.DataFrame(records_list)
    json_buffer = io.StringIO()
    df.to_json(json_buffer, orient='records', lines=True)
    print(f'Write file: s3://dcb-staging-zone/ibge/inss/{name_write}')
    s3_client.put_object(Body=json_buffer.getvalue(),
                         Bucket='dcb-staging-zone',
                         Key=f'ibge/inss/{name_write}')


if __name__ == '__main__':
    lambda_handler('', '')
