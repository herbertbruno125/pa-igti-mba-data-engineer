import datetime
import io

import boto3
import dateutil.relativedelta
import pandas as pd

s3_client = boto3.client('s3',
                         aws_access_key_id='',
                         aws_secret_access_key='',
                         region_name='us-east-2')

write_bucket = 'dcb-trusted-zone'
__write_name = f'bc-inss-csldo-{datetime.date.today()}.csv'
write_key = f'insight/bc-states/process_date={datetime.date.today()}/{__write_name}'

reference_month = datetime.date.today() - dateutil.relativedelta.relativedelta(months=9)
reference_month = reference_month.replace(day=1)


def extract_municipio(x):
    return x['nomeIBGE']


def extract_regiao(x):
    return str(x['nomeRegiao']).upper()


def extract_uf(x):
    return x['uf']['sigla']


def extract_beneficio(x):
    return x['descricaoDetalhada']


def lambda_handler(event, context):
    file = s3_client.get_object(Bucket='dcb-raw-zone', Key=f'ibge/inss/bpc-inss-{reference_month}.json')
    df = pd.read_json(io.BytesIO(file['Body'].read()), lines=True)

    df['municipio_nome'] = df['municipio'].apply(lambda z: extract_municipio(z))
    df['regiao'] = df['municipio'].apply(lambda z: extract_regiao(z))
    df['uf'] = df['municipio'].apply(lambda z: extract_uf(z))
    df['beneficio'] = df['tipo'].apply(lambda z: extract_beneficio(z))

    df = df[['municipio_nome', 'regiao', 'uf', 'quantidadeBeneficiados', 'beneficio', 'dataReferencia']]

    df.rename(columns={'dataReferencia': 'data_referencia',
                       'municipio_nome': 'municipio',
                       'quantidadeBeneficiados': 'quantidade_beneficiados'}, inplace=True)

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep='|', index=False)

    print(f'Write file: s3://{write_bucket}/{write_key}')

    s3_client.put_object(Body=csv_buffer.getvalue(),
                         Bucket=write_bucket,
                         Key=write_key)

if __name__ == '__main__':
    lambda_handler('', '')
