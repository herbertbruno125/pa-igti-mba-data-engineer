import datetime
import time

import dateutil.relativedelta
import pandas as pd
import requests

TOKEN = '6bb4f0cf0cac856708ba7a9aabd0886c'

if __name__ == '__main__':
    df = pd.read_json('/home/herbert/Documents/projeto_aplicado/inputs/ibge_trat/municipality.json',
                      orient='records', lines=True)

    reference_month = datetime.date.today() - dateutil.relativedelta.relativedelta(months=7)
    reference_month_str = reference_month.strftime('%Y%m')

    codes = list(df['code'])

    URL = "https://api.portaldatransparencia.gov.br/api-de-dados/bpc-por-municipio?codigoIbge={}&mesAno={}&pagina=1"
    headers = {'chave-api-dados': "{}".format(TOKEN)}
    records_list = list()
    count = 0
    for cod_city in codes:
        count += 1
        print(f'run for {cod_city} number {count} from {len(codes)}')
        _URL = URL.format(cod_city, reference_month_str)
        response = requests.get(_URL, headers=headers)
        data = response.json()

        if len(data) == 0:
            continue

        records_list.append(data[0])

        if count == 680:
            time.sleep(60)
            count = 0

    df = pd.DataFrame(records_list)
    base_write = 's3a://pa-igti-mba-data-engineer-staging-zone'
    prefix_write = 'ibge/inss'
    date_write = f'{reference_month.year}/{reference_month.strftime("%m")}'
    name_write = f'bpc-inss-{reference_month}.json'
    df.to_json(f'{base_write}/{date_write}/{prefix_write}/{name_write}', orient='records', lines=True)
