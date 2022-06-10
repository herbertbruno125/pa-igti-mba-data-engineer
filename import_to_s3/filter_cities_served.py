import pandas as pd
import structlog
from pandas import DataFrame


def extract_initials():
    df = pd.read_json(
        '/home/herbert/Documents/projeto_aplicado/inputs/depara_estados_atendidos/estados_atendidos.json',
        lines=True)
    res = list(df['sigla'])
    return res


class FilterCities:

    def __init__(self) -> None:
        self.input_file = '/home/herbert/Documents/projeto_aplicado/inputs/ibge/municipality.csv'
        self.logger = structlog.get_logger()
        self.write_file = '/home/herbert/Documents/projeto_aplicado/inputs/ibge_trat/municipality.json'

    def __add_input(self) -> DataFrame:
        self.logger.info('Add input file to filter')
        df = pd.read_csv(self.input_file)
        return df

    def __filter_rules(self, df, states):
        self.logger.info('Filter city codes')
        df = df[df['uf'].isin(states)]
        return df

    def __write_records(self, df):
        self.logger.info('Write File')
        df = df[['code', 'uf', 'name']]
        df.to_json(self.write_file, orient='records', lines=True, force_ascii=False)

    def run(self):
        df = self.__add_input()
        filter_df: DataFrame = self.__filter_rules(df, extract_initials())
        self.__write_records(filter_df)


if __name__ == '__main__':
    filter_cities = FilterCities()
    filter_cities.run()
