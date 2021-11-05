#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys, logging
from datetime import datetime

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', \
                    datefmt='%Y-%m-%d %H:%M:%S', filemode='w', level=logging.INFO)

try:
    import psycopg2, yaml, requests
    import psycopg2.extras as pcge
    import xml.etree.ElementTree as et
    from requests.exceptions import HTTPError
except ImportError as err:
    logging.error(f"Error {err} occured in module {__name__} file: {__file__}")
    sys.exit(2)


def read_yaml_config(file: str, section: str) -> dict:
    with open(file, 'r') as yaml_stream:
        fd = yaml.full_load(yaml_stream)
        if section in fd:
            settings = fd[section]
            return settings
        else:
            logging.error(f"Section {section} not find in the file {file}")
            sys.exit(2)


class SQLClient:
    '''Description SQLClient class'''
    def __init__(self,settings: dict) -> None:
        self.settings = settings
        try:
            self.conn = psycopg2.connect(**self.settings)
            self.curs = self.conn.cursor()
            logging.info(f"Established connect to database '{self.settings['database']}' on host '{self.settings['host']}'")
        except psycopg2.DatabaseError as err:
            logging.error(f"Couldn't establish connect to database '{self.settings['database']}' on host '{self.settings['host']}'")
            sys.exit(2)

    def execute_one(self, query: str, returning: bool) -> list:
        try:
            self.curs.execute(query)
            self.conn.commit()
            if returning:
                result = self.curs.fetchall()
                return result
        except psycopg2.DatabaseError as err:
            logging.error(f"Couldn't exequte query {query} because occured error '{err}'")
            sys.exit(2)

    def insert_batch(self, query_string: str, data: list) -> None:
        try:
            pcge.execute_batch(self.curs, query_string, data)
            self.conn.commit()
            logging.info("Batch insert rows successfully completed")
        except psycopg2.DatabaseError as err:
            logging.error(f"Couldn't exequte bulk insert because occured error '{err}'")
            sys.exit(2)

    def close_connection(self) -> None:
        if self.conn is not None:
            self.curs.close()
            self.conn.close()
            logging.info(f"Closed connect to database '{self.settings['database']}'on host '{self.settings['host']}'")


def current_date_format() -> str:
    current_date_req = datetime.today().strftime('%d/%m/%Y')
    return current_date_req


def get_currency_data(url: str) -> str:
    try:
        response = requests.get(url)
        rsp_xml_content = response.text
        return rsp_xml_content
    except Exception as err:
        logging.error(f"Error: '{err}' occurred during get data")
        sys.exit(2)


def parse_xml_content(rsp_xml_content: str) -> list:
    root = et.fromstring(rsp_xml_content)
    data_list = []
    for child in root.findall('Valute'):
        data_str = []
        data_str.append(child.attrib['ID'])
        for title in ['NumCode', 'CharCode', 'Nominal', 'Name', 'Value']:
            if title == 'Nominal':
                data_str.append(int(child.find(title).text))
            elif title == 'Value':
                data_str.append(float(child.find(title).text.replace(',','.')))
            else:
                data_str.append(child.find(title).text)
        data_str = tuple(data_str)
        data_list.append(data_str)
    return data_list


def processing(config: str, section: str, currency_base: str) -> None:
    current_date_req = current_date_format()
    url = f"{currency_base}{current_date_req}"
    rsp_xml_content = get_currency_data(url)
    dbsettings = read_yaml_config(config, section)
    cli = SQLClient(dbsettings)
    query_template = f"INSERT INTO cbrf.currency VALUES(%s, %s, %s, %s, %s, %s)"
    data_list = parse_xml_content(rsp_xml_content)
    cli.insert_batch(query_template, data_list)
    cli.close_connection()


if __name__ == '__main__':
    currency_base = 'http://www.cbr.ru/scripts/XML_daily.asp?date_req='
    config, section = 'config.yaml', 'postgres'
    processing(config, section, currency_base)

'''
0 2 * * *   /home/postgres/scripts/python/cbrf_api/currency_daily.py >> /home/postgres/logs/cbrf_apy/currency_daily/$(date +\%Y\%m\%d)_currency_daily.log 2>&1
'''