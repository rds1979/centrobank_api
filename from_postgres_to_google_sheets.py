#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os, sys, logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', \
                    datefmt='%Y-%m-%d %H:%M:%S', filemode='w', level=logging.INFO)

try:
    import yaml, psycopg2, httplib2
    import apiclient.discovery
    from oauth2client.service_account import ServiceAccountCredentials
except ImportError as err:
    logging.error(f"Error {err} occured in module '{__name__}', file '{__file__}'")
    sys.exit(2)

CREDENTIALS_FILE = 'storeez--data-0000001-699b8a426ee9.json'
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE,
                                                               ['https://www.googleapis.com/auth/spreadsheets',
                                                                'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = apiclient.discovery.build('sheets', 'v4', http = httpAuth)


class FileManager:
    '''Description FileManager class'''
    def read_text_file(self, file: str) -> list:
        lines = []
        with open(file, 'r') as fd:
            for line in fd:
                line = line.replace("\n", "")
                if "'" in line:
                    line = line.replace("'", ' ')
                lines.append(line)
        return lines

    def read_yaml_file(self, file: str, section: str) -> dict:
        with open(file, 'r') as yaml_stream:
            fd = yaml.full_load(yaml_stream)
            if section in fd:
                settings = fd[section]
                return settings
            else:
                logging.error(f"Section '{section}' not find in the file '{file}'")
                sys.exit(2)


class SQLClient:
    '''Description SQLClient class'''
    def __init__(self, dbsettings: dict) -> None:
        self.settings = dbsettings
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

    def close_connection(self) -> None:
        if self.conn is not None:
            self.curs.close()
            self.conn.close()
            logging.info(f"Closed connect to database '{self.settings['database']}'on host '{self.settings['host']}'")


def processing(file: str, section: str) -> None:
    fm = FileManager()
    dbsettings = fm.read_yaml_file(file, section)
    cli = SQLClient(dbsettings)
    query = f'''SELECT * FROM core.place LIMIT 3'''
    cli.close_connection()


if __name__ == '__main__':
    config, section = 'config.yaml', 'postgres_esb_prod'
    processing(config, section)
