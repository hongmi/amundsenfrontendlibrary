# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from http import HTTPStatus

import requests
import uuid
import json

from requests import Response
from typing import Any, Dict  # noqa: F401
from flask import current_app as app

from amundsen_application.base.base_metabase_preview_client import BaseMetabasePreviewClient

# 'main' is an existing default Superset database which serves for demo purposes
DEFAULT_DATABASE_MAP = {
    'main': 1,
}
DEFAULT_URL = 'https://metabase.xunliandata.com'


class MetabasePreviewClient(BaseMetabasePreviewClient):
    def __init__(self,
                 *,
                 database_map: Dict[str, int] = DEFAULT_DATABASE_MAP,
                 url: str = DEFAULT_URL) -> None:
        self.database_map = database_map
        self.headers = {'Content-Type': 'application/json'}
        self.url = url
        self.username = app.config['METABASE_USERNAME']
        self.password = app.config['METABASE_PASSWORD']
        self.metabase_session_id = ''

    def login(self) -> int:
        data = {
            'username': self.username,
            'password': self.password
        }
        r = requests.post(self.url + '/api/session', headers=self.headers, data=json.dumps(data))
        if r.ok:
            self.metabase_session_id = r.json()['id']
            return HTTPStatus.OK

        return r.status_code

    def post_to_sql_json(self, *, params: Dict, headers: Dict) -> Response:
        """
        Returns the post response from Superset's `sql_json` endpoint
        """
        headers = dict(self.headers)
        headers['Cookie'] = 'metabase.SESSION=' + self.metabase_session_id

        # Create the appropriate request data
        try:
            request_data = {
                'database': 0,
                'type': 'native',
                'native': {
                    'query': ''
                }
            }  # type: Dict[str, Any]

            # Superset's sql_json endpoint requires the id of the database that it will execute the query on
            database_id = app.config['METABASE_DATABASE_ID']  # OR params.get('database') in a real use case
            request_data['database'] = database_id

            # Generate the sql query for the desired data preview content
            try:
                # 'main' is an existing default Superset schema which serves for demo purposes
                schema = params.get('schema')  # OR params.get('schema') in a real use case

                # 'ab_role' is an existing default Superset table which serves for demo purposes
                table_name = params.get('tableName')  # OR params.get('tableName') in a real use case

                sql = 'SELECT * FROM {schema}.{table} LIMIT 50'.format(schema=schema, table=table_name)
                # sql = 'select * from default.sample_07 limit 50'

                request_data['native']['query'] = sql
            except Exception as e:
                logging.error('Encountered error generating request sql: ' + str(e))
        except Exception as e:
            logging.error('Encountered error generating request data: ' + str(e))

        # Post request to Superset's `sql_json` endpoint
        return requests.post(self.url + '/api/dataset', data=json.dumps(request_data), headers=headers)
