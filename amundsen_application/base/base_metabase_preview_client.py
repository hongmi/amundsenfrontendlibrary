# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import abc

from flask import Response as FlaskResponse, make_response, jsonify
from http import HTTPStatus
from requests import Response
from typing import Dict

from amundsen_application.base.base_preview_client import BasePreviewClient
from amundsen_application.models.preview_data import ColumnItem, PreviewData, PreviewDataSchema





class BaseMetabasePreviewClient(BasePreviewClient):
    @abc.abstractmethod
    def __init__(self) -> None:
        self.headers = {}  # type: Dict

    @abc.abstractmethod
    def login(self) -> int:
        pass

    @abc.abstractmethod
    def post_to_sql_json(self, *, params: Dict, headers: Dict) -> Response:
        """
        Returns the post response from Superset's `sql_json` endpoint
        """
        pass  # pragma: no cover

    def convert_metabase_datatype_to_hive(self, metabase_type: str):
        type_map = {
            'type/Text': 'string',
            'type/Datetime': 'timestamp',
            'type/Integer': 'int',
            'type/BigInteger': 'bigint',
            'type/Boolean': 'boolean',
            'type/Date': 'date',
            'type/Decimal': 'decimal'
        }
        return type_map.get(metabase_type, 'string')

    def make_data_dict(self, columns: list, rows_data: list):
        return [{k:v for k,v in zip(columns, row_data)} for row_data in rows_data]

    def get_preview_data(self, params: Dict, optionalHeaders: Dict = None) -> FlaskResponse:
        """
        Returns a FlaskResponse object, where the response data represents a json object
        with the preview data accessible on 'preview_data' key. The preview data should
        match amundsen_application.models.preview_data.PreviewDataSchema
        """
        try:
            # Clone headers so that it does not mutate instance's state
            headers = dict(self.headers)

            # Merge optionalHeaders into headers
            if optionalHeaders is not None:
                headers.update(optionalHeaders)

            # Request preview data
            response = self.post_to_sql_json(params=params, headers=headers)

            # Verify and return the results
            response_body = response.json()
            response_dict = response_body['data']
            '''
            response_dict = {
                'columns': [
                    {
                        'name': 'col1',
                        'type': 'string'
                    }, {
                        'name': 'col2',
                        'type': 'int'
                    },
                ],
                'data':[{
                    'col1': 'col1_value',
                    'col2': 1
                }]
            }
            '''
            columns = [ColumnItem(c['name'], self.convert_metabase_datatype_to_hive(c['base_type']))
                       for c in response_dict['cols']]
            rows_dict = self.make_data_dict([col.column_name for col in columns], response_dict['rows'])
            preview_data = PreviewData(columns, rows_dict)
            data = PreviewDataSchema().dump(preview_data)[0]
            if response_body['status'] == 'completed':
                payload = jsonify({'preview_data': data})
                return make_response(payload, HTTPStatus.OK)
            else:
                return make_response(jsonify({'preview_data': {}}), HTTPStatus.INTERNAL_SERVER_ERROR)
        except Exception as e:
            return make_response(jsonify({'preview_data': {}}), HTTPStatus.INTERNAL_SERVER_ERROR)
