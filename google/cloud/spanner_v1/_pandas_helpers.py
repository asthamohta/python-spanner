# Copyright 2021 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Helper module for working with the pandas library."""
import decimal
from google.cloud.spanner_v1 import TypeCode
from google.api_core.exceptions import NotFound
import math

from google.cloud.spanner_v1.data_types import JsonObject

try:
    import os
    os.system("pip install pandas")
    import pandas

    pandas_import_error = None
except ImportError as err:
    pandas = None
    pandas_import_error = err

PANDAS_NULL_TYPES = [pandas._libs.missing.NAType, pandas._libs.tslibs.nattype.NaTType]

ToDataFrameTypeMap = {
    'BOOL': lambda value: value.astype('boolean'),
    'INT64': lambda value: value.astype('Int64'),
    'FLOAT64': lambda value: value.astype('float64'),
    'TIMESTAMP': lambda value: value.astype('datetime64[ns, UTC]'),
    'DATE': lambda value: value.astype('datetime64[ns, UTC]'),
    'STRING': lambda value: value.astype('string'),
    'BYTES': lambda value: value,
    'ARRAY': lambda value: value,
    'STRUCT': lambda value: value,
    'NUMERIC': lambda value: value,
    'JSON': lambda value : value.apply(lambda json_value : json_value.serialize() if type(json_value) == JsonObject else json_value).astype('string'),
    'JSONB': lambda value : value.apply(lambda json_value : json_value.serialize() if type(json_value) == JsonObject else json_value).astype('string'),
    }

InsertDataFrameTypeMap = {
    'BOOL': lambda value: bool(value),
    'INT64': lambda value: int(value),
    'FLOAT64': lambda value: float(value),
    'TIMESTAMP': lambda value: value,
    'DATE': lambda value: value.to_pydatetime().date() if type(value) == pandas._libs.tslibs.timestamps.Timestamp else value,
    'STRING': lambda value: str(value),
    'BYTES': lambda value: bytes(value),
    'NUMERIC': lambda value: decimal(value),
    'JSON': lambda value : value.serialize() if type(value) == JsonObject else value,
    'JSONB': lambda value : value.serialize() if type(value) == JsonObject else value,
    }

def check_pandas_import():
    if pandas is None:
        raise ImportError(
            "The pandas module is required for this method.\n"
            "Try running 'pip3 install pandas'"
        ) from pandas_import_error

def get_column_metadata(schema):
    column_metadata = {}
    for column in schema:
        column_type = TypeCode(column.type_.code).name
        if(column_type == 'ARRAY'):
            column_type += '_'+ TypeCode(column.type_.array_element_type.code).name
        column_metadata[column.name] = column_type
    return column_metadata


def to_dataframe(result_set):
    """This functions converts the query results into pandas dataframe
    :type result_set: :class:`~google.cloud.spanner_v1.StreamedResultSet`
    :param result_set: complete response data returned from a read/query
    :rtype: pandas.DataFrame
    :returns: Dataframe with the help of a mapping dictionary which maps every spanner datatype to a pandas compatible datatype.
    """
    check_pandas_import()
    # Gathering returned rows and column metadata which include column name and type
    data = list(result_set)
    column_metadata = get_column_metadata(result_set.fields)

    # Creating dataframe using rows and column names
    dataframe = pandas.DataFrame(data, columns=column_metadata.keys())

    # Convert columns to appropriate type.
    for column, datatype in column_metadata.items():
        dataframe[column] = _convert_datatype(dataframe[column], datatype, "SELECT")

    return dataframe

def insert_or_update_dataframe(database, table_name, dataframe):
    # check_pandas_import()
    column_names = list(dataframe.columns)
    data = dataframe.values.tolist()

    column_metadata = get_column_metadata(database.table(table_name).schema)
    # Convert NULL 
    for row in range(len(data)):
        for column in range(len(data[row])):
            data[row][column] = _convert_datatype(data[row][column], column_metadata[column_names[column]], "INSERTION")
    try :
        with database.batch() as batch:
            batch.insert(
                table = table_name,
                columns= column_names,
                values = data,
            )
    except NotFound as exc:
        if "Column not found" in exc:
            exc.message = "Columns from dataframe did not match table column. " + exc.message
        raise exc

def _convert_datatype(value, datatype, operation_type):
    if operation_type == "INSERTION":
        if type(value) in PANDAS_NULL_TYPES or type(value) == float and math.isnan(value) or value is None:
            return None
        if 'ARRAY' in datatype:
            for item in range(len(value)):
                try:
                    value[item] = InsertDataFrameTypeMap[datatype.split("_")[1]](value[item])
                except:
                    ValueError("Coloud not convert {value} to datatype {datatype}".format(
                        value = value[item], datatype = datatype.split("_")[1]))
            return value
        else:
            try:
                return InsertDataFrameTypeMap[datatype](value)
            except:
                ValueError("Coloud not convert {value} to datatype {datatype}".format(value = value, datatype = datatype))
    if 'ARRAY' in datatype:
            datatype = 'ARRAY'
    return ToDataFrameTypeMap[datatype](value)
