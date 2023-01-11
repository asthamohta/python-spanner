
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from . import _helpers
from google.cloud import spanner_v1
from google.cloud.spanner_v1.data_types import JsonObject
from google.api_core import datetime_helpers
import decimal
import datetime
import pandas
from google.cloud.spanner_admin_database_v1 import DatabaseDialect
from . import _sample_data
import math

# for referrence
TABLE_NAME = "pandas_spanner_all_types"

SOME_DATE = datetime.date(2011, 1, 17)
SOME_TIME = datetime.datetime(1989, 1, 17, 17, 59, 12, 345612)
NANO_TIME = datetime_helpers.DatetimeWithNanoseconds(1995, 8, 31, nanosecond=987654321)
POS_INF = float("+inf")
NEG_INF = float("-inf")
BYTES_1 = b"Ymlu"
BYTES_2 = b"Ym9vdHM="
NUMERIC_1 = decimal.Decimal("0.123456789")
NUMERIC_2 = decimal.Decimal("1234567890")
JSON_1 = JsonObject(
    {
        "sample_array": [23, 76, 19],
        "sample_boolean": True,
        "sample_float": 7871.298,
        "sample_int": 872163,
        "sample_null": None,
        "sample_string": "abcdef",
    }
)
JSON_2 = JsonObject(
    {"sample_object": {"id": 2635, "name": "Anamika",}},
)
TABLE_NAME_1 = "Functional_Alltypes"
LIVE_ALL_TYPES_COLUMNS = ["pkey",
    "int_value",
    "int_array",
    "bool_value",
    "bool_array",
    "bytes_value",
    "bytes_array",
    "date_value",
    "date_array",
    "float_value",
    "float_array",
    "string_value",
    "string_array",
    "timestamp_value",
    "timestamp_array",
    "numeric_value",
    "numeric_array",
    "json_value",
    "json_array"]
EMULATOR_ALL_TYPES_COLUMNS = LIVE_ALL_TYPES_COLUMNS[:15]
POSTGRES_ALL_TYPES_COLUMNS = LIVE_ALL_TYPES_COLUMNS[:17] + ["jsonb_value", "jsonb_array"]
SPANNER_ALL_TYPES_VALUES = [
    [1, 2, [3,4, None], True, [True, False, None], BYTES_1, [BYTES_1, BYTES_2, None], SOME_DATE, [SOME_DATE, None], 
    1.48729, [3.891723, 1738.3826, None], "Student", ["Student", "Teacher", None], SOME_TIME, [SOME_TIME, NANO_TIME, None],
    NUMERIC_1, [NUMERIC_1, NUMERIC_2, None], JSON_1, [JSON_1, JSON_2, None]],
    [2, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
]

if _helpers.USE_EMULATOR:
    ALL_TYPES_COLUMNS = EMULATOR_ALL_TYPES_COLUMNS
    ALL_TYPES_ROWDATA = [SPANNER_ALL_TYPES_VALUES[0][:15], SPANNER_ALL_TYPES_VALUES[1][:15]]
elif _helpers.DATABASE_DIALECT == "POSTGRESQL":
    ALL_TYPES_COLUMNS = POSTGRES_ALL_TYPES_COLUMNS
    ALL_TYPES_ROWDATA = SPANNER_ALL_TYPES_VALUES
else:
    ALL_TYPES_COLUMNS = LIVE_ALL_TYPES_COLUMNS
    ALL_TYPES_ROWDATA = SPANNER_ALL_TYPES_VALUES

PANDAS_VALID_DTYPES = [
    (['pkey','int_value'],[101, int(math.pow(2,7))-1],'Int8'),
    (['pkey','int_value'],[102, int(math.pow(2,15))-1],'Int16'),
    (['pkey','int_value'],[103, int(math.pow(2,31))-1],'Int32'),
    (['pkey','int_value'],[104, int(math.pow(2,63))-1],'Int64'),
    (['pkey','int_value'],[105, int(math.pow(2,8)-1)],'UInt8'),
    (['pkey','int_value'],[106, int(math.pow(2,16)-1)],'UInt16'),
    (['pkey','int_value'],[107, int(math.pow(2,32)-1)],'UInt32'),
    (['pkey','timestamp_value'],[108, SOME_TIME],'datetime64[ns, UTC]'),
    (['pkey','date_value'],[109, SOME_DATE],'datetime64[ns, UTC]'),
    (['pkey','bool_value'],[110, True],'boolean'),
    (['pkey','float_value'],[111, 1.48729],'float16'),
    (['pkey','float_value'],[112, 3.891723],'float32'),
    (['pkey','float_value'],[113, 1738.3826],'float64'),
]

@pytest.fixture(scope="session")
def pandas_database(shared_instance, database_operation_timeout, database_dialect):
    database_name = _helpers.unique_id("test_pandas", separator="_")

    if database_dialect == DatabaseDialect.POSTGRESQL:
        pandas_database = shared_instance.database(
            database_name,
            database_dialect=database_dialect,
        )

        operation = pandas_database.create()
        operation.result(database_operation_timeout)

        operation = pandas_database.update_ddl(ddl_statements=_helpers.PANDAS_DDL_STATEMENTS)
        operation.result(database_operation_timeout)

    else:
        pandas_database = shared_instance.database(
            database_name,
            ddl_statements=_helpers.PANDAS_DDL_STATEMENTS,
        )

        operation = pandas_database.create()
        operation.result(database_operation_timeout)

    with pandas_database.batch() as batch:
        batch.insert(TABLE_NAME, ALL_TYPES_COLUMNS, ALL_TYPES_ROWDATA)

    yield pandas_database

    pandas_database.drop()


@pytest.fixture(scope="function")
def sessions_to_delete():
    to_delete = []

    yield to_delete

    for session in to_delete:
        session.delete()


@pytest.mark.parametrize(("insertion_data"), PANDAS_VALID_DTYPES)
def test_insert_dataframe_then_read_pandas_all_types(insertion_data, pandas_database):
    with pandas_database.batch() as batch:
        keyset = spanner_v1.KeySet(all_=True)
        batch.delete(TABLE_NAME, keyset)

    columns = insertion_data[0]
    data = [insertion_data[1]]
    datatypes = insertion_data[2]
    dataframe = pandas.DataFrame(data, columns=columns)

    for column in dataframe.columns:
        if column != 'pkey':
            dataframe[column] = dataframe[column].astype(datatypes)

    if 'float' in datatypes:
        data = dataframe.values.tolist()
    pandas_database.insert_or_update_dataframe(TABLE_NAME, dataframe)

    with pandas_database.snapshot() as snapshot:
        result_set = snapshot.execute_sql("SELECT * FROM {table}".format(table=TABLE_NAME))
    
    result_set_data = list(result_set)
    result_set_columns = [column_metadata.name for column_metadata in result_set.fields]
    row = []

    for item in range(len(result_set_columns)):
        if result_set_columns[item] in columns:
            row.append(result_set_data[0][item])

    _sample_data._check_rows_data([row], data)


def test_insert_dataframe_then_read_spanner_all_datatypes(pandas_database):
    with pandas_database.batch() as batch:
        keyset = spanner_v1.KeySet(all_=True)
        batch.delete(TABLE_NAME, keyset)

    dataframe = pandas.DataFrame(ALL_TYPES_ROWDATA, columns = ALL_TYPES_COLUMNS)
    pandas_database.insert_or_update_dataframe(TABLE_NAME, dataframe)

    with pandas_database.snapshot() as snapshot:
        rows = list(snapshot.execute_sql("SELECT * FROM {table}".format(table=TABLE_NAME)))
    _sample_data._check_rows_data(rows,ALL_TYPES_ROWDATA)
    

@pytest.mark.parametrize(("limit"), [(0), (1), (2)])
def test_read_dataframe(limit, pandas_database):
    with pandas_database.snapshot(multi_use=True) as snapshot:
        results = snapshot.execute_sql(
        "Select * from pandas_spanner_all_types limit {limit}".format(limit=limit)
    )
    dataframe = results.to_dataframe()
    assert len(dataframe) == limit
    assert type(dataframe) == pandas.DataFrame
    expected_dtypes = [
        'Int64',
        'Int64',
        'object',
        'boolean',
        'object',
        'object',
        'object',
        'datetime64[ns, UTC]',
        'object',
        'float64',
        'object',
        'string',
        'object',
        'datetime64[ns, UTC]',
        'object',
        'object',
        'object',
        'string',
        'object'
    ]
    actual_dtypes = [dtype.name for dtype in dataframe.dtypes.to_list()]
    for actual, expected in zip(actual_dtypes, expected_dtypes):
        assert actual == expected
