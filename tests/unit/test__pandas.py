# Copyright 2023 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import unittest

class Test_convert_datatype(unittest.TestCase):
    VALUES = [
        ([]'BOOL'),
        ('INT64'),
        ('FLOAT64'),
        ('TIMESTAMP'),
        ('DATE'),
        ('STRING'),
        ('BYTES'),
        ('NUMERIC'),
        ('JSON'),
        ('JSONB'),
    ]
    def _callFUT(self, *args, **kw):
        from google.cloud.spanner_v1._pandas_helpers import _convert_datatype

        return _convert_datatype(*args, **kw)

    def test_convert_datatypes_insertion(self):
        base = merge = None
        result = self._callFUT(base, merge)
        self.assertIsNone(result)

    
    def test_convert_datatypes_select(self):
        base = merge = None
        result = self._callFUT(base, merge)
        self.assertIsNone(result)