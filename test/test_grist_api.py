# -*- coding: utf-8 -*-
# pylint: disable=no-self-use,missing-docstring,bad-whitespace

# The test is intended to test the behavior of the library, i.e. translating python calls to HTTP
# requests and interpreting the results. But we can only know it's correct by sending these
# requests to an actual Grist instance and checking their effects.
#
# These tests rely on the "vcr" library to record and replay requests. When writing the tests, run
# them with VCR_RECORD=1 environment variables to run against an actual Grist instance. If the
# tests pass, the HTTP requests and responses get recorded in test/fixtures/vcr/. When tests run
# without this environment variables, the requests get matched, and responses get replayed. When
# replaying, we are not checking Grist functionality, only that correct requests get produced, and
# that responses get parsed.
#
# To record interactions with VRC_RECORD=1, you need to use a functional instance of Grist. Upload
# document test/fixtures/TestGristDocAPI.grist to Grist, and set SERVER and DOC_ID constants below
# to point to it. Find your API key, and set GRIST_API_KEY to it.

# Run nosetests with --nologcapture to see logging, and with -s to see print output.

from __future__ import unicode_literals, print_function
from collections import namedtuple
from datetime import date
import logging
import os
import unittest
import requests
from vcr import VCR
from grist_api import GristDocAPI, date_to_ts

SERVER = "http://localhost:8080/o/docs-8"
DOC_ID = "28a446f2-903e-4bd4-8001-1dbd3a68e5a5"
LIVE = bool(os.environ.get("VCR_RECORD", None))

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s')
logging.getLogger("vcr").setLevel(logging.INFO)
logging.getLogger("grist_api").setLevel(logging.INFO)

vcr = VCR(
    cassette_library_dir='test/fixtures/vcr',
    filter_headers=['authorization'],
    # To update recorded requests, remove file, and run with VCR_RECORD=1 env var.
    record_mode="all" if LIVE else "none")

def datets(*args):
  return int(date_to_ts(date(*args)))

initial_data = {
    "Table1": [
      ('id',  'Text_Field', 'Num',  'Date',               'ColorRef', 'ColorRef_Value'),
      (1,     'Apple',      5,      datets(2019, 6, 26),  1,          "RED"),
      (2,     'Orange',     8,      datets(2019, 5, 1),   2,          "ORANGE"),
      (3,     'Melon',      12,     datets(2019, 4, 2),   3,          "GREEN"),
      (4,     'Strawberry', 1.5,    datets(2019, 3, 3),   1,          "RED"),
      ]
    }

class TestGristDocAPI(unittest.TestCase):
  def setUp(self):
    self._grist_api = GristDocAPI(DOC_ID, server=SERVER, api_key=None if LIVE else "unused")

  def assert_data(self, records, expected_with_headers):
    headers = expected_with_headers[0]
    expected = expected_with_headers[1:]
    actual = [tuple(getattr(rec, h) for h in headers) for rec in records]
    self.assertEqual(actual, expected)

  @vcr.use_cassette()
  def test_fetch_table(self):
    # Test the basic fetch_table
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, initial_data["Table1"])

    # Test fetch_table with filters
    data = self._grist_api.fetch_table('Table1', {"ColorRef": 1})
    self.assert_data(data, [
      ('id',  'Text_Field', 'Num',  'Date',               'ColorRef', 'ColorRef_Value'),
      (1,     'Apple',      5,      datets(2019, 6, 26),  1,          "RED"),
      (4,     'Strawberry', 1.5,    datets(2019, 3, 3),   1,          "RED"),
    ])

  @vcr.use_cassette()
  def test_add_delete_records(self):
    data = self._grist_api.add_records('Table1', [
      {"Text_Field": "Eggs", "Num": 2, "ColorRef": 3, "Date": date(2019, 1, 17)},
      {"Text_Field": "Beets", "Num": 2}
    ])
    self.assertEqual(data, [5, 6])

    data = self._grist_api.fetch_table('Table1', {"Num": 2})
    self.assert_data(data, [
      ('id',  'Text_Field', 'Num',  'Date',               'ColorRef', 'ColorRef_Value'),
      (5,     'Eggs',       2,      datets(2019, 1, 17),  3,          "GREEN"),
      (6,     'Beets',      2,      None,                 0,          None),
    ])

    self._grist_api.delete_records('Table1', [5, 6])

    data = self._grist_api.fetch_table('Table1', {"Num": 2})
    self.assert_data(data, [
      ('id',  'Text_Field', 'Num',  'Date',               'ColorRef', 'ColorRef_Value'),
    ])

  @vcr.use_cassette()
  def test_update_records(self):
    self._grist_api.update_records('Table1', [
      {"id": 1, "Num": -5, "Text_Field": "snapple", "ColorRef": 2},
      {"id": 4, "Num": -1.5, "Text_Field": None, "ColorRef": 2},
    ])

    # Note that the formula field gets updated too.
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, [
      ('id',  'Text_Field', 'Num',  'Date',               'ColorRef', 'ColorRef_Value'),
      (1,     'snapple',    -5,     datets(2019, 6, 26),  2,          "ORANGE"),
      (2,     'Orange',     8,      datets(2019, 5, 1),   2,          "ORANGE"),
      (3,     'Melon',      12,     datets(2019, 4, 2),   3,          "GREEN"),
      (4,     None,         -1.5,   datets(2019, 3, 3),   2,          "ORANGE"),
    ])

    # Revert the changes.
    self._grist_api.update_records('Table1', [
      {"id": 1, "Num": 5, "Text_Field": "Apple", "ColorRef": 1},
      {"id": 4, "Num": 1.5, "Text_Field": "Strawberry", "ColorRef": 1},
    ])
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, initial_data["Table1"])

  @vcr.use_cassette()
  def test_update_records_varied(self):
    # Mismatched column sets cause an error.
    with self.assertRaisesRegexp(ValueError, "needs group_if_needed"):
      self._grist_api.update_records('Table1', [
        {"id": 1, "Num": -5, "Text_Field": "snapple"},
        {"id": 4, "Num": -1.5, "ColorRef": 2},
      ])

    # Check that no changes were made.
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, initial_data["Table1"])

    # Try again with group_if_needed flag
    self._grist_api.update_records('Table1', [
      {"id": 1, "Num": -5, "Text_Field": "snapple"},
      {"id": 4, "Num": -1.5, "ColorRef": 2},
    ], group_if_needed=True)

    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, [
      ('id',  'Text_Field', 'Num',  'Date',               'ColorRef', 'ColorRef_Value'),
      (1,     'snapple',    -5,     datets(2019, 6, 26),  1,          "RED"),
      (2,     'Orange',     8,      datets(2019, 5, 1),   2,          "ORANGE"),
      (3,     'Melon',      12,     datets(2019, 4, 2),   3,          "GREEN"),
      (4,     'Strawberry', -1.5,   datets(2019, 3, 3),   2,          "ORANGE"),
    ])

    # Revert the changes.
    self._grist_api.update_records('Table1', [
      {"id": 1, "Num": 5, "Text_Field": "Apple"},
      {"id": 4, "Num": 1.5, "ColorRef": 1},
    ], group_if_needed=True)

    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, initial_data["Table1"])

  @vcr.use_cassette()
  def test_sync_table(self):
    # The sync_table method requires data as objects with attributes, so use namedtuple.
    Rec = namedtuple('Rec', ['name', 'num', 'date'])  # pylint: disable=invalid-name
    self._grist_api.sync_table('Table1', [
      Rec('Apple', 17, date(2020, 5, 1)),
      Rec('Banana', 33, date(2020, 5, 2)),
      Rec('Melon', 28, None)
    ], [
      ('Text_Field', 'name', 'Text'),
    ], [
      ('Num', 'num', 'Numeric'),
      ('Date', 'date', 'Date'),
    ])

    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, [
      ('id',  'Text_Field', 'Num',  'Date',               'ColorRef', 'ColorRef_Value'),
      (1,     'Apple',      17,     datets(2020, 5, 1),   1,          "RED"),
      (2,     'Orange',     8,      datets(2019, 5, 1),   2,          "ORANGE"),
      (3,     'Melon',      28,     None,                 3,          "GREEN"),
      (4,     'Strawberry', 1.5,    datets(2019, 3, 3),   1,          "RED"),
      (5,     'Banana',     33,     datets(2020, 5, 2),   0,          None),
    ])

    # Revert data, and delete the newly-added record.
    self._grist_api.sync_table('Table1', [
      Rec('Apple', 5, date(2019, 6, 26)),
      Rec('Melon', 12, date(2019, 4, 2)),
    ], [
      ('Text_Field', 'name', 'Text'),
    ], [
      ('Num', 'num', 'Numeric'),
      ('Date', 'date', 'Date'),
    ])
    self._grist_api.delete_records('Table1', [5])

    # Check we are back to where we started.
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, initial_data["Table1"])

  @vcr.use_cassette()
  def test_sync_table_with_methods(self):
    # Try sync_table with a method in place of col name, and with omitting types.
    # Types (third member of tuples) may be omitted if values have correct type.
    # TODO should add test cases where value is e.g. numeric for date, and specifying type of
    # "Date" is required for correct syncing.
    self._grist_api.sync_table('Table1', [
      ('Apple', 17, date(2020, 5, 1)),
      ('Banana', 33, date(2020, 5, 2)),
      ('Melon', 28, None)
    ], [
      ('Text_Field', lambda r: r[0]),
    ], [
      ('Num', lambda r: r[1]),
      ('Date', lambda r: r[2]),
    ])

    # check the results
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, [
      ('id',  'Text_Field', 'Num',  'Date',               'ColorRef', 'ColorRef_Value'),
      (1,     'Apple',      17,     datets(2020, 5, 1),   1,          "RED"),
      (2,     'Orange',     8,      datets(2019, 5, 1),   2,          "ORANGE"),
      (3,     'Melon',      28,     None,                 3,          "GREEN"),
      (4,     'Strawberry', 1.5,    datets(2019, 3, 3),   1,          "RED"),
      (5,     'Banana',     33,     datets(2020, 5, 2),   0,          None),
    ])

    # Revert data, and delete the newly-added record.
    self._grist_api.sync_table('Table1', [
      ('Apple', 5, date(2019, 6, 26)),
      ('Melon', 12, date(2019, 4, 2)),
    ], [
      ('Text_Field', lambda r: r[0]),
    ], [
      ('Num', lambda r: r[1]),
      ('Date', lambda r: r[2]),
    ])
    self._grist_api.delete_records('Table1', [5])

    # Check we are back to where we started.
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, initial_data["Table1"])

  @vcr.use_cassette()
  def test_sync_table_with_filters(self):
    Rec = namedtuple('Rec', ['name', 'num', 'date'])  # pylint: disable=invalid-name
    self._grist_api.sync_table('Table1', [
      Rec('Melon', 100, date(2020, 6, 1)),
      Rec('Strawberry', 200, date(2020, 6, 2)),
    ], [
      ('Text_Field', 'name', 'Text'),
    ], [
      ('Num', 'num', 'Numeric'),
      ('Date', 'date', 'Date'),
    ],
    filters={"ColorRef": 1})

    # Note that Melon got added because it didn't exist in the filtered view.
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, [
      ('id',  'Text_Field', 'Num',  'Date',               'ColorRef', 'ColorRef_Value'),
      (1,     'Apple',      5,      datets(2019, 6, 26),  1,          "RED"),
      (2,     'Orange',     8,      datets(2019, 5, 1),   2,          "ORANGE"),
      (3,     'Melon',      12,     datets(2019, 4, 2),   3,          "GREEN"),
      (4,     'Strawberry', 200,    datets(2020, 6, 2),   1,          "RED"),
      (5,     'Melon',      100,    datets(2020, 6, 1),   0,          None),
    ])

    # Revert data, and delete the newly-added record.
    self._grist_api.sync_table('Table1', [
      Rec('Strawberry', 1.5, date(2019, 3, 3)),
    ], [
      ('Text_Field', 'name', 'Text'),
    ], [
      ('Num', 'num', 'Numeric'),
      ('Date', 'date', 'Date'),
    ],
    filters={"ColorRef": 1})
    self._grist_api.delete_records('Table1', [5])

    # Check we are back to where we started.
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, initial_data["Table1"])

  @vcr.use_cassette()
  def test_chunking(self):
    my_range = range(50)

    # Using chunk_size should produce 5 requests (4 of 12 records, and 1 of 2). We can only tell
    # that by examining the recorded fixture in "test/fixtures/vcr/test_chunking" after running
    # with VCR_RECORD=1.
    data = self._grist_api.add_records('Table1', [
      {"Text_Field": "Chunk", "Num": n} for n in my_range
    ], chunk_size=12)
    self.assertEqual(data, [5 + n for n in my_range])

    # Verify data is correct.
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, initial_data['Table1'] + [
      (5 + n, 'Chunk',      n,      None,     0, None)
      for n in my_range
    ])

    # Update data using chunking.
    self._grist_api.update_records('Table1', [
      {"id": 5 + n, "Text_Field": "Peanut Butter", "ColorRef": 2}
      for n in my_range
    ], chunk_size=12)

    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, initial_data['Table1'] + [
      (5 + n, 'Peanut Butter',  n,      None,     2,          'ORANGE')
      for n in my_range
    ])

    # Delete data using chunking.
    self._grist_api.delete_records('Table1', [5 + n for n in my_range],
        chunk_size=12)
    data = self._grist_api.fetch_table('Table1')
    self.assert_data(data, initial_data["Table1"])

  @vcr.use_cassette()
  def test_errors(self):
    with self.assertRaisesRegexp(requests.HTTPError, "Table not found.*Unicorn"):
      self._grist_api.fetch_table('Unicorn')
    with self.assertRaisesRegexp(requests.HTTPError, "ColorBoom"):
      self._grist_api.fetch_table('Table1', {"ColorRef": 1, "ColorBoom": 2})
    with self.assertRaisesRegexp(requests.HTTPError, "Invalid column.*NumX"):
      self._grist_api.add_records('Table1', [{"Text_Field": "Beets", "NumX": 2}])
