"""
Client-side library to interact with Grist.

Handling data types. Currently, datetime.date and datetime.datetime objects sent to Grist (with
add_records() or update_records()) get converted to numerical timestamps as expected by Grist.

Dates received from Grist remain as numerical timestamps, and may be converted using ts_to_date()
function exported by this module.
"""

# pylint: disable=wrong-import-position,wrong-import-order,import-error
from future import standard_library
from future.builtins import range, str
from future.utils import viewitems
standard_library.install_aliases()

import datetime
import decimal
import itertools
import json
import logging
import os
import requests
import sys
import time
from collections import namedtuple
from numbers import Number
from urllib.parse import quote_plus

# Set environment variable GRIST_LOGLEVEL=DEBUG for more verbosity, WARNING for less.
log = logging.getLogger('grist_api')

ColSpec = namedtuple('ColSpec', ('gcol', 'ncol', 'gtype'))
def make_colspec(gcol, ncol, gtype=None):
  return ColSpec(gcol, ncol, gtype)


def init_logging():
  if not log.handlers:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s %(name)s %(message)s'))
    log.setLevel(os.environ.get("GRIST_LOGLEVEL", "INFO"))
    log.addHandler(handler)
    log.propagate = False

def get_api_key():
  key = os.environ.get("GRIST_API_KEY")
  if key:
    return key
  key_path = os.path.expanduser("~/.grist-api-key")
  if os.path.exists(key_path):
    with open(key_path, "r") as key_file:
      return key_file.read().strip()
  raise KeyError("Grist API key not found in GRIST_API_KEY env, nor in %s" % key_path)

class GristDocAPI(object):
  """
  Class for interacting with a Grist document.
  """
  def __init__(self, doc_id, api_key=None, server='https://api.getgrist.com', dryrun=False):
    """
    Initialize GristDocAPI with the API Key (available from user settings), DocId (the part of the
    URL after /doc/), and optionally a server URL. If dryrun is true, will not make any changes to
    the doc. The API key, if omitted, is taken from GRIST_API_KEY env var, or ~/.grist-api-key file.
    """
    self._dryrun = dryrun
    self._server = server
    self._api_key = api_key or get_api_key()
    self._doc_id = doc_id

  def call(self, url, json_data=None, method=None, prefix=None):
    """
    Low-level interface to make a REST call.
    """
    if prefix is None:
      prefix = '/api/docs/%s/' % self._doc_id
    data = json.dumps(json_data, sort_keys=True).encode('utf8') if json_data is not None else None
    method = method or ('POST' if data else 'GET')

    while True:
      full_url = self._server + prefix + url
      if self._dryrun and method != 'GET':
        log.info("DRYRUN NOT sending %s request to %s", method, full_url)
        return None
      log.debug("sending %s request to %s", method, full_url)
      resp = requests.request(method, full_url, data=data, headers={
        'Authorization': 'Bearer %s' % self._api_key,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      })
      if not resp.ok:
        # If the error has {"error": ...} content, use the message in the Python exception.
        err_msg = None
        try:
          error_obj = resp.json()
          if error_obj and isinstance(error_obj.get("error"), str):
            err_msg = error_obj.get("error")
            # TODO: This is a temporary workaround: SQLITE_BUSY shows up in messages for a
            # temporary problem for which it's safe to retry.
            if 'SQLITE_BUSY' in err_msg:
              log.warn("Retrying after error: %s", err_msg)
              time.sleep(2)
              continue
        except Exception:   # pylint: disable=broad-except
          pass

        if err_msg:
          raise requests.HTTPError(err_msg, response=resp)
        else:
          raise resp.raise_for_status()
      return resp.json()

  def fetch_table(self, table_name, filters=None):
    """
    Fetch all data in the table by the given name, returning a list of namedtuples with field
    names corresponding to the columns in that table.

    If filters is given, it should be a dictionary mapping column names to values, to fetch only
    records that match.
    """
    query = ''
    if filters:
      query = '?filter=' + quote_plus(json.dumps(
        {k: [to_grist(v)] for k, v in viewitems(filters)}, sort_keys=True))

    columns = self.call('tables/%s/data%s' % (table_name, query))
    # convert columns to rows
    Record = namedtuple(table_name, columns.keys())   # pylint: disable=invalid-name
    count = len(columns['id'])
    values = columns.values()
    log.info("fetch_table %s returned %s rows", table_name, count)
    return [Record._make(v[i] for v in values) for i in range(count)]

  def add_records(self, table_name, record_dicts, chunk_size=None):
    """
    Adds new records to the given table. The data is a list of dictionaries, with keys
    corresponding to the columns in the table. Returns a list of added rowIds.

    If chunk_size is given, we'll make multiple requests, each limited to chunk_size rows.
    """
    if not record_dicts:
      return []

    call_data = []
    for records in chunks(record_dicts, max_size=chunk_size):
      columns = set().union(*records)
      col_values = {col: [to_grist(rec.get(col)) for rec in records] for col in columns}
      call_data.append(col_values)

    results = []
    for data in call_data:
      log.info("add_records %s %s", table_name, desc_col_values(data))
      results.extend(self.call('tables/%s/data' % table_name, json_data=data) or [])
    return results

  def delete_records(self, table_name, record_ids, chunk_size=None):
    """
    Deletes records from the given table. The data is a list of record IDs.
    """
    # There is an endpoint missing to delete records, but we can use the "apply" endpoint
    # meanwhile.
    for rec_ids in chunks(record_ids, max_size=chunk_size):
      log.info("delete_records %s %s records", table_name, len(rec_ids))
      data = [['BulkRemoveRecord', table_name, rec_ids]]
      self.call('apply', json_data=data)

  def update_records(self, table_name, record_dicts, group_if_needed=False, chunk_size=None):
    """
    Update existing records in the given table. The data is a list of dictionaries, with keys
    corresponding to the columns in the table. Each record must contain the key "id" with the
    rowId of the row to update.

    If records aren't all for the same set of columns, then a single-call update is impossible.
    With group_if_needed is set, we'll make multiple calls. Otherwise, will raise an exception.

    If chunk_size is given, we'll make multiple requests, each limited to chunk_size rows.
    """
    groups = {}
    for rec in record_dicts:
      groups.setdefault(tuple(sorted(rec.keys())), []).append(rec)
    if len(groups) > 1 and not group_if_needed:
      raise ValueError("update_records needs group_if_needed for varied sets of columns")

    call_data = []
    for columns, group_records in sorted(groups.items()):
      for records in chunks(group_records, max_size=chunk_size):
        col_values = {col: [to_grist(rec[col]) for rec in records] for col in columns}
        if 'id' not in col_values or None in col_values["id"]:
          raise ValueError("update_records requires 'id' key in each record")
        call_data.append(col_values)

    for data in call_data:
      log.info("update_records %s %s", table_name, desc_col_values(data))
      self.call('tables/%s/data' % table_name, json_data=data, method="PATCH")

  def sync_table(self, table_id, new_data, key_cols, other_cols, grist_fetch=None,
      chunk_size=None, filters=None):
    # pylint: disable=too-many-locals,too-many-arguments
    """
    Updates Grist table with new data, updating existing rows or adding new ones, matching rows on
    the given key columns. (This method does not remove rows from Grist.)

    New data is a list of objects with column IDs as attributes (e.g. namedtuple or sqlalchemy
    result rows).

    Parameters key_cols and other_cols list primary-key columns, and other columns to be synced.
    Each column in these lists must have the form (grist_col_id, new_data_col_id[, opt_type]).
    See make_type() for available types. In place of grist_col_id or new_data_col_id, you may use
    a function that takes a record and returns a value.

    Initial Grist data is fetched using fetch_table(table_id), unless grist_fetch is given, in
    which case it should contain the result of such a call.

    If chunk_size is given, individual requests will be limited to chunk_size rows each.

    If filters is given, it should be a dictionary mapping grist_col_ids from key columns to
    values. Only records matching these filters will be synced.
    """
    key_cols = [make_colspec(*cs) for cs in key_cols]
    other_cols = [make_colspec(*cs) for cs in other_cols]

    def grist_attr(rec, colspec):
      if callable(colspec.gcol):
        return colspec.gcol(rec)
      return make_type(getattr(rec, colspec.gcol), colspec.gtype)

    def ext_attr(rec, colspec):
      if callable(colspec.ncol):
        return colspec.ncol(rec)
      return make_type(getattr(rec, colspec.ncol), colspec.gtype)

    # Maps unique keys to Grist rows
    grist_rows = {}
    for rec in (grist_fetch or self.fetch_table(table_id, filters=filters)):
      grist_rows[tuple(grist_attr(rec, cs) for cs in key_cols)] = rec

    all_cols = key_cols + other_cols

    update_list = []
    add_list = []
    data_count = 0
    filtered_out = 0
    for nrecord in new_data:
      key = tuple(ext_attr(nrecord, cs) for cs in key_cols)
      if filters and any((cs.ncol in filters and ext_attr(nrecord, cs) != filters[cs.ncol])
                         for cs in key_cols):
        filtered_out += 1
        continue

      data_count += 1

      grecord = grist_rows.get(key)
      if grecord:
        changes = [(cs, grist_attr(grecord, cs), ext_attr(nrecord, cs))
            for cs in other_cols
            if grist_attr(grecord, cs) != ext_attr(nrecord, cs)
        ]
        update = {cs.gcol: nval for (cs, gval, nval) in changes}
        if update:
          log.debug("syncing: #%r %r needs updates %r", grecord.id, key,
              [(cs.gcol, gval, nval) for (cs, gval, nval) in changes])
          update["id"] = grecord.id
          update_list.append(update)
      else:
        log.debug("syncing: %r not in grist", key)
        update = {cs.gcol: ext_attr(nrecord, cs) for cs in all_cols}
        add_list.append(update)

    log.info("syncing %s (%d) with %d records (%d filtered out): %d updates, %d new",
      table_id, len(grist_rows), data_count, filtered_out, len(update_list), len(add_list))
    self.update_records(table_id, update_list, group_if_needed=True, chunk_size=chunk_size)
    self.add_records(table_id, add_list, chunk_size=chunk_size)


EPOCH = datetime.datetime(1970, 1, 1)
DATE_EPOCH = EPOCH.date()

# Converts timestamp in seconds to a naive datetime representing UTC.
def ts_to_dt(timestamp):
  return EPOCH + datetime.timedelta(seconds=timestamp)

# Converts datetime to timestamp in seconds.
# Defaults to UTC if dtime is unaware (has no associated timezone).
def dt_to_ts(dtime):
  offset = dtime.utcoffset()
  if offset is None:
    offset = datetime.timedelta(0)
  return (dtime.replace(tzinfo=None) - offset - EPOCH).total_seconds()

# Converts date to timestamp of the UTC midnight in seconds.
def date_to_ts(date):
  return (date - DATE_EPOCH).total_seconds()

# Converts timestamp in seconds to date.
def ts_to_date(timestamp):
  return DATE_EPOCH + datetime.timedelta(seconds=timestamp)

def to_grist(value):
  if isinstance(value, datetime.datetime):
    return value.isoformat()
  if isinstance(value, datetime.date):
    return date_to_ts(value)
  if isinstance(value, decimal.Decimal):
    return float(value)
  return value

def make_type(value, grist_type):
  """
  Convert a value, whether from Grist or external, to a sensible type, determined by grist_type,
  which should correspond to the type of the column in Grist. Currently supported types are:
    Numeric:  empty values default to 0.0
    Text:     empty values default to ""
    Date:     in Grist values are numerical timestamps; in Python, datetime.date.
    DateTime: in Grist values are numerical timestamps; in Python, datetime.datetime.
  """
  if grist_type in ('Text', None):
    return '' if value is None else value
  if grist_type == 'Date':
    return (value.date() if isinstance(value, datetime.datetime)
            else ts_to_date(value) if isinstance(value, Number)
            else value)
  if grist_type == 'DateTime':
    return ts_to_date(value) if isinstance(value, Number) else value
  return value

def desc_col_values(data):
  """
  Returns a human-readable summary of the given TableData object (dict mapping column name to list
  of values).
  """
  rows = 0
  for _, values in viewitems(data):
    rows = len(values)
    break
  return "%s rows, cols (%s)" % (rows, ', '.join(sorted(data.keys())))

def chunks(items, max_size=None):
  """
  Generator to return subsets of items as chunks of size at most max_size.
  """
  if max_size is None:
    yield list(items)
    return
  it = iter(items)
  while True:
    chunk = list(itertools.islice(it, max_size))
    if not chunk:
      return
    yield chunk
