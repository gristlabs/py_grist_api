grist_api
=========

.. image:: https://img.shields.io/pypi/v/grist_api.svg
    :target: https://pypi.python.org/pypi/grist_api/
.. image:: https://img.shields.io/pypi/pyversions/grist_api.svg
    :target: https://pypi.python.org/pypi/grist_api/
.. image:: https://travis-ci.org/gristlabs/py_grist_api.svg?branch=master
    :target: https://travis-ci.org/gristlabs/py_grist_api
.. image:: https://readthedocs.org/projects/py_grist_api/badge/?version=latest
    :target: https://py-grist-api.readthedocs.io/en/latest/index.html

.. Start of user-guide

The ``grist_api`` module is a Python client library for interacting with Grist.

Installation
------------
grist_api is available on PyPI: https://pypi.python.org/pypi/grist_api/::

    pip install grist_api

The code is on GitHub: https://github.com/gristlabs/py_grist_api.

The API Reference is here: https://py-grist-api.readthedocs.io/en/latest/grist_api.html.

Usage
-----

See ``tests/test_grist_api.py`` for usage examples.  A simple script to add
some rows to a table and then fetch all cells in the table could look like:

.. code-block:: python

    from grist_api import GristDocAPI
    import os

    SERVER = "https://subdomain.getgrist.com"         # your org goes here
    DOC_ID = "9dc7e414-2761-4ef2-bc28-310e634754fb"   #  document id goes here

    # Get api key from your Profile Settings, and run with GRIST_API_KEY=<key>
    api = GristDocAPI(DOC_ID, server=SERVER)

    # add some rows to a table
    rows = api.add_records('Table1', [
        {'food': 'eggs'},
        {'food': 'beets'}
    ])

    # fetch all the rows
    data = api.fetch_table('Table1')
    print(data)


Tests
-----
Tests are in the ``tests/`` subdirectory. To run all tests, run::

    nosetests
