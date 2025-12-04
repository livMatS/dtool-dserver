dtool-dserver
=============

A dtool storage broker that accesses datasets through a dserver instance
using signed URLs.

This broker allows users to access datasets stored on S3, Azure, or other
backends without needing direct credentials for those backends. Instead,
the dserver generates time-limited signed URLs on behalf of the user.

Features
--------

- Read datasets through dserver signed URLs
- Create new datasets through dserver signed URLs
- Automatic caching of downloaded items
- Support for all dtool operations (ls, cp, info, item fetch, etc.)
- Backend-agnostic (works with any storage backend dserver supports)

Installation
------------

.. code-block:: bash

    pip install dtool-dserver

Configuration
-------------

Environment variables:

- ``DSERVER_TOKEN``: JWT authentication token for dserver
- ``DSERVER_TOKEN_FILE``: Alternative: path to file containing the token
- ``DSERVER_PROTOCOL``: Protocol to use (http or https, default: http)
- ``DSERVER_CACHE_DIR``: Cache directory for downloaded items
  (default: system temp directory)

URI Format
----------

The URI format encodes both the dserver endpoint and the backend storage
location::

    dserver://<server>/<backend>/<bucket_or_container>/<uuid>

Examples:

- ``dserver://localhost:5000/s3/my-bucket/550e8400-e29b-41d4-a716-446655440000``
- ``dserver://dserver.example.com/azure/my-container/dataset-uuid``

Usage
-----

List datasets::

    dtool ls dserver://localhost:5000/s3/my-bucket/

Copy a dataset to local storage::

    dtool cp dserver://localhost:5000/s3/my-bucket/uuid /local/path/

Create a new dataset::

    dtool create dserver://localhost:5000/s3/my-bucket/new-dataset

Authentication
--------------

Obtain a token from dserver using the authentication endpoint::

    curl -X POST http://localhost:5000/token \
         -H "Content-Type: application/json" \
         -d '{"username": "user", "password": "pass"}'

Set the token in your environment::

    export DSERVER_TOKEN=<your_token>

Or save it to a file::

    echo "<your_token>" > ~/.dserver_token
    export DSERVER_TOKEN_FILE=~/.dserver_token
