surge: an asyncio BitTorrent client
===================================

Installation
------------

To run surge you need Python 3.8 with `aiofiles`_ and `aiohttp`_. The
recommended way of installing these dependencies is to use `poetry`_. Run

.. code-block::

    poetry install --no-dev

in the root folder, which automatically gathers the dependencies from the
provided ``pyproject.toml``.

.. _aiofiles: https://pypi.org/project/aiofiles/
.. _aiohttp: https://pypi.org/project/aiohttp/
.. _poetry: https://python-poetry.org/

Usage
-----

There is a minimal command-line interface for downloading a single torrent,
located in ``surge/__main__.py``. Inside of a ``poetry shell`` or another
appropriate environment it can be run as ``python -m surge``.

**Example:**

.. code-block::

    $ python -m surge debian.torrent
    Downloading debian.torrent to download/.
    Got 50 peers from http://bttracker.debian.org:6969/announce.
    Progress: 1/1340 pieces.
    Progress: 2/1340 pieces.
    Progress: 3/1340 pieces.
    (...)
    Progress: 1340/1340 pieces.
    Exiting.

**Complete usage:**

.. code-block::

    $ python -m surge --help
    usage: __main__.py [-h] [--debug] [--resume] file

    Download files from the BitTorrent network.

    positional arguments:
    file        path to the torrent file

    optional arguments:
    -h, --help  show this help message and exit
    --debug     enable logging
    --resume    resume download
