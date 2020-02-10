Surge
=====

Surge is a client for the BitTorrent network, built on top of Python's
couroutine-based concurrency model and the asyncio event loop. Please note that
Surge is download-only for now.

Installation
------------

Surge needs Python 3.8 with `aiofiles`_ and `aiohttp`_. The recommended way of
installing these dependencies is to use `poetry`_. Run

.. code-block::

    poetry install --no-dev

in the root folder, which automatically gathers the dependencies from the
provided ``pyproject.toml``. The flag ``--no-dev`` instructs poetry not to
install the additional packages that are required to run the tests.

.. _aiofiles: https://pypi.org/project/aiofiles/
.. _aiohttp: https://pypi.org/project/aiohttp/
.. _poetry: https://python-poetry.org/

Usage
-----

Surge has a minimal command-line interface for downloading a single torrent,
located in the ``__main__`` module. Inside of a ``poetry shell`` or another
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

**Help page:**

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


Architecture
------------

Features
~~~~~~~~

Concurrency:
    Surge is designed to connect to many peers at the same time.

Availability tracking:
    Surge keeps track of which pieces its peers have, enabling it to make
    successful requests.

Endgame mode:
    Surge requests the last few pieces from every available peer, so that
    a handful of slow peers cannot stall the download.

Incremental writes:
    Surge writes pieces to the file system immediately after downloading and
    verifing them, keeping its memory usage low.

Request queuing:
    Surge keeps open multiple requests from each peer, improving network
    performance.

Actor model
~~~~~~~~~~~

Peers are modeled as actors that exchange messages with a central mediator. The
strong encapsulation provided by this actor model makes it easy to deal with
unresponsive or malicious peers.

For details about the implementation of the actor model, see the documentation
of the ``actor`` module.
