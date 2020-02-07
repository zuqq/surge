surge: an asyncio BitTorrent client
===================================

Installation
------------

To run surge you need Python 3.8 with `aiofiles`_ and `aiohttp`_. The
recommended way of installing these dependencies is to use `poetry`_. Run

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


Architecture
------------

Features
~~~~~~~~~~~

**Concurrency**:
    surge is designed to connect to many peers at the same time.

**Availability tracking**:
    surge keeps track of which pieces the peers have, enabling it to make
    successful requests.

**Robustness**:
    surge drops peers that ignore its request or respond with invalid data.

**Endgame mode**:
    surge requests the last few pieces from every available peer, so that
    a handful of slow peers cannot stall the download.

**Incremental writes**:
    surge writes pieces to the filesystem immediately after downloading and
    verifing them.

Actor model
~~~~~~~~~~~

The different components of the program are modeled as actors. For details about
the actor implementation, see the documentation of the ``actor`` module.

What follows is a brief description of the actors defined in the ``torrent`` module.

The global state is held by ``Torrent`` and the following of its children:

.. code-block::

    Torrent
    |
    ├─ FileWriter
    |
    ├─ PeerQueue
    │
    ├─ PieceQueue

``Torrent`` spawns a ``PeerConnection`` for every active peer, which governs the
local state:

.. code-block::

    └─ PeerConnection
       |
       ├─ BlockReceiver
       |
       ├─ BlockRequester
       │
       └─ BlockQueue
