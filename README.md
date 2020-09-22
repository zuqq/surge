# surge

Surge is a client for the BitTorrent network, built on top of Python's
coroutine-based concurrency model and the asyncio event loop. Please note that
it is download-only for now.


## Installation

Surge requires Python 3.8 and [docopt]. The recommended way of installing these
dependencies is to use [poetry]. Run

```
poetry install
```

in the root folder, which automatically gathers the dependencies from the
provided `pyproject.toml`.

[docopt]: https://pypi.org/project/docopt/
[poetry]: https://python-poetry.org/


## Usage

Surge has a minimal command-line interface for downloading a single torrent,
located in the `__main__` module. Inside of a `poetry shell` or another
appropriate environment it can be run as `python -m surge`.

**Example:**

```
$ python -m surge --file debian.torrent
Reading metadata from debian.torrent.
Progress: 1340/1340 pieces.
```

**Help page:**

```
$ python -m surge --help
Download files from the BitTorrent network.

Usage:
    __main__.py (-h | --help)
    __main__.py [--folder FOLDER] [--resume] [--log LOG]
                [--peers PEERS] [--requests REQUESTS]
                (--file FILE | --magnet MAGNET)

Options:
    -h, --help          Show this screen.
    --folder FOLDER     Destination folder
    --resume            Resume the download.
    --log LOG           Log file.
    --peers PEERS       Maximal number of peers [default: 50].
    --requests REQUEST  Maximal number of requests [default: 50].
    --file PATH         Torrent file.
    --magnet MAGNET     Magnet link.
```


## Architecture

### Protocol extensions

Surge supports the following extensions to the [BitTorrent protocol][BEP 0003]:

- [Metadata file exchange][BEP 0009]
- [Tracker list][BEP 0012]
- [UDP tracker protocol][BEP 0015]
- [Compact tracker response][BEP 0023]

[BEP 0003]: http://bittorrent.org/beps/bep_0003.html
[BEP 0009]: http://bittorrent.org/beps/bep_0009.html
[BEP 0012]: http://bittorrent.org/beps/bep_0012.html
[BEP 0015]: http://bittorrent.org/beps/bep_0015.html
[BEP 0023]: http://bittorrent.org/beps/bep_0023.html

### Features

- **Concurrency**: Surge is designed to connect to many peers at the same time.
- **Availability tracking**: Surge keeps track of which pieces its peers have,
  enabling it to make successful requests.
- **Endgame mode**: Surge requests the last few pieces from every available peer,
  so that a handful of slow peers cannot stall the download.
- **Incremental writes**: Surge writes pieces to the file system immediately after
  downloading and verifing them, keeping its memory usage low.
- **Request queuing**: Surge uses request pipelining, improving network throughput.

### Actor model

Peers are modeled as actors; the strong encapsulation provided by this model
makes it easy to deal with unresponsive or malicious peers.

For details about the implementation of the actor model, see the documentation
of the `actor` module.

### Protocol implementation

The protocol is implemented in the spirit of [Sans I/O], meaning that its
state machine is completely independent from any objects performing I/O;
this enables convenient mock-free unit testing.

The state machine is based on generators functions, which provide a natural way
to express multiphasic protocols.

[Sans I/O]: https://sans-io.readthedocs.io/
