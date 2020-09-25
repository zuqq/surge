# surge

[![Build Status](https://travis-ci.com/zuqq/surge.svg?branch=master)](https://travis-ci.com/zuqq/surge)

Surge is a client for the BitTorrent network, built on top of Python's
coroutine-based concurrency model and the asyncio event loop. Please note that
it is download-only for now.

## Installation

Surge requires Python 3.8 and [docopt]. The recommended way of installing these
dependencies is to use [poetry]. Run `poetry install` in the root folder, which
automatically gathers the dependencies from the provided `pyproject.toml` and
installs them in a new virtual environment.

[docopt]: https://pypi.org/project/docopt/
[poetry]: https://python-poetry.org/


## Usage

Surge has a minimal command-line interface for downloading a single torrent,
located in the `__main__` module. Inside of a `poetry shell` or another
appropriate environment it can be run as `python -m surge`.

**Example:**

```
$ poetry shell
$ python -m surge --magnet 'magnet:?xt=urn:btih:4fb4bcb6fbb2181c7c5373d5bc9e5d781764164a&tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce'
Downloading metadata from peers...Done.
Writing metadata to 4fb4bcb6fbb2181c7c5373d5bc9e5d781764164a.torrent.
Progress: 1396/1396 pieces.
$ md5sum debian-10.5.0-amd64-netinst.iso
a3ebc76aec372808ad80000108a2593a  debian-10.5.0-amd64-netinst.iso
```

**Help page:**

```
$ poetry run python -m surge --help
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

### Actor model

Peers are modeled as [actors]; the strong encapsulation provided by this model
makes it easy to connect to many peers at once and deal with unresponsive or
malicious peers in a uniform way.

For details about the implementation of the actor model, see the documentation
of the `actor` module.

[actors]: https://en.wikipedia.org/wiki/Actor_model

### Request pipelining

Surge uses request pipelining, even across pieces; this improves network
throughput substantially.

### Incremental writes

Downloaded pieces are written to the file system immediately after they're
downloaded and verified, freeing up memory.

### Endgame mode

Surge requests the last few pieces from every available peer, so that a handful
of slow peers cannot stall the download.

### Pure protocol implementation

The protocol is implemented in the spirit of [Sans I/O], meaning that its
state machine is completely independent from any objects performing I/O;
this enables convenient mock-free unit testing.

The state machine is based on generators functions, which provide a natural way
to express multiphasic protocols.

[Sans I/O]: https://sans-io.readthedocs.io/

### Supported protocol extensions

Surge supports the following extensions to the [base protocol][BEP 0003]:

- [Metadata file exchange][BEP 0009]
- [Tracker list][BEP 0012]
- [UDP tracker protocol][BEP 0015]
- [Compact tracker response][BEP 0023]

[BEP 0003]: http://bittorrent.org/beps/bep_0003.html
[BEP 0009]: http://bittorrent.org/beps/bep_0009.html
[BEP 0012]: http://bittorrent.org/beps/bep_0012.html
[BEP 0015]: http://bittorrent.org/beps/bep_0015.html
[BEP 0023]: http://bittorrent.org/beps/bep_0023.html
