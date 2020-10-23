# surge

[![Build Status](https://travis-ci.com/zuqq/surge.svg?branch=master)](https://travis-ci.com/zuqq/surge)

Surge is an implementation of the [BitTorrent protocol]. It specializes in
downloading from many peers at once, relying on Python's built-in coroutines
for concurrency. In addition to the original BitTorrent specification, Surge
also supports magnet URIs and UDP trackers.

Please note that Surge is download-only for now.

[BitTorrent protocol]: https://en.wikipedia.org/wiki/BitTorrent


## Installation

Surge requires Python 3.8 with [docopt] and [uvloop]. The recommended way of
installing these dependencies is to use [poetry]. Run `poetry install`, which
reads the dependencies from the provided `pyproject.toml` and installs them in a
new virtual environment.

[docopt]: https://pypi.org/project/docopt/
[uvloop]: https://pypi.org/project/uvloop/
[poetry]: https://python-poetry.org/


## Example

Inside of a `poetry shell`, downloading the latest [Debian release] looks like this:

```bash
$ python -m surge.magnet 'magnet:?xt=urn:btih:be00b2943b4228bdae969ddae01e89c34932255e&tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce'
$ python -m surge be00b2943b4228bdae969ddae01e89c34932255e.torrent
Download progress: 1396/1396 pieces.
$ md5sum debian-10.6.0-amd64-netinst.iso
42c43392d108ed8957083843392c794b  debian-10.6.0-amd64-netinst.iso
```

[Debian release]: https://cdimage.debian.org/debian-cd/current/amd64/bt-cd/


## Features

- Request pipelining: Surge pipelines block requests, even across pieces; this
  improves network throughput substantially.
- Incremental writes: Surge writes pieces to the file system immediately after
  downloading and verifing them, freeing up memory.
- Endgame mode: Surge requests the last few pieces from every available peer, so
  that a handful of slow peers cannot stall the download.

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


## Architecture

### Actor model

Peers are modeled as [actors]; the strong encapsulation provided by this model
makes it easy to connect to many peers at once and deal with unresponsive or
malicious peers in a uniform way.

For details about the implementation of the actor model, see the documentation
of the `actor` module.

[actors]: https://en.wikipedia.org/wiki/Actor_model

### Pure protocol implementation

The protocol is implemented in the spirit of [Sans I/O], meaning that its
state machine is completely independent from any objects performing I/O;
this enables convenient mock-free unit testing.

The state machine is based on generators functions, which provide a natural way
to express multiphasic protocols.

[Sans I/O]: https://sans-io.readthedocs.io/
