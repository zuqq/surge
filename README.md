# surge

![tests](https://github.com/zuqq/surge/actions/workflows/tests.yml/badge.svg)

Surge is an implementation of the [BitTorrent protocol]. It specializes in
downloading from many peers at once, relying on Python's built-in coroutines
for concurrency. In addition to the original BitTorrent specification, Surge
also supports magnet URIs and UDP trackers.

Please note that Surge is download-only for now.

[BitTorrent protocol]: https://en.wikipedia.org/wiki/BitTorrent

## Requirements

Surge requires Python 3.8; all of its dependencies are provided by the standard
library.

There is optional support for [uvloop], which you can install via [Poetry] by
running `poetry install -E uvloop`.

[uvloop]: https://pypi.org/project/uvloop/
[Poetry]: https://python-poetry.org/

## Example

If you chose to install uvloop via Poetry, you need to run `poetry shell`
before trying out the example.

Downloading the latest [Debian release] looks like this:

```console
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
