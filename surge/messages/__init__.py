from ._base import *
from ._extension import Handshake as ExtensionHandshake
from ._extension import Metadata as MetadataProtocol
from ._metadata import Data as Metadata
from ._metadata import Request as MetadataRequest


def extension_handshake():
    return ExtensionProtocol(ExtensionHandshake())


def metadata_request(index, ut_metadata):
    return ExtensionProtocol(MetadataProtocol(MetadataRequest(index), ut_metadata))
