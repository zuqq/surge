from ._base import *
from ._extension import Handshake as ExtensionHandshake
from ._extension import Metadata as MetadataProtocol
from ._metadata import Data as MetadataData
from ._metadata import Reject as MetadataReject
from ._metadata import Request as MetadataRequest


def extension_handshake():
    return ExtensionProtocol(ExtensionHandshake())


def metadata_data(index, total_size, data):
    return ExtensionProtocol(MetadataProtocol(MetadataData(index, total_size, data)))


def metadata_reject(index):
    return ExtensionProtocol(MetadataProtocol(MetadataReject(index)))


def metadata_request(index, ut_metadata):
    return ExtensionProtocol(MetadataProtocol(MetadataRequest(index), ut_metadata))
