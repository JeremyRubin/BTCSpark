from struct import Struct
from hashlib import sha256
import pprint
def VAR_INT(buf, offset):
    l = UINT8_T(buf[offset:offset+1])
    if l == 0xfd:
        return UINT16_T(buf[1+offset:3+offset]), 3+offset
    elif l == 0xfe:
        return UINT32_T(buf[1+offset:5+offset]), 5+offset
    elif l == 0xff:
        return UINT64_T(buf[1+offset:9+offset]), 9+offset
    else:
        return l, 1+offset

double_sha256 = lambda x: sha256(sha256(x).digest()).digest()
prettyf = lambda x: "    "+pprint.pformat(x).replace("\n", "\n    ")
def detuple(f):
    def detuple_wrapper(x):
        return f(x)[0]
    return detuple_wrapper
UINT64_T = detuple(Struct("<Q").unpack)
UINT32_T = detuple(Struct("<I").unpack)
UINT16_T = detuple(Struct("<H").unpack)
UINT8_T  = detuple(Struct("<B").unpack)
INT64_T  = detuple(Struct("<q").unpack)
INT32_T  = detuple(Struct("<i").unpack)
INT16_T  = detuple(Struct("<h").unpack)
INT8_T   = detuple(Struct("<b").unpack)


def SCRIPT_VAR_INT(buf, offset):
    l = UINT8_T(buf[offset:offset+1])
    if l == 0x4c:
        return UINT8_T(buf[1+offset:2+offset]), 2+offset
    elif l == 0x4d:
        return UINT16_T(buf[1+offset:3+offset]), 3+offset
    elif l == 0x4e:
        return UINT32_T(buf[1+offset:5+offset]), 5+offset
    else:
        return l, 1+offset
