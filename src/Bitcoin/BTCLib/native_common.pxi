from struct import Struct
from hashlib import sha256
import pprint
def memoryviewstrip(m):
    count = 0
    while True:
        if not m[count].isspace():
            break
        count += 1
    count2 = len(m)-1
    while True:
        if not m[count2].isspace():
            break
        count2 -= 1
    return m[count:count2+1] # ugh slices
HEX_STR_TO_CHR = dict(("%02x"%k,chr(k)) for k in xrange(256))
def memoryviewunhexlify(m):
    count = 0
    insertcount = 0
    l = len(m)
    if (l>>1)<<1 != l:
        raise ValueError("Odd Sized Buffer")
    while count < l:
        # print str(m[count:count+2].tobytes())
        p = HEX_STR_TO_CHR[str(m[count:count+2].tobytes())]
        m[insertcount] = p
        count +=2
        insertcount +=1
    return m[:insertcount]




cdef uint64_t VAR_INT(bytes buf, size_t * offset):
    cdef uint64_t ret
    cdef uint8_t l =UINT8_T(buf, offset) 
    offset[0] += 1
    if l == 0xfd:
        ret = <uint64_t>UINT16_T(buf, offset)
        offset[0] += 2
    elif l == 0xfe:
        ret = <uint64_t>UINT32_T(buf, offset)
        offset[0] += 4
    elif l == 0xff:
        ret = UINT64_T(buf, offset)
        offset[0] += 8
    else:
        ret = <uint64_t> l
        ret2 = ret
    return ret

double_sha256 = lambda x: sha256(sha256(x).digest()).digest()
prettyf = lambda x: "    "+pprint.pformat(x).replace("\n", "\n    ")
def detuple(f):
    def detuple_wrapper(x):
        return f(x)[0]
    return detuple_wrapper

cdef inline uint64_t UINT64_T(bytes buf, size_t * offset):
    return (<uint64_t*>((<uint8_t*>buf)+offset[0]))[0]

cdef inline uint32_t UINT32_T(bytes buf, size_t * offset):
    return (<uint32_t*>((<uint8_t*>buf)+offset[0]))[0]

cdef inline uint16_t UINT16_T(bytes buf, size_t * offset):
    return (<uint16_t*>((<uint8_t*>buf)+offset[0]))[0]

cdef inline uint8_t UINT8_T(bytes buf, size_t * offset):
    return (<uint8_t*>((<uint8_t*>buf)+offset[0]))[0]

cdef inline int64_t INT64_T(bytes buf, size_t * offset):
    return ((<int64_t*>((<uint8_t*>buf)+offset[0]))[0])

cdef inline int32_t INT32_T(bytes buf, size_t * offset):
    return (<int32_t*>((<uint8_t*>buf)+offset[0]))[0]

cdef inline int16_t INT16_T(bytes buf, size_t * offset):
    return (<int16_t*>((<uint8_t*>buf)+offset[0]))[0]

cdef inline int8_t INT8_T(bytes buf, size_t * offset):
    return (<int8_t*>((<uint8_t*>buf)+offset[0]))[0]
cdef SCRIPT_VAR_INT(buf, size_t  offset):
    l = UINT8_T(buf, &offset)
    offset += 1
    if l == 0x4c:
        return UINT8_T(buf, &offset), 2+offset
    elif l == 0x4d:
        return UINT16_T(buf, &offset), 3+offset
    elif l == 0x4e:
        return UINT32_T(buf, &offset), 5+offset
    else:
        return l, 1+offset
