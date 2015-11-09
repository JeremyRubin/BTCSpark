from common import *
from bitcoinscript import Script
from struct import Struct
import struct, binascii, sys
import os.path


class HasParent:
    @property
    def parent(self):
        return self._parent
    @parent.setter
    def parent(self, value):
        self._parent=value
    
class TransactionInputOutPoint(object, HasParent):
    def __init__(self, hash=None, index=None):
        self.hash = hash
        self.index = index
        self._parent = None
    FMT = Struct("<32s I")
    def hex_hash(self):
        return bytes(self.hash[::-1]).encode('hex')
    def to_dict(self):
        return dict(hash=self.hash, hex_hash = self.hex_hash(), index=self.index)
    
    @staticmethod
    def of_buffer(a, offset):
        prev, index = TransactionInputOutPoint.FMT.unpack(a[offset:36+offset])
        offset += 36
        return TransactionInputOutPoint(prev, index), offset

    def __str__(self):
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  self.to_dict()))
        
class TransactionInput(object, HasParent):
    def __init__(self, previous_output=None, signature_script=None, sequence=None, index=None):
        previous_output.parent = self 
        self.previous_output = previous_output
        self.signature_script = signature_script
        self.sequence = sequence
        self._parent = None
        self.index = index
    def to_dict(self):
        return dict(previous_output = self.previous_output.to_dict(), signature_script = self.signature_script,sequence= self.sequence,
                    signature_script_parsed=self.signature_script_parsed())

    def signature_script_parsed(self):
        return Script.convert_script(self.signature_script)
    @staticmethod
    def of_buffer(a, offset, index):
        outpoint, offset = TransactionInputOutPoint.of_buffer(a, offset)
        l,offset = VAR_INT(a, offset)
        signature = a[offset:l+offset]
        offset+=l
        sequence = UINT32_T(a[offset:4+offset])
        offset += 4
        return TransactionInput(outpoint, signature, sequence, index), offset
    def __str__(self):
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  self.to_dict()))

class TransactionOutput(object, HasParent):
    def __init__(self, value=None, pk_script_length=None, pk_script=None, index=None):
        self.value = value
        self.pk_script_length = pk_script_length
        self.pk_script = pk_script
        self._parent = None
        self.index = index
    def hex_pk_script(self):
        return bytes(self.pk_script).encode('hex')
    def pk_script_parsed(self):
        return Script.convert_script(self.pk_script)
    def to_dict(self):
        return dict(value = self.value, pk_script_length = self.pk_script_length, pk_script = self.pk_script, hex_pk_script = self.hex_pk_script(), pk_script_parsed =self.pk_script_parsed())
      
    @staticmethod
    def of_buffer(a, offset, index):
        amount = INT64_T(a[offset:8+offset])
        offset += 8
        l,offset = VAR_INT(a, offset)
        script = a[offset:l+offset]
        offset += l 
        return TransactionOutput(amount, l, script, index), offset
    def __str__(self):
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  self.to_dict()))

def Tx__s_factory(_type):
    class __factory__(object, HasParent):
        def __init__(self, l):
            for i in l:
                i.parent = self
            self.l = l
            self._parent = None
        def to_list(self):
            return self.l
        def __getitem__(self, sl):
            return self.l[sl]
        def first(self):
            return self.l[0]
        def last(self):
            return self.l[-1]
        @staticmethod
        def of_buffer(buffer, total, offset):
            ret = []
            count = 0
            while count != total:
                item, offset = _type.of_buffer(buffer, offset , count)
                count += 1
                ret.append(item)
            return __factory__(ret),offset
        def filter(self, lam):
            return filter(lam, self.l)
        def map(self, lam):
            return map(lam, self.l)
        def flatMap(self, lam):
            return reduce(lambda x,y:x+y, map(lam, self.l))
        def reduce(self, lam):
            return reduce(lam, self.l)
        def __str__(self):
            l = self.to_list()
            ll = len(l)
            if ll >1:
                return "%s Object (%d items):\n%s\n\n    ...TRUNCATED %d ITEMS...\n\n%s"%(self.__class__.__name__,
                                                                                    ll, prettyf(l[0].to_dict()),
                                                                                    ll-2, prettyf(l[-1].to_dict()))
            if ll == 1:
                return "%s Object (1 items):\n%s"%(self.__class__.__name__,prettyf(l[0].to_dict()))
            if ll == 0:
                return "%s Object (0 items):"%(self.__class__.__name__)
    __factory__.__name__ = _type.__name__+"s"
    return __factory__
TransactionInputs = Tx__s_factory(TransactionInput)
TransactionOutputs = Tx__s_factory(TransactionOutput)
    
class Transaction(object, HasParent):
    def __init__(self, version=None, tx_in_count=None,
                 tx_ins=None, tx_out_count=None, tx_outs=None,
                 lock_time=None, index=None):

        tx_ins.parent = self
        tx_outs.parent = self
        self.version = version
        self.tx_in_count = tx_in_count
        self.tx_ins = tx_ins
        self.tx_out_count = tx_out_count
        self.tx_outs = tx_outs
        self.lock_time = lock_time
        self.index = index
        self._parent=None
    def to_dict(self):
        return dict(
        version      =  self.version,
        tx_in_count  =  self.tx_in_count,
        tx_ins        =  map(TransactionInput.to_dict,self.tx_ins.to_list()),
        tx_out_count =  self.tx_out_count,
        tx_outs       =  map(TransactionOutput.to_dict, self.tx_outs.to_list()),
        lock_time    =  self.lock_time,
        index        =  self.index)
        
    @staticmethod
    def of_buffer(tx, offset,tx_count):
        tx_ver = UINT32_T(tx[offset:offset+4])
        offset += 4
        tx_in_count, offset = VAR_INT(tx, offset)
        tx_ins, offset = TransactionInputs.of_buffer(tx, tx_in_count, offset)
        tx_out_count, offset = VAR_INT(tx, offset)
        tx_outs, offset = TransactionOutputs.of_buffer(tx, tx_out_count, offset)
        lock_time = UINT32_T(tx[offset:offset+4])
        offset+=4
        return Transaction(tx_ver, tx_in_count, tx_ins,
                   tx_out_count, tx_outs, lock_time,
                   tx_count), offset

    def __str__(self):
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  self.to_dict()))
Transactions = Tx__s_factory(Transaction)

class BlockHeader(object, HasParent):
    def __init__(self, version=None, prev_block=None, merkle_root=None,
                 block_hash=None, timestamp=None, bits=None, nonce=None,
                 txn_count=None):

        self.version = version
        self.prev_block = prev_block
        self.merkle_root = merkle_root
        self.block_hash = block_hash
        self.timestamp = timestamp
        self.bits = bits
        self.nonce = nonce
        self.txn_count = txn_count
        self._parent = None
    FIXED_HEADER_BYTES = 80 # 4+32+32+4+4+4
    FMT = Struct("<i 32s 32s I I I")
    @staticmethod
    def of_buffer(data, offset):
        header = data[offset:offset+BlockHeader.FIXED_HEADER_BYTES]
        block_hash = double_sha256(header) 
        s =  BlockHeader.FMT.unpack( header)
        offset += BlockHeader.FIXED_HEADER_BYTES
        version, prev_block, merkle_root, timestamp, bits, nonce = s
        txn_count, offset = VAR_INT(data, offset)
        return BlockHeader(version, prev_block, merkle_root,
                     block_hash, timestamp, bits, nonce,
                           txn_count), offset
    def hex_block_hash(self):
        return bytes(self.block_hash[::-1]).encode('hex')
    def hex_prev_block(self):
        return bytes(self.prev_block[::-1]).encode('hex')
    def hex_merkle_root(self):
        return bytes(self.merkle_root[::-1]).encode('hex')
    def to_dict(self):
        return dict(
                version = self.version,
                prev_block = self.prev_block,
                merkle_root = self.merkle_root,
                block_hash = self.block_hash,
                hex_block_hash = self.hex_block_hash(),
                hex_merkle_root = self.hex_merkle_root(),
                hex_prev_block = self.hex_prev_block(),
                timestamp = self.timestamp,
                bits = self.bits,
                nonce = self.nonce,
                txn_count = self.txn_count)

    def __str__(self):
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  self.to_dict() ))
class Block(object):
    def __init__(self, header=None, txns=None):
        header.parent = self
        txns.parent = self
        self.header = header
        self.txns = txns
        self.height = -1
    def setHeight(self, h):
        self.height = h
    def to_dict(self):
        return dict(
                header = self.header.to_dict() ,
                txns = map(Transaction.to_dict,self.txns.to_list()) )
    def __str__(self):
        d = self.to_dict()
        d["txns"] = "..."
        # d = dict(map(lambda (k,v): (k, repr(v)), d.iteritems()))
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  d ))
    @staticmethod
    def of_string(data, offset):
        f = binascii.unhexlify(data)
        return Block.of_buffer(bytearray(f), offset/2) # is that kosher?  This is usually 0 anyways....
    @staticmethod
    def of_buffer(data, offset):
        header, offset = BlockHeader.of_buffer(data, offset)
        txns,offset = Transactions.of_buffer(data, header.txn_count, offset )
        return Block(header, txns), offset

    @staticmethod
    def of_file(name):
        f = open(name) # this gets rid of some allocations I think!
        data = bytearray(os.path.getsize(name))
        f.readinto(data)
        return Block.of_buffer(binascii.unhexlify(data),0) # unhex is smaller


if __name__ == "__main__":
    from pprint import pprint as pretty
    import cProfile as prof
    e, _ = Block.of_file(sys.argv[1])
    print e
    # print e.header
    # print e.txns
    # print e.txns[0]
    # print e.txns[0].tx_ins
    print e.txns[0].tx_outs

    # pretty(e["txns"][:2])
    # e["txns"] = '...'
    # pretty(e)






