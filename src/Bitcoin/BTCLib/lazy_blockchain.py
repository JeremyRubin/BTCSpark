"""
    Copyright 2015 Jeremy Rubin

    This program is free software: you can redistribute it and/or modify it
    under the terms of the Affero GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or (at your
    option) any later version.

    This program is distributed in the hope that it will be useful, but WITHOUT
    ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
    FITNESS FOR A PARTICULAR PURPOSE.  See the Affero GNU General Public
    License for more details.

    You should have received a copy of the Affero GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
from common import *
from bitcoinscript import Script
from struct import Struct
import struct, binascii, sys
import os.path
import mmap


def lazy(f):
    @classmethod
    def __lazy_factory__(cls, *a):
        def __lazy__():
            return f(*a)
        __lazy__.__name__ = "lazy_%s"%cls.__name__
        return __lazy__
    return __lazy_factory__



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
    @property
    def hex_hash(self):
        return bytes(self.hash[::-1]).encode('hex')
    def to_dict(self):
        return dict(hash=self.hash, hex_hash = self.hex_hash, index=self.index)
    
    @lazy
    def lazy_of_buffer(a, offset):
        return TransactionInputOutPoint.of_buffer(a, offset)

    @staticmethod
    def of_buffer(a, offset):
        prev, index = TransactionInputOutPoint.FMT.unpack(a[offset:36+offset])
        offset += 36
        return TransactionInputOutPoint(prev, index)
    @staticmethod
    def buffer_length(a,offset):
        return offset+36
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
        return dict(previous_output = self.previous_output().to_dict(), signature_script = self.signature_script,sequence= self.sequence,
                    signature_script_parsed=self.signature_script_parsed)
    @property
    def signature_script_parsed(self):
        return Script.convert_script(self.signature_script)
    @property
    def hex_signature_script(self):
        return bytes(self.signature_script).encode('hex')
    @lazy
    def lazy_of_buffer(a, offset, index):
        return TransactionInput.of_buffer(a, offset, index)
    @staticmethod
    def of_buffer(a, offset, index):
        outpoint = TransactionInputOutPoint.lazy_of_buffer(a, offset)
        offset += 36
        l,offset = VAR_INT(a, offset)
        signature = a[offset:l+offset]
        offset+=l
        sequence = UINT32_T(a[offset:4+offset])
        offset += 4
        return TransactionInput(outpoint, signature, sequence, index)
    @staticmethod
    def buffer_length(a, offset, index):
        offset += 36 #offset = TransactionInputOutPoint.buffer_length(a, offset)
        l,offset = VAR_INT(a, offset)
        return offset +l + 4

    def __repr__(self):
        return "<%s instance at %s: %s>"%(self.__class__.__name__,id(self), "spending <%s>, script <%s>"%(self.previous_output, self.signature_script_parsed))
    def __str__(self):
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  self.to_dict()))

class TransactionOutput(object, HasParent):
    def __init__(self, value=None, pk_script_length=None, pk_script=None, index=None):
        self.value = value
        self.pk_script_length = pk_script_length
        self.pk_script = pk_script
        self._parent = None
        self.index = index
    @property
    def hex_pk_script(self):
        return bytes(self.pk_script).encode('hex')
    @property
    def pk_script_parsed(self):
        return Script.convert_script(self.pk_script)
    def __repr__(self):
        return "<%s instance at %s: %s>"%(self.__class__.__name__,id(self), "btc <%f>, script <%s>"%(self.value/10e7, self.pk_script_parsed))
    def to_dict(self):
        return dict(value = self.value, pk_script_length = self.pk_script_length, pk_script = self.pk_script, hex_pk_script = self.hex_pk_script, pk_script_parsed =self.pk_script_parsed)
    @lazy
    def lazy_of_buffer(a, offset, index):
        return TransactionOutput.of_buffer(a, offset, index)
    @staticmethod
    def of_buffer(a, offset, index):
        amount = INT64_T(a[offset:8+offset])
        offset += 8
        l,offset = VAR_INT(a, offset)
        script = a[offset:l+offset] 
        offset += l 
        return TransactionOutput(amount, l, script, index)
    @staticmethod
    def buffer_length(a, offset, index):
        offset += 8
        l, offset = VAR_INT(a, offset)
        return  offset+l

    def __str__(self):
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  self.to_dict()))

def Tx__s_factory(_type):
    class __factory__(object, HasParent):
        def __init__(self, l):
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
        def __iter__(self):
            return iter(self.l)
        @lazy
        def lazy_of_buffer(a,total,  offset):
            return __factory__.of_buffer(a,total, offset)
        @staticmethod
        def of_buffer(buffer, total, offset):
            ret = []
            count = 0
            while count != total:
                ret.append( _type.lazy_of_buffer(buffer, offset , count))
                offset = _type.buffer_length(buffer, offset, count)
                count += 1
            obj = __factory__(ret)
            for i in ret:
                i.parent = obj
            return obj
        @staticmethod
        def buffer_length(buffer, total, offset):
            count = 0
            while count != total:
                offset = _type.buffer_length(buffer, offset , count)
                count += 1
            return offset
        def __len__(self):
            return len(self.l)
        def filter(self, lam):
            return filter(lam, self.l)
        def map(self, lam):
            return map(lam, self.l)
        def flatMap(self, lam):
            return reduce(lambda x,y:x+y, map(lam, self.l))
        def reduce(self, lam, i):
            if i is None:
                return reduce(lam, self.l)
            else:
                return reduce(lam, self.l, i)
        def __repr__(self):
            return "<%s instance at %s: %s>"%(self.__class__.__name__,id(self), "items <%d>"%(len(self.l)))
        def __str__(self):
            l = self.to_list()
            ll = len(l)
            if ll >1:
                return "%s Object (%d items):\n%s\n\n    ...TRUNCATED %d ITEMS...\n\n%s"%(self.__class__.__name__,
                                                                                    ll, prettyf(l[0]().to_dict()),
                                                                                    ll-2, prettyf(l[-1]().to_dict()))
            if ll == 1:
                return "%s Object (1 items):\n%s"%(self.__class__.__name__,prettyf(l[0]().to_dict()))
            if ll == 0:
                return "%s Object (0 items):"%(self.__class__.__name__)
    __factory__.__name__ = _type.__name__+"s"
    return __factory__

TransactionInputs = Tx__s_factory(TransactionInput)
TransactionOutputs = Tx__s_factory(TransactionOutput)
    
class Transaction(object, HasParent):
    def __init__(self, version=None, tx_in_count=None,
                 tx_ins=None, tx_out_count=None, tx_outs=None,
                 lock_time=None, index=None, tx_id = None):

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
        self.tx_id = tx_id
    def to_dict(self):
        return dict(
        version      =  self.version,
        tx_in_count  =  self.tx_in_count,
        tx_ins        =  map(lambda x: TransactionInput.to_dict(x()),self.tx_ins().to_list()),
        tx_out_count =  self.tx_out_count,
        tx_outs       =  map(lambda x: TransactionOutput.to_dict(x()), self.tx_outs().to_list()),
        lock_time    =  self.lock_time,
        tx_id    =  self.tx_id,
        hex_tx_id    =  self.hex_tx_id,
        index        =  self.index)
    @property
    def hex_tx_id(self):
        return bytes(self.tx_id[::-1]).encode('hex')
        
    @lazy
    def lazy_of_buffer(a, offset, tx_count):
        return Transaction.of_buffer(a, offset, tx_count)
        
    @staticmethod
    def buffer_length(tx, offset,tx_count):
        offset += 4
        tx_in_count, offset = VAR_INT(tx, offset)
        offset = TransactionInputs.buffer_length(tx, tx_in_count, offset)
        tx_out_count, offset = VAR_INT(tx, offset)
        offset = TransactionOutputs.buffer_length(tx, tx_out_count, offset)
        return offset+4

    @staticmethod
    def of_buffer(tx, offset,tx_count):
        offset_0 = offset
        tx_ver = UINT32_T(tx[offset:offset+4])
        offset += 4
        tx_in_count, offset = VAR_INT(tx, offset)
        tx_ins = TransactionInputs.lazy_of_buffer(tx, tx_in_count, offset)
        offset = TransactionInputs.buffer_length(tx, tx_in_count, offset)
        tx_out_count, offset = VAR_INT(tx, offset)
        tx_outs = TransactionOutputs.lazy_of_buffer(tx, tx_out_count, offset)
        offset = TransactionOutputs.buffer_length(tx, tx_out_count, offset)
        lock_time = UINT32_T(tx[offset:offset+4])
        offset+=4
        tx_id = double_sha256(tx[offset_0:offset])
        return Transaction(tx_ver, tx_in_count, tx_ins,
                   tx_out_count, tx_outs, lock_time,
                           tx_count, tx_id)

    def __repr__(self):
        return "<%s instance at %s: %s>"%(self.__class__.__name__,id(self),
                                          "tx_id <%s> btc <%s>, outputs <%d>, inputs <%d>"%(self.hex_tx_id,
                                                                                            self.tx_outs.reduce(lambda x,y: x+y.value,0),
                                                                                            len(self.tx_outs),
                                                                                            len(self.tx_ins)))
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
    @property
    def hex_block_hash(self):
        return bytes(self.block_hash[::-1]).encode('hex')
    @property
    def hex_prev_block(self):
        return bytes(self.prev_block[::-1]).encode('hex')
    @property
    def hex_merkle_root(self):
        return bytes(self.merkle_root[::-1]).encode('hex')
    def to_dict(self):
        return dict(
                version = self.version,
                prev_block = self.prev_block,
                merkle_root = self.merkle_root,
                block_hash = self.block_hash,
                hex_block_hash = self.hex_block_hash,
                hex_merkle_root = self.hex_merkle_root,
                hex_prev_block = self.hex_prev_block,
                timestamp = self.timestamp,
                bits = self.bits,
                nonce = self.nonce,
                txn_count = self.txn_count)
    def __repr__(self):
        return "<%s instance at %s: %s>"%(self.__class__.__name__,id(self), "hash <%s>, txns <%d>"%(self.hex_block_hash, self.txn_count))

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
                txns = map(lambda x: Transaction.to_dict(x()),self.txns().to_list()) )
    def __repr__(self):
        return "<%s instance at %s: %s>"%(self.__class__.__name__,id(self), "hash <%s>, txns <%d>"%(self.header.hex_block_hash, self.header.txn_count))
    def __str__(self):
        d = self.to_dict()
        d["txns"] = self.txns
        # d = dict(map(lambda (k,v): (k, repr(v)), d.iteritems()))
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  d ))
    @staticmethod
    def of_string(data, offset):
        f = binascii.unhexlify(data.strip())
        return Block.of_buffer(bytearray(f), offset/2) # is that kosher?  This is usually 0 anyways....
    @lazy
    def lazy_of_buffer(a, offset, end=-1):
        return Block.of_buffer(a, offset, end=end)
    @staticmethod
    def of_buffer(data,offset, end=-1):
        header, offset = BlockHeader.of_buffer(data, offset)
        txns= Transactions.lazy_of_buffer(data, header.txn_count, offset)
        return Block(header, txns)


    @staticmethod
    def of_file(name):
        f = open(name) # this gets rid of some allocations I think!
        data = bytearray(os.path.getsize(name))
        f.readinto(data)
        return Block.of_buffer(binascii.unhexlify(data.strip()),0) # unhex is smaller

class Blocks(object):
    @staticmethod
    def of_buffer(data, offset, size, err=None):
        try:
            offset = 0
            while size - offset > 8:
                #TODO: What if the magic is not there?
                m = data[offset:offset+4]
                magic = UINT32_T(m)
                if magic != 0xD9B4BEF9:
                    offset += 1
                    continue
                offset+=4
                n_bytes = UINT32_T(data[offset:offset+4])
                offset+=4
                x = offset+n_bytes
                lazy_block = Block.lazy_of_buffer(data,offset, x) # unhex is smaller
                offset = x
                yield lazy_block
        except Exception as e:
            sys.stdout.write("FATAL %s:\n"%e)
            raise ValueError("Malformed file %s"%err)
        raise StopIteration

    @staticmethod
    def of_file(name, iterator=True):
        size = os.path.getsize(name)
        with open(name) as f: # this gets rid of some allocations I think!
            data = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            if iterator:
                return Blocks.of_buffer(data, 0, size)
            else:
                return list(Blocks.of_buffer(data, 0, size))
        
    @staticmethod
    def of_file_full(name, iterator=True):
        size = os.path.getsize(name)
        with open(name) as f: # this gets rid of some allocations I think!
            data =  f.read()
            if iterator:
                return Blocks.of_buffer(data, 0, size)
            else:
                return list(Blocks.of_buffer(data, 0, size))

if __name__ == "__main__":
    for i in sys.argv[1:]:
        # print i
        e = Blocks.of_file_full(i,  True)
        a = e.next()
        b = a().txns().to_list()[0]().tx_outs().map
        print b
        # print e[0]().txns().to_list()[0]()
        # txns = block_objs.flatMap(lambda b: 
        #                   iter(b().txns()))\
        #                  .map(lambda txn: txn())
        # # Transaction Output Amount Distribution
        # txns.flatMap(lambda txn: txn.tx_outs()\
        #     .map(lambda lazy_txo: lazy_txo())\
        #     .map(lambda txo: ((txo.value>>14)<<14, 1)))\
        #     .reduceByKey(lambda x,y: x+y)\
        #     .saveAsTextFile(result_name("txouts_values"))
        
        # print        e
        # print e
        # # print e.header
        # print e.txns
        # print e.txns[0]
        # print e.txns[0].tx_ins
        # print e.txns[0].tx_outs

        # pretty(e["txns"][:2])
        # e["txns"] = '...'
        # pretty(e)






