from bitcoinscript import Script
from struct import Struct
import struct, binascii, sys
import os.path
import mmap
from libc.stdint cimport uint32_t, int64_t,  int32_t, uint64_t, int16_t, uint16_t, int8_t, uint8_t
from libc.string cimport memcpy
include "native_common.pxi"

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free

cdef class __ManyMixin__:
    cdef readonly object l
    def __init__(self, l):
        self.l = l
    cpdef to_list(self):
        return self.l
    def __getitem__(self, sl):
        return self.l[sl]
    cpdef first(self):
        return self.l[0]
    cpdef last(self):
        return self.l[-1]
    def __iter__(self):
        return iter(self.l)
    def __len__(self):
        return len(self.l)
    cpdef filter(self, lam):
        return filter(lam, self.l)
    cpdef map(self, lam):
        return map(lam, self.l)
    cpdef flatMap(self, lam):
        return [y for x in map(lam, self.l) for y in x]
    cpdef reduce(self, lam, i):
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
cdef class LazyTransactionInputOutPoint:
    cdef bytes a
    cdef size_t offset
    def __cinit__(self, bytes a, size_t o):
        self.a=a
        self.offset = o
    def __call__(self):
        return TransactionInputOutPoint.of_buffer(self.a, &self.offset)
ctypedef uint8_t hashv[32]
cdef class TransactionInputOutPoint:
    cdef readonly bytes hash
    cdef readonly uint32_t index 
    property hex_hash:
        def __get__(self):
            return bytes(self.hash[::-1]).encode('hex')
    

    @staticmethod
    cdef of_buffer(bytes a, size_t * offset):
        r = TransactionInputOutPoint()
        r.hash =  a[offset[0]:offset[0]+32]
        offset[0]+=32
        r.index = UINT32_T(a, offset)
        offset[0]+=4
        return r
    @staticmethod
    cdef buffer_length(bytes a,size_t offset):
        return offset+36
    def __str__(self):
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  self.to_dict()))
    def to_dict(self):
        return dict(hash=self.hash, hex_hash = self.hex_hash, index=self.index)
        


cdef class LazyTransactionInput:
    cdef bytes a
    cdef size_t offset
    cdef uint32_t i
    def __cinit__(self, bytes a, size_t o, uint32_t i):
        self.a=a
        self.offset = o
        self.i = i
    def __call__(self):
        return TransactionInput.of_buffer(self.a, &self.offset, self.i)

cdef class TransactionInput:
    cdef readonly LazyTransactionInputOutPoint previous_output
    cdef readonly bytes signature_script
    cdef readonly uint32_t sequence
    cdef readonly uint32_t index

    #def __cinit__(self, previous_output, signature_script, sequence, index):
    #    self.previous_output = previous_output
    #    self.signature_script = signature_script
    #    self.sequence = sequence
    #    self.index = index
    def to_dict(self):
        return dict(previous_output = self.previous_output().to_dict(), signature_script = self.signature_script,sequence= self.sequence,
                    signature_script_parsed=self.signature_script_parsed)
    property signature_script_parsed:
        def __get__(self):
            return Script.convert_script(self.signature_script)
    property hex_signature_script:
        def __get__(self):
            return bytes(self.signature_script).encode('hex')
    @staticmethod
    cdef of_buffer(bytes a, size_t * offset,uint32_t index):
        r = TransactionInput()
        r.index = index
        r.previous_output = LazyTransactionInputOutPoint(a, offset[0])
        offset[0] += 36
        l = VAR_INT(a, offset)
        r.signature_script = a[offset[0]:offset[0]+l]
        offset[0] +=l
        r.sequence = UINT32_T(a, offset)#a[offset[0]:4+offset[0]])
        offset[0] += 4
        return r
    @staticmethod
    cdef size_t buffer_length(bytes a, size_t offset, uint32_t index):
        offset += 36 #offset = TransactionInputOutPoint.buffer_length(a, offset)
        l = VAR_INT(a, &offset)
        return offset+l+4

    def __repr__(self):
        return "<%s instance at %s: %s>"%(self.__class__.__name__,id(self), "spending <%s>, script <%s>"%(self.previous_output, self.signature_script_parsed))
    def __str__(self):
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  self.to_dict()))

cdef class LazyTransactionInputs:
    cdef bytes a
    cdef size_t offset
    cdef uint32_t t
    def __cinit__(self, bytes a,  uint32_t t, size_t o):
        self.a=a
        self.offset = o
        self.t = t
    def __call__(self):
        return TransactionInputs.of_buffer(self.a,self.t, &self.offset)
cdef class TransactionInputs(__ManyMixin__):
    _type = TransactionInput
    @staticmethod
    cdef TransactionInputs of_buffer(bytes buffer, uint32_t total, size_t * offset):
        ret = []
        count = 0
        while count != total:
            ret.append(LazyTransactionInput(buffer, offset[0] , count))
            offset[0] = TransactionInput.buffer_length(buffer, offset[0], count)
            count += 1
        obj = TransactionInputs(ret)
        return obj
    @staticmethod
    cdef size_t buffer_length(bytes buffer, uint32_t total, size_t offset):
        count = 0
        while count != total:
            offset = TransactionInput.buffer_length(buffer, offset , count)
            count += 1
        return offset






#################################
##     TXO
#################################

cdef class LazyTransactionOutput:
    cdef bytes a
    cdef size_t offset
    cdef uint32_t i
    def __cinit__(self, bytes a, size_t o, uint32_t i):
        self.a=a
        self.offset = o
        self.i = i
    def __call__(self):
        return TransactionOutput.of_buffer(self.a, &self.offset, self.i)
cdef class TransactionOutput:
    cdef readonly int64_t value
    cdef readonly uint32_t index 
    cdef readonly uint32_t pk_script_length
    cdef readonly bytes pk_script
    
    def __cinit__(self, int64_t value, uint32_t pk_script_length, bytes pk_script, uint32_t index):
        self.value = value
        self.pk_script_length = pk_script_length
        self.pk_script = pk_script
        self.index = index
    property hex_pk_script:
        def __get__(self):
            return bytes(self.pk_script).encode('hex')
    property pk_script_parsed:
        def __get__(self):
            return Script.convert_script(self.pk_script)
    def __repr__(self):
        return "<%s instance at %s: %s>"%(self.__class__.__name__,id(self), "btc <%f>, script <%s>"%(self.value/10e7, self.pk_script_parsed))
    def to_dict(self):
        return dict(value = self.value, pk_script_length = self.pk_script_length, pk_script = self.pk_script, hex_pk_script = self.hex_pk_script, pk_script_parsed =self.pk_script_parsed)
    def __str__(self):
        return "%s Object:\n%s"%(self.__class__.__name__,prettyf(  self.to_dict()))
    @staticmethod
    cdef of_buffer(bytes a, size_t * offset, uint32_t index):
        amount = INT64_T(a, offset)
        offset[0] += 8
        l = VAR_INT(a, offset)
        script = a[offset[0]:l+offset[0]] 
        offset[0] += l 
        return TransactionOutput(amount, l, script, index)
    @staticmethod
    cdef buffer_length(bytes a, size_t offset, uint32_t index):
        offset += 8
        l= VAR_INT(a, &offset)
        return  offset+l

cdef class LazyTransactionOutputs:
    cdef bytes a
    cdef size_t offset
    cdef uint32_t t
    def __cinit__(self, bytes a,  uint32_t t, size_t o):
        self.a=a
        self.offset = o
        self.t = t
    def __call__(self):
        return TransactionOutputs.of_buffer(self.a,self.t, &self.offset)
cdef class TransactionOutputs(__ManyMixin__):
    _type = TransactionOutput
    @staticmethod
    cdef TransactionOutputs of_buffer(bytes buffer, uint32_t total, size_t * offset):
        ret = []
        count = 0
        while count != total:
            ret.append(LazyTransactionOutput(buffer, offset[0] , count))
            offset[0] = TransactionOutput.buffer_length(buffer, offset[0], count)
            count += 1
        obj = TransactionOutputs(ret)
        return obj
    @staticmethod
    cdef size_t buffer_length(bytes buffer, uint32_t total, size_t offset):
        count = 0
        while count != total:
            offset = TransactionOutput.buffer_length(buffer, offset , count)
            count += 1
        return offset



    
cdef class LazyTransaction:
    cdef bytes a
    cdef size_t offset
    cdef uint32_t c
    def __cinit__(self, bytes a,   size_t o, uint32_t c):
        self.a=a
        self.offset = o
        self.c = c
    def __call__(self):
        return Transaction.of_buffer(self.a, &self.offset, self.c)
cdef class Transaction:
    cdef readonly uint64_t tx_in_count, tx_out_count
    cdef readonly LazyTransactionInputs tx_ins
    cdef readonly LazyTransactionOutputs tx_outs
    cdef readonly uint32_t lock_time, index, version
    cdef readonly bytes tx_id
    def __cinit__(self, uint32_t version, uint64_t tx_in_count,
                 LazyTransactionInputs tx_ins, uint64_t tx_out_count, LazyTransactionOutputs tx_outs,
                 uint32_t lock_time,uint32_t index,bytes tx_id):
        self.version = version
        self.tx_in_count = tx_in_count
        self.tx_ins = tx_ins
        self.tx_out_count = tx_out_count
        self.tx_outs = tx_outs
        self.lock_time = lock_time
        self.index = index
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
    property hex_tx_id:
        def __get__(self):
            return bytes(self.tx_id[::-1]).encode('hex')
        
        
    @staticmethod
    cdef size_t buffer_length(bytes tx, size_t offset, uint32_t tx_count):
        offset += 4
        cdef uint64_t tx_in_count= VAR_INT(tx, &offset)
        offset = TransactionInputs.buffer_length(tx, tx_in_count, offset)
        cdef uint64_t tx_out_count = VAR_INT(tx, &offset)
        offset = TransactionOutputs.buffer_length(tx, tx_out_count, offset)
        return offset+4

    @staticmethod
    cdef Transaction of_buffer(bytes tx, size_t * offset, uint32_t tx_count):
        cdef size_t offset_0  = offset[0]
        offset[0] += 4
        tx_ver = UINT32_T(tx, offset)#tx[offset[0]-4:offset[0]])

        tx_in_count = VAR_INT(tx, offset)
        tx_ins = LazyTransactionInputs(tx, tx_in_count, offset[0])
        offset[0] = TransactionInputs.buffer_length(tx, tx_in_count, offset[0])

        tx_out_count = VAR_INT(tx, offset)
        tx_outs = LazyTransactionOutputs(tx, tx_out_count, offset[0])
        offset[0] = TransactionOutputs.buffer_length(tx, tx_out_count, offset[0])

        lock_time = UINT32_T(tx, offset)#[offset[0]:offset[0]+4])
        offset[0]+=4

        cdef bytes tx_id = double_sha256(tx[offset_0:offset[0]])
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

cdef class LazyTransactions:
    cdef bytes a
    cdef size_t offset
    cdef uint32_t t
    def __cinit__(self, bytes a,  uint32_t t, size_t o):
        self.a=a
        self.offset = o
        self.t = t
    def __call__(self):
        return Transactions.of_buffer(self.a,self.t, &self.offset)
cdef class Transactions(__ManyMixin__):
    _type = Transaction

    @staticmethod
    cdef Transactions of_buffer(bytes buf, uint32_t total, size_t * offset):
        ret = []
        count = 0
        while count != total:
            r = LazyTransaction(buf, offset[0] , count)
            ret.append(r)
            offset[0] = Transaction.buffer_length(buf, offset[0], count)
            count += 1
        obj = Transactions(ret)

        return obj
    @staticmethod
    cdef size_t buffer_length(bytes buffer, uint32_t total, size_t offset):
        count = 0
        while count != total:
            offset = Transaction.buffer_length(buffer, offset , count)
            count += 1
        return offset


cdef class BlockHeader:
    cdef readonly uint32_t version
    cdef readonly bytes prev_block
    cdef readonly bytes merkle_root
    cdef readonly bytes block_hash
    cdef readonly uint32_t timestamp
    cdef readonly size_t bits
    cdef readonly uint32_t nonce
    cdef readonly uint32_t txn_count
    def __cinit__(self, version, prev_block, merkle_root,
                 block_hash, timestamp, bits, nonce,
                 txn_count):

        self.version = version
        self.prev_block = prev_block
        self.merkle_root = merkle_root
        self.block_hash = block_hash
        self.timestamp = timestamp
        self.bits = bits
        self.nonce = nonce
        self.txn_count = txn_count
    FIXED_HEADER_BYTES = 80 # 4+32+32+4+4+4
    FMT = Struct("<i 32s 32s I I I")
    @staticmethod
    cdef BlockHeader of_buffer(bytes data, size_t * offset):
        header = data[offset[0]:offset[0]+BlockHeader.FIXED_HEADER_BYTES]
        block_hash = double_sha256(header) 
        version, prev_block, merkle_root, timestamp, bits, nonce  =  BlockHeader.FMT.unpack( header)
        offset[0] += BlockHeader.FIXED_HEADER_BYTES
        txn_count = VAR_INT(data, offset)
        return BlockHeader(version, prev_block, merkle_root,
                     block_hash, timestamp, bits, nonce,
                           txn_count)
    property hex_block_hash:
        def __get__(self):
            return bytes(self.block_hash[::-1]).encode('hex')
    property hex_prev_block:
        def __get__(self):
            return bytes(self.prev_block[::-1]).encode('hex')
    property hex_merkle_root:
        def __get__(self):
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
            

cdef class LazyBlock:
    cdef bytes a
    cdef size_t offset
    def __cinit__(self, bytes a, size_t o):
        self.a=a
        self.offset = o
    def __call__(self):
        return Block.of_buffer(self.a, &self.offset)
cdef class Block:
    cdef readonly BlockHeader header
    cdef readonly LazyTransactions txns
    cdef readonly uint32_t height
    def __init__(self, header=None, txns=None):
        self.header = header
        self.txns = txns
        self.height = -1
    cdef setHeight(self, h):
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
    cdef Block of_buffer(bytes data, size_t * offset):
        cdef BlockHeader header = BlockHeader.of_buffer(data, offset)
        cdef LazyTransactions txns= LazyTransactions(data, header.txn_count, offset[0])
        return Block(header, txns)


cdef class Blocks:
    @staticmethod
    def of_buffer(bytes data, size_t offset, size_t size, err=None):
        try:
            while size - offset > 8:
                #TODO: What if the magic is not there?
                magic = UINT32_T(data, &offset)#m)
                if magic != 0xD9B4BEF9:
                    offset += 1
                    continue
                offset+=4
                n_bytes = UINT32_T(data, &offset)#data[offset:offset+4])
                offset+=4
                x = offset+n_bytes
                lazy_block = LazyBlock(data,offset)# don't need end , x) # unhex is smaller
                offset = x
                yield lazy_block
        except Exception as e:
            sys.stdout.write("FATAL %s:\n"%e)
            raise ValueError("Malformed file %s"%err)
        raise StopIteration

    @staticmethod
    def of_file(name, iterator=True):
        size = os.path.getsize(name)
        cdef size_t offset = 0
        with open(name) as f: # this gets rid of some allocations I think!
            data = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            if iterator:
                return Blocks.of_buffer(data, offset, size)
            else:
                return list(Blocks.of_buffer(data, offset, size))
        
    @staticmethod
    def of_file_full(name, iterator=True):
        size = os.path.getsize(name)
        cdef size_t offset = 0
        with open(name) as f: # this gets rid of some allocations I think!
            data = f.read()
            if iterator:
                return Blocks.of_buffer(data, offset, size)
            else:
                return list(Blocks.of_buffer(data, offset, size))

def main(args):
    for i in args:
        # print i
        e = Blocks.of_file_full(i,  True)
        a = e.next()
        z = a()

        print "block meth", dir(z)
        print z.header
        b =z.txns()
        print "txns meth", dir(b)
        print b
        print b[0]()
        "Checking available methods"
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






