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
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from BTCLib.blockchain import *
from BTCLib.bitcoinscript import Script
from os import urandom
import os
from datetime import datetime 
import re
def result_name_maker( hdfs =os.environ["HDFS_URL"]):
    d = datetime.now()
    dstr = "-".join(map(str, [d.year, d.month,d.day,d.hour, d.minute, d.second]))
    entropy =urandom(4).encode('hex')
    results_dir =  "%s/media/ephemeral0/results/%s__%s/%s"%(hdfs, dstr, entropy, "%s")
    def inner(name):
        return results_dir%name
    return inner


from ast import literal_eval
class SparkBlock:
    def __init__(self):
#        self.conf = SparkConf().set("spark.executor.memory", "500M").set("spark.driver.memory", "2g")

        self.logFile = "logfile"  # Should be some file on your system
        self.sc = SparkContext(appName="SparkBlock Analysis")
        with open("/root/credentials/creds.txt") as f:
            self.ACCESS_KEY = f.readline().strip()#.replace("/", "%2F")
            self.SECRET_KEY = f.readline().strip()#.replace("/", "%2F")
            hconf = self.sc._jsc.hadoopConfiguration()
            hconf.set("fs.s3n.awsAccessKeyId", self.ACCESS_KEY)
            hconf.set("fs.s3n.awsSecretAccessKey", self.SECRET_KEY)
        self.chain = None
        self.logger = self.sc._jvm.org.apache.log4j.LogManager
    def no_warn(self):
        self.logger.getLogger("org").setLevel( logger.Level.ERROR )
        self.logger.getLogger("akka").setLevel( logger.Level.ERROR )
    def read_block_dir(self, uri):
        blocks = self.sc.wholeTextFiles(uri)
        blockobjs = blocks.map(lambda (n,c): (n, Block.of_string(c.strip(),0)[0]))
        return blockobjs
    def get_files(self, uri):
        from boto.s3.connection import S3Connection

        conn = S3Connection(self.ACCESS_KEY, self.SECRET_KEY)
        bucket = conn.get_bucket(uri)
        for key in bucket.list():
            yield key
    def fetch_chain(self):
        if self.chain is None:
            self.chain = sb.sc.binaryFiles(
                os.environ["HDFS_URL"]+"/media/ephemeral0/blocks/"
                ).flatMap(lambda (name,blk): Blocks.of_buffer(blk,0,len(blk), err=name))
        return self.chain
class Node:
    def __init__(self, parent, node):
        self.parent = parent
        self.node = node
        self.height = None
    def set_height(self, chain):
        try:
            next_tmp = chain[self.parent]
            if next_tmp.height is None:
                self.height = 1+ next_tmp.set_height(chain)
            else:
                self.height = next_tmp.height +1
        except KeyError:
            # Base Case
            self.height = 0
        return self.height


if __name__ == "__main__":
    import sys
    result_name = result_name_maker()
    sb = SparkBlock()
    # sb.no_warn()
    block_objs = sb.fetch_chain()

    # sys.setrecursionlimit(10**6)
    # parent_child = block_objs.map(lambda b: b()).map(lambda b: ( b.header.prev_block, b.header.block_hash)).collect()
    # chain = dict((child, Node(parent, child)) for parent, child in parent_child)
    # for _, v in chain.iteritems():
    #     v.set_height(chain) 
    # height, longest = max((v.height, v) for _, v in chain.iteritems)


    # print height, longest


    # txns = block_objs.flatMap(lambda b: 
    #                       b.txns)
    # # Transaction Output Amount Distribution
    # txns.flatMap(lambda txn:
    #              map(lambda txo:
    #                  ((txo.value>>14)<<14, 1),
    #              txn.tx_outs))\
    #     .reduceByKey(lambda x,y: x+y)\
    #     .saveAsTextFile(result_name("txouts_values"))
            
    # unlazy = lambda x: x()
    # coinbases = block_objs.map(unlazy)\
    #                       .map(lambda b: b.txns()[0]().tx_ins()[0]().signature_script)\
    #                       .filter(lambda f: "BIP100" in f)\
    #                       .saveAsTextFile(result_name("BIP100_Blocks"))
    coinbases = block_objs.map(lambda f: f.txns[0].tx_ins[0].signature_script).filter(lambda f: "BIP100" in f)\
                                                                              .saveAsTextFile(result_name("BIP100_Blocks"))
    while True:
        pass
    txns = block_objs.map(unlazy)\
                     .flatMap(lambda b: 
                          b.txns())\
                     .map(unlazy)
    # Transaction Output Amount Distribution
    txns.flatMap(lambda txn:
                 map(lambda txo:
                     ((txo.value>>14)<<14, 1),
                 txn.tx_outs().map(unlazy)))\
        .reduceByKey(lambda x,y: x+y)\
        .saveAsTextFile(result_name("txouts_values"))
    # Precompute
    get_type_val = lambda txo: (re.sub("<(.*?)>", "<>",
                                       txo.pk_script_parsed),
                                txo.value)
    address_types = txns.flatMap(lambda txn:
                                 map(get_type_val,
                                     txn.tx_outs()\
                                     .map(unlazy)))\
                        .cache()

    # How much Bitcoin is held per address type?
    address_types.reduceByKey(lambda x,y: x+y)\
                 .saveAsTextFile(result_name("btc_addr_dist"))

    # How many addresses of each type are there?
    address_types.countByKey()\
                 .saveAsTextFile(result_name("address_count"))

#     txns = block_objs.flatMap(lambda b: iter(b().txns()))#.cache()
#     """TXOUT DISTRIBUTION"""
#     f = txns.flatMap(lambda txn: txn().tx_outs().map(
#         lambda l_txo: 
#             (lambda txo: (txo.value, 1))(l_txo()))   ).reduceByKey(lambda x,y: x+y)
#     f.saveAsTextFile(result_name("txouts_values.dat"))

#    """How many Bitcoin are held in what types of addresses? """
#    addr_form = lambda x: re.sub("<(.*?)>", "<>", x)
#    address_types = txns.flatMap(lambda txn: txn.tx_outs.map(lambda txo: (addr_form(txo.pk_script_parsed), txo.value)  ))#.cache()
#    btc_in_addr = address_types.reduceByKey(lambda x,y: x+y)
#    btc_in_addr.saveAsTextFile(result_name("btc_in_addr_type_pop.dat"))

    # How many addresses of each type are there?
    # For both above I'm mostly interested in pay-to-sig, pay-to-sighash, pay-to-scripthash and pay-to-multisig
    # address_type_popularity = address_types.countByKey()
    # address_type_popularity.saveAsTextFile("s3n://bitcoinresults/%s/addr_type_pop.dat"%name)
#    address_types.unpersist()
    # What is the typical # of signatures needed for pay-to-multisig? (provided we have script on the blockchain...)
    
    # def filter_map_add_OP_FALSE(txn):
    #     for txi in txn.tx_ins:
    #         try:
    #             a = addr_form(txi.signature_script_parsed)
    #             # if txi.signature_script.startswith(str(chr(Script.OP_FALSE))):
    #             yield (a,1)
    #         except:
    #             sys.stdout.write("script: "+txi.hex_signature_script+"\n")
    # multisig_dist = txns.flatMap(filter_map_ad).reduceByKey(lambda x,y: x+y)##.cache()
    # multisig_dist.saveAsTextFile(result_name("multisig_dist.dat"))
    # A histogram of #addresses/money held on one axis and #signatures on the other
    # What is the money distribution? E.g. what percentage of addrs have more than 10 cents in them?
    # What is the # of UTXOs/value held in them, for which we know scripthashes and for which we don't?
    sb.sc.stop()
# bobjs = sb.read_block_dir("block/")
# txs = bobjs.flatMap(lambda (n,x): x.txns.to_list()).cache()
# t = txs.collect()
# print t[:10]
#import lsm
#txdb = lsm.LSM("dbs/tx_index.db")
#txdb = lsm.LSM("dbs/tx_parent.db")
#KEY_SIZE = 5
#def put_txs(e,db):
#    with db.transaction() as t:
#        for tx in e.txns:
#            key = tx.tx_id[:KEY_SIZE]
#            try:
#                l = literal_eval(db[key])
#                if i not in l:
#                    l.append(i)
#                    db[key] =  str(l)
#            except KeyError:
#                db[key]=str([i])
#
#
#def tx_parent(e,db):
#    with db.transaction() as t:
#        for tx in e.txns:
#            for child in tx.tx_ins:
#                parent_key = tx.tx_id.previous_output.hash[:KEY_SIZE]
#                try:
#                    l = literal_eval(db[parent_key])
#                    if i not in l:
#                        l.append(i)
#                        db[key] =  str(l)
#                except KeyError:
#                    db[key]=str([i])
#g = None
#for i in sys.argv[1:]:
#    # print i
#    e, _ = Block.of_file(i)
#    g = e.txns[-1]
#    put_txs(e,txdb)
#print Block.of_file(txdb[g.tx_id[:KEY_SIZE]])

# n_txs_by_n_inputs = blockJSONs.flatMap(lambda f: f.txns.to_list()).map(lambda x: (x.tx_in_count, 1)).reduceByKey(lambda x,y: x+y)

# odd_tx = blockJSONs.flatMap(lambda f: f.txns.to_list()).filter(lambda x: x.tx_in_count == 620)

# CHILD = True
# PARENT = not CHILD
# parent_child = blockJSONs.flatMap(lambda f: [(f.header.prev_block, (CHILD,f)),
#                                              (f.header.block_hash, (PARENT,f))]).reduceByKey(
#                                                  lambda (i,x),(j,y): (y,x) if j == PARENT else (x,y) ).cache()
# parent_or_child = parent_child.filter(lambda (h, (k,v)): k == PARENT or k == CHILD).map(lambda (k,v): (k, list(v)))
# others = parent_child.filter(lambda (k, _): k not in [PARENT, CHILD]).map(lambda (k,v): (k, list(v)))

# coinbases = blockJSONs.map(lambda f: f.txns[0].tx_ins[0].signature_script).cache()
# bip100 = coinbases.filter(lambda f: "BIP100" in f)

# def t():
#     acc = sc.accumulator(0)
#     acc2 = sc.accumulator(0)
#     txn_n = blockJSONs.foreach(lambda x: acc.add(x.header.txn_count))
#     use_more_than_one_utxo = blockJSONs.foreach(lambda x: acc2.add(len(x.txns.filter(lambda f: len(f.tx_ins) > 1))))
#     return acc2.value/float(acc.value)
# def t():
#     acc = sc.accumulator(0)
#     acc2 = sc.accumulator(0)
#     # txn_n = blockJSONs.foreach(lambda x: acc.add(x.header.txn_count))
#     use_more_than_one_utxo = blockJSONs.foreach(lambda x: x.txns.map(lambda f: acc2.add(len(f.tx_ins) -1)))
#     return acc2.value
# if __name__ == "__main__":
#     import time
#     from datetime import datetime
#     time.sleep(3)
#     d = datetime.now()
#     print t()
#     print datetime.now()-d
#     import sys
#     sys.exit()
        
# # txns = blockJSONs.sample(False, 0.1).flatMap(lambda x: x.txns.to_list()).cache()
# # txn2 = txns.flatMap(lambda x: x.tx_outs.map(lambda y: (y.pk_script_parsed(), y.parent))).cache()
# # txn3 = txn2.filter(lambda (x,y): not x.startswith("OP_DUP OP_HASH160")).top(5)

# # parent_child2 = blockJSONs.cartesian(blockJSONs).filter(lambda (x,y): x.header.prev_block == y.header.block_hash)

# # c = parent_child.collect()
# # kvparent = dict(c)
# # TOP = None
# # print kvparent
# # TOP = filter(lambda (k,(parent,child)): not (parent.header.prev_block in kvparent), kvparent.iteritems())

# # t = TOP
# # n = 0
# # while True:
# #     t.setHeight(n)
# #     n+=1
# #     try:
# #         t = kvparent[t.block_hash][1]
# #     except KeyError:
# #         break


                                            

# # print "Available Vars"

# # for v in set(locals().keys()) - s:
# #     print v

# # import code
# # code.interact(local=dict(globals(), **locals()))









