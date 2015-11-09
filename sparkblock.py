from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from blockchain import *


conf = SparkConf().set("spark.executor.memory", "2g")
logFile = "logfile"  # Should be some file on your system
sc = SparkContext("local[3]", "Simple App", conf=conf)

logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

s = None
s = set(locals().keys())
blocks = sc.wholeTextFiles("blocks")

blockJSONs = blocks.map(lambda (n,c): Block.of_string(c,0)[0])


# blocks.pprint()

n_txs_by_n_inputs = blockJSONs.flatMap(lambda f: f.txns.to_list()).map(lambda x: (x.tx_in_count, 1)).reduceByKey(lambda x,y: x+y)

odd_tx = blockJSONs.flatMap(lambda f: f.txns.to_list()).filter(lambda x: x.tx_in_count == 620)

CHILD = True
PARENT = not CHILD
parent_child = blockJSONs.flatMap(lambda f: [(f.header.prev_block, (CHILD,f)),
                                             (f.header.block_hash, (PARENT,f))]).reduceByKey(
                                                 lambda (i,x),(j,y): (y,x) if j == PARENT else (x,y) ).cache()
parent_or_child = parent_child.filter(lambda (h, (k,v)): k == PARENT or k == CHILD).map(lambda (k,v): (k, list(v)))
others = parent_child.filter(lambda (k, _): k not in [PARENT, CHILD]).map(lambda (k,v): (k, list(v)))

bip100blocks = blockJSONs.filter(lambda f: "BIP100" in f.txns[0].tx_ins[0].signature_script)
# txns = blockJSONs.sample(False, 0.1).flatMap(lambda x: x.txns.to_list()).cache()
# txn2 = txns.flatMap(lambda x: x.tx_outs.map(lambda y: (y.pk_script_parsed(), y.parent))).cache()
# txn3 = txn2.filter(lambda (x,y): not x.startswith("OP_DUP OP_HASH160")).top(5)

# parent_child2 = blockJSONs.cartesian(blockJSONs).filter(lambda (x,y): x.header.prev_block == y.header.block_hash)

# c = parent_child.collect()
# kvparent = dict(c)
# TOP = None
# print kvparent
# TOP = filter(lambda (k,(parent,child)): not (parent.header.prev_block in kvparent), kvparent.iteritems())

# t = TOP
# n = 0
# while True:
#     t.setHeight(n)
#     n+=1
#     try:
#         t = kvparent[t.block_hash][1]
#     except KeyError:
#         break


                                            

print "Available Vars"

for v in set(locals().keys()) - s:
    print v

# import code
# code.interact(local=dict(globals(), **locals()))









