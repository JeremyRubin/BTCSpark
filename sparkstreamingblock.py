from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from blockchain import *

logFile = "logfile"  # Should be some file on your system
sc = SparkContext("local[1]", "Simple App")

logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

ssc = StreamingContext(sc, 1)

blocks =   ssc.textFileStream("file:///Users/jeremyrubin/Desktop/sparkybitcoin/test")
# blocks = sc.wholeTextFiles("blocks")

blockJSONs = blocks.map(lambda c: Block.of_string(c,0))


# blocks.pprint()

n_txs_by_n_inputs = blockJSONs.flatMap(lambda f: f.txns.to_list()).map(lambda x: (x.tx_in_count, 1)).reduceByKey(lambda x,y: x+y)
n_txs_by_n_inputs.pprint()#saveAsTextFiles("outputs/HE", "txt")
#print n_txs_by_n_inputs.collect()


ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate






