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






