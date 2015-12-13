
Bitcoin Spark Framework (BTCSpark)
==================================


What is BTCSpark?
-----------------

BTCSpark is a layer for accessing the Bitcoin Blockchain from
[https://github.com/apache/spark](Apache Spark).

The goal of BTCSpark is to offer high quality, easy to use, performant, and
free software to Bitcoin developers and analysts.

Benchmarks
----------

The following benchhmark finds the Transaction Output Amount Distribution
(TOAD). On an AWS 6 node (5 slave, one master) m3.large cluster, with the
blockchain in hadoop on ephemera storage, this take 8.4 minutes to run using
the nativ_lazy_blockchain implementation.

```
    block_objs = sb.fetch_chain()
    unlazy = lambda x: x()
    txns = block_objs.map(unlazy)\
                     .flatMap(lambda b: 
                          b.txns)\
                     .map(unlazy)
    txns.flatMap(lambda txn:
                 map(lambda txo:
                     ((txo.value>>14)<<14, 1),
                 txn.tx_outs.map(unlazy)))\
        .reduceByKey(lambda x,y: x+y)\
        .saveAsTextFile("txouts_values")
```

Finding the BIP100 Blocks takes 5.0 minutes on the same cluster.

```
    block_objs.map(unlazy)\
              .map(lambda b: b.txns[0]().tx_ins[0]().signature_script)\
              .filter(lambda f: "BIP100" in f)\
              .saveAsTextFile(result_name("BIP100_Blocks"))
```

Note: Unless you have a lot of memory, or you've reduced the working set largely,
it isn't recommended to use caching as the overhead of re-parsing isn't horrible.

License
-------

BTCSpark is released under the terms of the AGPL license. See
[COPYING](COPYING) for more information. Non-free license may also be purchased
from Jeremy Rubin for organizations who are unable to use AGPL licensed
software.

