from native_lazy_blockchain import *

import sys
if __name__ == "__main__":
    for i in sys.argv[1:]:
        # print i
        print i
        e = Blocks.of_file_full(i,  True)
        print e
        a = e.next()
        z = a()

        print z.header
        b =z.txns
        print b
        print len(b)
        print b[0]().tx_ins
        "Checking available methods"
