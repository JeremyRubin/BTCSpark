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
