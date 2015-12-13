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
class Script:
    OP_0 = 0x00
    OP_FALSE = OP_0
    OP_PUSHDATA1 = 0x4c
    OP_PUSHDATA2 = 0x4d
    OP_PUSHDATA4 = 0x4e
    OP_1NEGATE = 0x4f
    OP_RESERVED = 0x50
    OP_1 = 0x51
    OP_TRUE=OP_1
    OP_2 = 0x52
    OP_3 = 0x53
    OP_4 = 0x54
    OP_5 = 0x55
    OP_6 = 0x56
    OP_7 = 0x57
    OP_8 = 0x58
    OP_9 = 0x59
    OP_10 = 0x5a
    OP_11 = 0x5b
    OP_12 = 0x5c
    OP_13 = 0x5d
    OP_14 = 0x5e
    OP_15 = 0x5f
    OP_16 = 0x60
    # control
    OP_NOP = 0x61
    OP_VER = 0x62
    OP_IF = 0x63
    OP_NOTIF = 0x64
    OP_VERIF = 0x65
    OP_VERNOTIF = 0x66
    OP_ELSE = 0x67
    OP_ENDIF = 0x68
    OP_VERIFY = 0x69
    OP_RETURN = 0x6a
    # stack ops
    OP_TOALTSTACK = 0x6b
    OP_FROMALTSTACK = 0x6c
    OP_2DROP = 0x6d
    OP_2DUP = 0x6e
    OP_3DUP = 0x6f
    OP_2OVER = 0x70
    OP_2ROT = 0x71
    OP_2SWAP = 0x72
    OP_IFDUP = 0x73
    OP_DEPTH = 0x74
    OP_DROP = 0x75
    OP_DUP = 0x76
    OP_NIP = 0x77
    OP_OVER = 0x78
    OP_PICK = 0x79
    OP_ROLL = 0x7a
    OP_ROT = 0x7b
    OP_SWAP = 0x7c
    OP_TUCK = 0x7d
    # splice ops
    OP_CAT = 0x7e
    OP_SUBSTR = 0x7f
    OP_LEFT = 0x80
    OP_RIGHT = 0x81
    OP_SIZE = 0x82
    # bit logic
    OP_INVERT = 0x83
    OP_AND = 0x84
    OP_OR = 0x85
    OP_XOR = 0x86
    OP_EQUAL = 0x87
    OP_EQUALVERIFY = 0x88
    OP_RESERVED1 = 0x89
    OP_RESERVED2 = 0x8a
    # numeric
    OP_1ADD = 0x8b
    OP_1SUB = 0x8c
    OP_2MUL = 0x8d
    OP_2DIV = 0x8e
    OP_NEGATE = 0x8f
    OP_ABS = 0x90
    OP_NOT = 0x91
    OP_0NOTEQUAL = 0x92
    OP_ADD = 0x93
    OP_SUB = 0x94
    OP_MUL = 0x95
    OP_DIV = 0x96
    OP_MOD = 0x97
    OP_LSHIFT = 0x98
    OP_RSHIFT = 0x99
    OP_BOOLAND = 0x9a
    OP_BOOLOR = 0x9b
    OP_NUMEQUAL = 0x9c
    OP_NUMEQUALVERIFY = 0x9d
    OP_NUMNOTEQUAL = 0x9e
    OP_LESSTHAN = 0x9f
    OP_GREATERTHAN = 0xa0
    OP_LESSTHANOREQUAL = 0xa1
    OP_GREATERTHANOREQUAL = 0xa2
    OP_MIN = 0xa3
    OP_MAX = 0xa4
    OP_WITHIN = 0xa5
    # crypto
    OP_RIPEMD160 = 0xa6
    OP_SHA1 = 0xa7
    OP_SHA256 = 0xa8
    OP_HASH160 = 0xa9
    OP_HASH256 = 0xaa
    OP_CODESEPARATOR = 0xab
    OP_CHECKSIG = 0xac
    OP_CHECKSIGVERIFY = 0xad
    OP_CHECKMULTISIG = 0xae
    OP_CHECKMULTISIGVERIFY = 0xaf
    # expansion
    OP_NOP1 = 0xb0
    OP_NOP2 = 0xb1
    OP_CHECKLOCKTIMEVERIFY = OP_NOP2
    OP_NOP3 = 0xb2
    OP_NOP4 = 0xb3
    OP_NOP5 = 0xb4
    OP_NOP6 = 0xb5
    OP_NOP7 = 0xb6
    OP_NOP8 = 0xb7
    OP_NOP9 = 0xb8
    OP_NOP10 = 0xb9
    # template matching params
    OP_SMALLINTEGER = 0xfa
    OP_PUBKEYS = 0xfb
    OP_PUBKEYHASH = 0xfd
    OP_PUBKEY = 0xfe
    OP_INVALIDOPCODE = 0xff
    #0x00 = "OP_0 ",


    opcode_name =     {0    : "OP_FALSE",
                       0x4c : "OP_PUSHDATA1",
                       0x4d : "OP_PUSHDATA2",
                       0x4e : "OP_PUSHDATA4",
                       0x4f : "OP_1NEGATE",
                       0x50 : "OP_RESERVED",
                       0x51 : "OP_1",
                       0x52 : "OP_2",
                       0x53 : "OP_3",
                       0x54 : "OP_4",
                       0x55 : "OP_5",
                       0x56 : "OP_6",
                       0x57 : "OP_7",
                       0x58 : "OP_8",
                       0x59 : "OP_9",
                       0x5a : "OP_10",
                       0x5b : "OP_11",
                       0x5c : "OP_12",
                       0x5d : "OP_13",
                       0x5e : "OP_14",
                       0x5f : "OP_15",
                       0x60 : "OP_16",
                       # control,
                       0x61 : "OP_NOP",
                       0x62 : "OP_VER",
                       0x63 : "OP_IF",
                       0x64 : "OP_NOTIF",
                       0x65 : "OP_VERIF",
                       0x66 : "OP_VERNOTIF",
                       0x67 : "OP_ELSE",
                       0x68 : "OP_ENDIF",
                       0x69 : "OP_VERIFY",
                       0x6a : "OP_RETURN",
                       # stack ops,
                       0x6b : "OP_TOALTSTACK",
                       0x6c : "OP_FROMALTSTACK",
                       0x6d : "OP_2DROP",
                       0x6e : "OP_2DUP",
                       0x6f : "OP_3DUP",
                       0x70 : "OP_2OVER",
                       0x71 : "OP_2ROT",
                       0x72 : "OP_2SWAP",
                       0x73 : "OP_IFDUP",
                       0x74 : "OP_DEPTH",
                       0x75 : "OP_DROP",
                       0x76 : "OP_DUP",
                       0x77 : "OP_NIP",
                       0x78 : "OP_OVER",
                       0x79 : "OP_PICK",
                       0x7a : "OP_ROLL",
                       0x7b : "OP_ROT",
                       0x7c : "OP_SWAP",
                       0x7d : "OP_TUCK",
                       # splice ops,
                       0x7e : "OP_CAT",
                       0x7f : "OP_SUBSTR",
                       0x80 : "OP_LEFT",
                       0x81 : "OP_RIGHT",
                       0x82 : "OP_SIZE",
                       # bit logic,
                       0x83 : "OP_INVERT",
                       0x84 : "OP_AND",
                       0x85 : "OP_OR",
                       0x86 : "OP_XOR",
                       0x87 : "OP_EQUAL",
                       0x88 : "OP_EQUALVERIFY",
                       0x89 : "OP_RESERVED1",
                       0x8a : "OP_RESERVED2",
                       # numeric,
                       0x8b : "OP_1ADD",
                       0x8c : "OP_1SUB",
                       0x8d : "OP_2MUL",
                       0x8e : "OP_2DIV",
                       0x8f : "OP_NEGATE",
                       0x90 : "OP_ABS",
                       0x91 : "OP_NOT",
                       0x92 : "OP_0NOTEQUAL",
                       0x93 : "OP_ADD",
                       0x94 : "OP_SUB",
                       0x95 : "OP_MUL",
                       0x96 : "OP_DIV",
                       0x97 : "OP_MOD",
                       0x98 : "OP_LSHIFT",
                       0x99 : "OP_RSHIFT",
                       0x9a : "OP_BOOLAND",
                       0x9b : "OP_BOOLOR",
                       0x9c : "OP_NUMEQUAL",
                       0x9d : "OP_NUMEQUALVERIFY",
                       0x9e : "OP_NUMNOTEQUAL",
                       0x9f : "OP_LESSTHAN",
                       0xa0 : "OP_GREATERTHAN",
                       0xa1 : "OP_LESSTHANOREQUAL",
                       0xa2 : "OP_GREATERTHANOREQUAL",
                       0xa3 : "OP_MIN",
                       0xa4 : "OP_MAX",
                       0xa5 : "OP_WITHIN",
                       # crypto,
                       0xa6 : "OP_RIPEMD160",
                       0xa7 : "OP_SHA1",
                       0xa8 : "OP_SHA256",
                       0xa9 : "OP_HASH160",
                       0xaa : "OP_HASH256",
                       0xab : "OP_CODESEPARATOR",
                       0xac : "OP_CHECKSIG",
                       0xad : "OP_CHECKSIGVERIFY",
                       0xae : "OP_CHECKMULTISIG",
                       0xaf : "OP_CHECKMULTISIGVERIFY",
                       # expansion,
                       0xb0 : "OP_NOP1",
                       # 0xb1 : "OP_NOP2",
                       0xb1 : "OP_CHECKLOCKTIMEVERIFY",
                       0xb2 : "OP_NOP3",
                       0xb3 : "OP_NOP4",
                       0xb4 : "OP_NOP5",
                       0xb5 : "OP_NOP6",
                       0xb6 : "OP_NOP7",
                       0xb7 : "OP_NOP8",
                       0xb8 : "OP_NOP9",
                       0xb9 : "OP_NOP10",
                       # template matching params,
                       0xfa : "OP_SMALLINTEGER",
                       0xfb : "OP_PUBKEYS",
                       0xfd : "OP_PUBKEYHASH",
                       0xfe : "OP_PUBKEY",
                       0xff : "OP_INVALIDOPCODE "}

    @classmethod
    def convert_byte(cls, x):
        try:
            o  = x if isinstance(x,int) else ord(x)
            return cls.opcode_name[o]
        except KeyError:
            return "OP_%s_MYSTERY"%x
    @classmethod
    def convert_script(cls, x):
        st = []
        count = 0
        while count < len(x):
            b = x[count]
            o  = b if isinstance(b,int) else ord(b)
            if 0 < o <79:
                n_bytes, count = SCRIPT_VAR_INT(x, count)
                st.append("<%s>"%(bytes(x[count:count+n_bytes]).encode('hex')))
                count+=n_bytes
            else:
                st.append(Script.convert_byte(b))
                count +=1
        return " ".join(st)
