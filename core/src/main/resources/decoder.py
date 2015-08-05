#!/usr/bin/env python2.7
import base64
import subprocess
import random
import sys


def protoc(fp):
    cmd = '/usr/local/bin/protoc --decode_raw < {}'.format(fp)
    print('command is {}'.format(cmd))
    return subprocess.call(cmd, shell=True)


def manipulate(msg):
    msg = msg[1:]
    msg = msg + '=' * (4 - len(msg) % 4)
    print('msg after manipulation: \n{}'.format(msg))
    return msg


def decode(msg):
    fn = '/tmp/{}.bin'.format(random.randint(0, 999))
    with open(fn, 'w') as f:
        decoded = base64.b64decode(manipulate(msg), '_-')
        f.write(decoded)
    protoc(fn)

if __name__ == '__main__':
    print('trying to decode {}'.format(sys.argv[1]))
    decode(sys.argv[1])
