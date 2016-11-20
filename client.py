#!/usr/bin/env python
import ctypes
import socket
import struct
import sys

usize_size = ctypes.sizeof(ctypes.c_void_p)

def recvframe(s):
    frame_size = struct.unpack("<I", s.recv(usize_size))
    buf = s.recv(frame_size)
    assert len(buf) == frame_size
    return buf

def sendframe(s, data):
    frame_size = struct.pack("<I", len(data))
    s.sendall(frame_size+data)

def main(host, port):
    s = socket.socket()
    s.connect((host, port))

if __name__ == '__main__':
    host = sys.argv[1]
    port = int(sys.argv[2], 10)
    main(host, port)
