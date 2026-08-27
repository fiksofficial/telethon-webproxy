import hashlib
import os
import struct
import random

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    HAS_CRYPTOGRAPHY = True
except ImportError:
    HAS_CRYPTOGRAPHY = False
    try:
        from pyaes import AESModeOfOperationCTR as AESModeCTR
    except ImportError:
        raise ImportError("Either cryptography or pyaes must be installed.")

def pack_padded_frame(payload: bytes) -> bytes:
    if len(payload) == 396:
        pad_size = 0
    else:
        pad_size = random.randint(1, 3)
    padding = os.urandom(pad_size)
    return struct.pack("<i", len(payload) + pad_size) + payload + padding

def unpack_padded_frame(frame_body: bytes) -> bytes:
    pad_size = len(frame_body) % 4
    if pad_size > 0:
        return frame_body[:-pad_size]
    return frame_body

class MTProxyObfuscator:
    def __init__(self, secret: bytes, dc_idx: int = 2):
        if secret.startswith(b"\xdd") and len(secret) == 17:
            secret = secret[1:]
        tag = b"\xdd\xdd\xdd\xdd"
        while True:
            init = bytearray(os.urandom(64))
            if (
                init[0] == 0xef
                or bytes(init[0:4]) in (b"\x48\x4f\x53\x54", b"\x50\x4f\x53\x54", b"\x47\x45\x54\x20", b"\xee\xee\xee\xee")
                or bytes(init[4:8]) == b"\0\0\0\0"
            ):
                continue
            break
        init[56:60] = tag
        init[60:62] = struct.pack("<h", dc_idx)
        ek = hashlib.sha256(bytes(init[8:40]) + secret).digest()
        eiv = bytes(init[40:56])
        rev = bytes(init[8:56])[::-1]
        dk = hashlib.sha256(rev[0:32] + secret).digest()
        div = rev[32:48]
        if HAS_CRYPTOGRAPHY:
            self.enc = Cipher(algorithms.AES(ek), modes.CTR(eiv)).encryptor()
            self.dec = Cipher(algorithms.AES(dk), modes.CTR(div)).decryptor()
            encrypted = self.enc.update(bytes(init))
            self.header = bytes(init[0:56]) + encrypted[56:64]
        else:
            self.enc = AESModeCTR(ek, eiv)
            self.dec = AESModeCTR(dk, div)
            encrypted = self.enc.encrypt(bytes(init))
            self.header = bytes(init[0:56]) + encrypted[56:64]
    def encrypt(self, data: bytes) -> bytes:
        if HAS_CRYPTOGRAPHY:
            return self.enc.update(data)
        return self.enc.encrypt(data)
    def decrypt(self, data: bytes) -> bytes:
        if HAS_CRYPTOGRAPHY:
            return self.dec.update(data)
        return self.dec.decrypt(data)
