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

def pack_frame(payload: bytes, is_randomized: bool = True) -> bytes:
    if is_randomized:
        if len(payload) == 396:
            pad_size = 0
        else:
            pad_size = random.randint(0, 3)
        padding = os.urandom(pad_size)
        return struct.pack("<i", len(payload) + pad_size) + payload + padding
    else:
        return struct.pack("<i", len(payload)) + payload

def unpack_frame(frame_body: bytes, is_randomized: bool = True) -> bytes:
    if is_randomized:
        pad_size = len(frame_body) % 4
        if pad_size > 0:
            return frame_body[:-pad_size]
    return frame_body

# Backward-compatibility aliases
pack_padded_frame = pack_frame
unpack_padded_frame = unpack_frame

class MTProxyObfuscator:
    def __init__(self, secret: bytes, dc_idx: int = 2):
        if secret.startswith(b"\xdd") and len(secret) == 17:
            self.is_randomized = True
            secret = secret[1:]
            tag = b"\xdd\xdd\xdd\xdd"
        else:
            self.is_randomized = False
            tag = b"\xee\xee\xee\xee"

        keywords = (b"PVrG", b"GET ", b"POST", b"\xee\xee\xee\xee")
        while True:
            init = bytearray(os.urandom(64))
            if (
                init[0] != 0xef
                and bytes(init[0:4]) not in keywords
                and bytes(init[4:8]) != b"\0\0\0\0"
            ):
                break

        rev = bytes(init[55:7:-1])
        ek = hashlib.sha256(bytes(init[8:40]) + secret).digest()
        eiv = bytes(init[40:56])
        dk = hashlib.sha256(rev[:32] + secret).digest()
        div = rev[32:48]

        init[56:60] = tag
        init[60:62] = struct.pack("<h", dc_idx)

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

