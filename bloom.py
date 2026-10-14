import hashlib, math

class Bloom:
    def __init__(self, n: int, p: float = 0.01) -> None:
        m = -n  * math.log(p) / (math.log(2) ** 2)
        self.m: int = int(m)
        self.k: int = max(1, round((m/n) * math.log(2)))
        self.bits: bytearray = bytearray((self.m + 7) // 8)

    def _hashes(self, key: object) -> list[int]:
        d = hashlib.md5(str(key).encode()).digest()
        h1 = int.from_bytes(d[:4], "big")
        h2 = int.from_bytes(d[4:8], "big") | 1
        return [(h1 + i * h2) % self.m for i in range(self.k)]

    def add(self, key: object) -> None:
        for b in self._hashes(key):
            self.bits[b >> 3] |= 1 << (b & 7)
    
    def __contains__(self, item: object) -> bool:
        return all(self.bits[b >> 3] >> (b & 7) & 1 for b in self._hashes(item))