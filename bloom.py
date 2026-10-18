import hashlib
import math

import config


class Bloom:
    def __init__(self, expected_items: int, false_positive_rate: float = config.BLOOM_FALSE_POSITIVE_RATE) -> None:
        if expected_items <= 0:
            raise ValueError("expected_items must be greater than zero")
        if not 0 < false_positive_rate < 1:
            raise ValueError("false_positive_rate must be between zero and one")
        bit_count = -expected_items * math.log(false_positive_rate) / (math.log(2) ** 2)
        self.bit_count: int = max(1, math.ceil(bit_count))
        self.hash_count: int = max(1, round((bit_count / expected_items) * math.log(2)))
        self.bits: bytearray = bytearray((self.bit_count + 7) // 8)

    def __setstate__(self, state: dict[str, object]) -> None:
        """Load legacy filters without changing their bit layout or hash count."""
        state = state.copy()
        for old_name, new_name in (("m", "bit_count"), ("k", "hash_count")):
            if new_name not in state and old_name in state:
                state[new_name] = state.pop(old_name)
        self.__dict__.update(state)

    def _hashes(self, key: object) -> list[int]:
        digest = hashlib.md5(str(key).encode()).digest()
        first_hash = int.from_bytes(digest[:4], "big")
        second_hash = int.from_bytes(digest[4:8], "big") | 1
        return [
            (first_hash + index * second_hash) % self.bit_count
            for index in range(self.hash_count)
        ]

    def add(self, key: object) -> None:
        for bit_index in self._hashes(key):
            self.bits[bit_index >> 3] |= 1 << (bit_index & 7)
    
    def __contains__(self, item: object) -> bool:
        return all(
            self.bits[bit_index >> 3] >> (bit_index & 7) & 1
            for bit_index in self._hashes(item)
        )