import pickle

import pytest

from bloom import Bloom

def test_no_false_negatives():
    b = Bloom(1000)
    for i in range(1000): b.add(i)
    assert all(i in b for i in range(1000))

def test_fp_rate_low():
    b = Bloom(1000, false_positive_rate=0.01)
    for i in range(1000): b.add(i)
    fp = sum(1 for i in range(1000, 3000) if i in b) / 2000
    assert fp < 0.05


@pytest.mark.parametrize("expected_items", [0, -1])
def test_invalid_capacity_is_rejected(expected_items):
    with pytest.raises(ValueError, match="expected_items"):
        Bloom(expected_items)


@pytest.mark.parametrize("rate", [0, 1, -0.1, 1.1, float("nan")])
def test_invalid_false_positive_rate_is_rejected(rate):
    with pytest.raises(ValueError, match="false_positive_rate"):
        Bloom(1, false_positive_rate=rate)


def test_tiny_filter_has_at_least_one_bit():
    bloom = Bloom(1, false_positive_rate=0.99)
    bloom.add(42)
    assert 42 in bloom


def test_legacy_pickle_keeps_original_hash_layout():
    bloom = Bloom(1000)
    for key in range(1000):
        bloom.add(key)
    original_bit_count = bloom.bit_count
    original_hash_count = bloom.hash_count
    bloom.__dict__["m"] = bloom.__dict__.pop("bit_count")
    bloom.__dict__["k"] = bloom.__dict__.pop("hash_count")

    restored = pickle.loads(pickle.dumps(bloom))

    assert restored.bit_count == original_bit_count
    assert restored.hash_count == original_hash_count
    assert all(key in restored for key in range(1000))