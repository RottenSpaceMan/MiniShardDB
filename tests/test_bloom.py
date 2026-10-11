from bloom import Bloom

def test_no_false_negatives():
    b = Bloom(1000)
    for i in range(1000): b.add(i)
    assert all(i in b for i in range(1000))

def test_fp_rate_low():
    b = Bloom(1000, p = 0.01)
    for i in range(1000): b.add(i)
    fp = sum(1 for i in range(1000, 3000) if i in b) / 2000
    assert fp <  0.05