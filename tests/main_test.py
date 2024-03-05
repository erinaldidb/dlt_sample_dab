from dlt_sample import main

def test_main():
    taxis = main.get_taxis()
    assert taxis.count() > 5
