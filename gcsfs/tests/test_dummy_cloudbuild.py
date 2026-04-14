import pytest

def test_pass_1(): assert True
def test_pass_2(): assert True
def test_pass_3(): assert True
def test_pass_4(): assert True
def test_pass_5(): assert True
def test_pass_6(): assert True
def test_pass_7(): assert True
def test_pass_8(): assert True
def test_pass_9(): assert True

def test_fail_1():
    assert False, "Deliberate failure to test Cloud Build cleanup"
