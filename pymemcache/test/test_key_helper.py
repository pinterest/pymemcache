from unittest import TestCase

from pymemcache import Client
import pytest


class CustomizedClient(Client):

    def _key_helper(self, key, remapped_keys, prefixed_keys):
        return -1


@pytest.mark.unit()
class TestKeyHelper(TestCase):

    def test_customized_client(self):
        client = CustomizedClient(())
        key = client._key_helper('fruits', {'fruits': 'fruits'}, ['fruits'])
        assert key == -1

    def test_default_client(self):
        client = Client(())
        key = client._key_helper('fruits', {'fruits': 'fruits'}, ['fruits'])
        assert key == 'fruits'
