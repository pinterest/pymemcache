from unittest import TestCase

from pymemcache import Client
import pytest


class CustomizedClient(Client):

    def _value_helper(self, expect_cas, line, buf, remapped_keys,
                      prefixed_keys):
        return 'key', 'value', None


@pytest.mark.unit()
class TestKeyHelper(TestCase):

    def test_customized_client(self):
        client = CustomizedClient(())
        key, value, buf = client._value_helper(False, None, None, None, None)
        assert key == 'key'
        assert value == 'value'
        assert buf is None
