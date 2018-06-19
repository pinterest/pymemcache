"""
Backwards compatibility with the older serialization api previously
provided by this module.
"""
from pymemache import codecs

_SERDE = codecs.Serde()

python_memcache_serializer = _SERDE.serialize
python_memcache_deserializer = _SERDE.deserialize
