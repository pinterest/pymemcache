import pytest

from unittest.mock import MagicMock, patch

from pymemcache import Client
from pymemcache.client.ext.aws_ec_client import AWSElastiCacheHashClient

from .test_client import MockSocketModule, MockMemcacheClient


@pytest.mark.unit()
@pytest.mark.parametrize(
    "connection_sting",
    ['cluster.abcxyz.cfg.use1.cache.amazonaws.com:11211', '1.1.1.1:11211']
)
def test_init_valid_node_endpoint(connection_sting, monkeypatch):
    with patch.object(AWSElastiCacheHashClient, 'reconfigure_nodes', new=MagicMock()) as mock:
        client = AWSElastiCacheHashClient(
            connection_sting,

            socket_module=MockSocketModule()
        )

    assert client._cfg_node == connection_sting
    assert mock.called


@pytest.mark.unit()
@pytest.mark.parametrize(
    "connection_sting",
    [
        'cluster.abcxyz.cfg.use1.cache.amazonaws.com:abc',
        'cluster.abcxyz.cfg.use1.cache.amazonaws.com',
        'cluster.abcxyz.cfg.use1.cache.amazonaws.com:123123',
        '1.1..1:11211',
    ]
)
def test_init_invalid_node_endpoint(connection_sting, monkeypatch):
    with patch.object(AWSElastiCacheHashClient, 'reconfigure_nodes', new=MagicMock()) as mock:
        with pytest.raises(ValueError):
            AWSElastiCacheHashClient(
                connection_sting,
                socket_module=MockSocketModule()
            )


@pytest.mark.parametrize(
    'server_configuration', [
        (True, ['10.0.0.1:11211', '10.0.0.2:11211']),
        (False, [
            'cluster.abcxyz.0001.use1.cache.amazonaws.com:11211',
            'cluster.abcxyz.0002.use1.cache.amazonaws.com:11211',
        ]),
    ],
)
@pytest.mark.unit()
def test_get_cluster_config_command(server_configuration, monkeypatch):
    use_vpc, configuration_list = server_configuration

    raw_command = MagicMock(
        return_value=b'CONFIG cluster 0 139\r\n'
                     b'4\n'
                     b'cluster.abcxyz.0001.use1.cache.amazonaws.com|10.0.0.1|11211 '
                     b'cluster.abcxyz.0002.use1.cache.amazonaws.com|10.0.0.2|11211'
    )

    with monkeypatch.context() as ctx:
        ctx.setattr(Client, 'raw_command', raw_command)
        ctx.setattr(AWSElastiCacheHashClient, 'client_class', MockMemcacheClient)

        client = AWSElastiCacheHashClient(
            'cluster.abcxyz.cfg.use1.cache.amazonaws.com:11211',
            socket_module=MockSocketModule(),
            use_vpc=use_vpc,
        )

        for name, client in client.clients.items():
            assert isinstance(client, MockMemcacheClient)
            assert name in configuration_list

    assert raw_command.called
