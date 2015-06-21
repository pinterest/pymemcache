import pytest
import socket


def pytest_addoption(parser):
    parser.addoption('--server', action='store',
                     default='localhost',
                     help='memcached server')

    parser.addoption('--port', action='store',
                     default='11211',
                     help='memcached server port')


@pytest.fixture(scope='session')
def host(request):
    return request.config.option.server


@pytest.fixture(scope='session')
def port(request):
    return int(request.config.option.port)


def pytest_generate_tests(metafunc):
    if 'socket_module' in metafunc.fixturenames:
        socket_modules = [socket]
        try:
            from gevent import socket as gevent_socket
        except ImportError:
            print("Skipping gevent (not installed)")
        else:
            socket_modules.append(gevent_socket)

        metafunc.parametrize("socket_module", socket_modules)

    if 'client_class' in metafunc.fixturenames:
        from pymemcache.client.base import PooledClient, Client
        from pymemcache.client.hash import HashClient
        class HashClientSingle(HashClient):
            def __init__(self, server, *args, **kwargs):
                super(HashClientSingle, self).__init__([server], *args, **kwargs)

        metafunc.parametrize(
            "client_class", [Client, PooledClient, HashClientSingle]
        )
