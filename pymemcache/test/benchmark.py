import argparse
import time


def test_client(name, client, size, count):
    client.flush_all()

    value = 'X' * size

    start = time.time()

    for i in xrange(count):
        client.set(str(i), value)

    for i in xrange(count):
        client.get(str(i))

    duration = time.time() - start
    print "{}: {}".format(name, duration)


def test_pylibmc(host, port, size, count):
    try:
        import pylibmc
    except Exception:
        print "Could not import pylibmc, skipping test..."
        return

    client = pylibmc.Client(['{}:{}'.format(host, port)])
    client.behaviors = {"tcp_nodelay": True}
    test_client('pylibmc', client, size, count)


def test_memcache(host, port, size, count):
    try:
        import memcache
    except Exception:
        print "Could not import pymemcache.client, skipping test..."
        return

    client = memcache.Client(['{}:{}'.format(host, port)])
    test_client('memcache', client, size, count)


def test_pymemcache(host, port, size, count):
    try:
        import pymemcache.client
    except Exception:
        print "Could not import pymemcache.client, skipping test..."
        return

    client = pymemcache.client.Client((host, port))
    test_client('pymemcache', client, size, count)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server',
                        metavar='HOST',
                        required=True)
    parser.add_argument('-p', '--port',
                        metavar='PORT',
                        type=int,
                        required=True)
    parser.add_argument('-z', '--size',
                        metavar='SIZE',
                        default=1024,
                        type=int)
    parser.add_argument('-c', '--count',
                        metavar='COUNT',
                        default=10000,
                        type=int)

    args = parser.parse_args()

    test_pylibmc(args.server, args.port, args.size, args.count)
    test_memcache(args.server, args.port, args.size, args.count)
    test_pymemcache(args.server, args.port, args.size, args.count)


if __name__ == '__main__':
    main()
