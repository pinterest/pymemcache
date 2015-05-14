import time
from .ring import HashRing


class ShardingClient(object):

    def __init__(self, clients, black_list_timeout=30):

        self.servers = {}
        for client in clients:
            self.servers[client.server] = client
        self.ring = HashRing(self.servers.keys())
        self.black_list = {}
        self.black_list_timeout = black_list_timeout
        self.last_sync_black_list_time = time.time()

    def check_key(self, key):
        """Checks key and add key_prefix."""
        client = self.get_client(key)
        return client.check_key(key)

    def sync_black_list(self, force=False):
        interval = time.time() - self.last_sync_black_list_time
        if interval > self.black_list_timeout or force:
            need_change_ring = True if force else False
            for key, added_time in list(self.black_list.items()):
                if (time.time() - added_time) > self.black_list_timeout:
                    del self.black_list[key]
                    need_change_ring = True
            if need_change_ring:
                self.ring = HashRing(
                    set(self.servers.keys()) ^ set(self.black_list.keys())
                )
                self.last_sync_black_list_time = time.time()

    def add_black_list(self, key):
        self.black_list[key] = time.time()
        self.sync_black_list(True)

    def get_client(self, key):
        if self.black_list:
            self.sync_black_list()
        client = self.servers[self.ring.get_node(key)]
        return client

    def close(self):
        for server in self.servers.values():
            server.close()

    def set(self, key, value, expire=0, noreply=True):
        client = self.get_client(key)
        try:
            return client.set(key, value, expire, noreply)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def set_many(self, values, expire=0, noreply=True):
        batch = {}
        for key, value in values.items():
            client = self.get_client(key)
            if client not in batch:
                batch[client] = {key: value}
            else:
                batch[client][key] = value

        for client, batch_dict in batch.items():
            try:
                client.set_many(batch_dict)
            except BaseException as e:
                self.add_black_list(client.server)
                raise e
        return True

    def replace(self, key, value, expire=0, noreply=True):
        client = self.get_client(key)
        try:
            return client.replace(key, value, expire, noreply)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def append(self, key, value, expire=0, noreply=True):
        client = self.get_client(key)
        try:
            return client.append(key, value, expire, noreply)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def prepend(self, key, value, expire=0, noreply=True):
        client = self.get_client(key)
        try:
            return client.prepend(key, value, expire, noreply)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def cas(self, key, value, cas, expire=0, noreply=False):
        client = self.get_client(key)
        try:
            return client.cas(key, value, cas, expire, noreply)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def get(self, key):
        client = self.get_client(key)
        try:
            return client.get(key)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def get_many(self, keys):
        batch = {}
        results = {}
        for key in keys:
            client = self.get_client(key)
            if client not in batch:
                batch[client] = [key]
            else:
                batch[client].append(key)
        for client, keys in batch.items():
            try:
                results.update(client.get_many(keys))
            except BaseException as e:
                self.add_black_list(client.server)
                raise e
        return results

    def gets(self, key):
        client = self.get_client(key)
        try:
            return client.gets(key)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def gets_many(self, keys):
        batch = {}
        results = {}
        for key in keys:
            client = self.get_client(key)
            if client not in batch:
                batch[client] = [key]
            else:
                batch[client].append(key)
        for client, keys in batch:
            try:
                results.update(client.gets_many(keys))
            except BaseException as e:
                self.add_black_list(client.server)
                raise e
        return results

    def delete(self, key, noreply=True):
        client = self.get_client(key)
        try:
            return client.delete(key, noreply)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def delete_many(self, keys, noreply=True):
        batch = {}
        for key in keys:
            client = self.get_client(key)
            if client not in batch:
                batch[client] = [key]
            else:
                batch[client].append(key)
        for client, keys in batch:
            try:
                client.delete_many(keys)
            except BaseException as e:
                self.add_black_list(client.server)
                raise e
        return True

    def add(self, key, value, expire=0, noreply=True):
        client = self.get_client(key)
        try:
            return client.add(key, value, expire, noreply)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def incr(self, key, value, noreply=False):
        client = self.get_client(key)
        try:
            return client.incr(key, value, noreply)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def decr(self, key, value, noreply=False):
        client = self.get_client(key)
        try:
            return client.decr(key, value, noreply)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def touch(self, key, expire=0, noreply=True):
        client = self.get_client(key)
        try:
            return client.touch(key, expire, noreply)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def stats(self, *args):
        results = {}
        for client in self.servers.values():
            results.update(client.stats(*args))
        return results

    def flush_all(self, delay=0, noreply=True):
        ok = True
        for client in self.servers.values():
            if not client.flush_all(delay, noreply):
                ok = False
        return ok

    def quit(self):
        for client in self.servers.values():
            client.quit()

    def __setitem__(self, key, value):
        client = self.get_client(key)
        try:
            return client.__setitem__(key, value)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def __getitem__(self, key):
        client = self.get_client(key)
        try:
            return client.__getitem__(key)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e

    def __delitem__(self, key):
        client = self.get_client(key)
        try:
            return client.__delitem__(key)
        except BaseException as e:
            self.add_black_list(client.server)
            raise e