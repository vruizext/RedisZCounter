__author__ = 'vruizext'

import redis


class RedisHelper(object):
    """
    Handles redis connections. Uses a Connection Pool to manage multiple connections
    """

    server = {}

    @classmethod
    def set_server(cls, server):
        cls.server = server

    @classmethod
    def get_pool(cls):
        try:
            pool = cls.pool
        except AttributeError:
            pool = redis.ConnectionPool(host=cls.server['host'], port=cls.server['port'], db=cls.server['db'])
        return pool

    @classmethod
    def get_connection(cls):
        return redis.Redis(connection_pool=cls.get_pool())

    @classmethod
    def get_pipe(cls):
        return cls.get_connection().pipeline()

    @classmethod
    def flushdb(cls):
        cls.get_connection().flushdb()