__author__ = 'vruizext'

import unittest
from redishelper import RedisHelper
from redisZcounter import RedisZSetCounter



class RedisCounterTest(unittest.TestCase):
    redis_server = {'host': 'localhost', 'port': 6379, 'db': 0}

    def test_incr(self):
        RedisHelper.redis_flushdb()
        # create a counter and increase value
        params = {'prefix': "test", 'ttl': 120}
        zcount = RedisZSetCounter(self.redis_server, params)
        tmp = zcount.incr('test_id', 'a', 1.0)
        # now check that the value in redis is right
        key = zcount.get_key('test_id')
        val = RedisHelper.get_connection().zscore(key, 'a')
        self.assertEqual(tmp, val, "wrong value for counter")

    def test_decr(self):
        RedisHelper.redis_flushdb()
        # create a counter and decrease value
        params = {'prefix': "test", 'ttl': 120}
        zcount = RedisZSetCounter(self.redis_server, params)
        tmp = zcount.decr('test_id', 'a', 1.0)
        # now check that the value in redis is right
        key = zcount.get_key('test_id')
        val = RedisHelper.get_connection().zscore(key, 'a')
        self.assertEqual(tmp, val, "wrong value for counter")

    def test_top_n(self):
        RedisHelper.redis_flushdb()
        params = {'prefix': "test", 'ttl': 120}
        zcount = RedisZSetCounter(self.redis_server, params)
        zcount.incr('test_id', 'a', 1)
        zcount.incr('test_id', 'b', 3)
        zcount.incr('test_id', 'c', 6)
        zcount.incr('test_id', 'd', 2)
        top3 = zcount.top_n('test_id', 3)
        print top3
        self.assertEqual(top3[0][0], 'c', "wrong ranking 1st position should be 'c'")
        self.assertEqual(top3[1][0], 'b', "wrong ranking 2nd position should be 'b'")
        self.assertEqual(top3[2][0], 'd', "wrong ranking 3rd position should be 'c'")

    def test_last_n(self):
        RedisHelper.redis_flushdb()
        params = {'prefix': "test", 'ttl': 120}
        zcount = RedisZSetCounter(self.redis_server, params)
        zcount.incr('test_id', 'c', 6)
        zcount.incr('test_id', 'b', 3)
        zcount.incr('test_id', 'a', 1)
        zcount.incr('test_id', 'd', 2)
        last3 = zcount.last_n('test_id', 3)
        print last3
        self.assertEqual(last3[0][0], 'a', "wrong ranking 1st position should be 'c'")
        self.assertEqual(last3[1][0], 'd', "wrong ranking 2nd position should be 'b'")
        self.assertEqual(last3[2][0], 'b', "wrong ranking 3rd position should be 'c'")
