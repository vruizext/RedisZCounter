__author__ = 'vruizext'

from redishelper import RedisHelper
from operator import itemgetter


class CounterBase(object):
    """
    Base class for the counter
    Allows to track a list of counts of events, things, etc
    """
    def __init__(self, prefix, ttl):
        self.prefix = prefix
        self.ttl = ttl

    def get_key(self, counter_id):
        """
        build the key that identifies a counter
        :return: counter key (string)
        """
        return "%s:%s" % (self.prefix, str(counter_id))

    def incr(self, counter_id, entry, count=1):
        """
        Increments the count of member_id in the current bucket by count
        Supports decreasing, when count < 0
        :param counter_id:  identifier for this counter
        :param entry: identifier of the member / item / whatever is being accounted
        :param count: increase count by this amount
        :return: the new count for this entry
        """
        pass

    def decr(self, counter_id, entry, count=1):
        """
        Decrease counter, by calling incr with negative count
        :param counter_id: suffix identifier for this counter
        :param entry: identifier of the member / item / whatever is being accounted
        :param count: decrease count by this amount
        :return: the new count of the entry
        """
        pass

    def top_n(self, counter_id, how_many):
        """
        get the top N entries in the counter
        :param counter_id: identifier for this counter
        :param how_many: how many items to return (top N)
        :return: dictionary containing tuples (id, count)
        """
        pass

    def last_n(self, counter_id, how_many):
        """
        get the last N entries in this counter
        :param counter_id: identifier for this counter
        :param how_many: how many items to return (top N)
        :return: dictionary containing tuples (id, count)
        """
        pass


class RedisZSetCounter(CounterBase):
    """
    Counter that uses redis to store data
    """

    def __init__(self, server, config):
        RedisHelper.set_server(server)
        self.redis = None
        CounterBase.__init__(self, config['prefix'], config['ttl'])

    def get_redis(self):
        """
        Get redis connection, lazy loading
        :return: redis object
        """
        if self.redis is None:
            self.redis = RedisHelper.get_connection()

        return self.redis

    def incr(self, counter_id, entry, count=1):
        """
        Increments the count of member_id in the current bucket by count
        Supports decreasing, when count < 0
        :param counter_id:  identifier for this counter
        :param entry: identifier of the member / item / whatever is being accounted
        :param count: increase count by this amount
        :return: the new count for this entry
        """
        key = self.get_key(counter_id)
        value = self.get_redis().zincrby(key, entry, count)
        self.get_redis().expire(key, self.ttl)
        return value

    def decr(self, counter_id, entry, count=1):
        """
        Decrease counter, by calling incr with negative count
        :param counter_id: suffix identifier for this counter
        :param entry: identifier of the member / item / whatever is being accounted
        :param count: decrease count by this amount
        :return: the new count of the entry
        """
        return self.incr(counter_id, entry, -count)

    def top_n(self, counter_id, how_many):
        """
        get the top N entries in the counter
        :param counter_id: identifier for this counter
        :param how_many: how many items to return (top N)
        :return: dictionary containing tuples (id, count)
        """
        key = self.get_key(counter_id)
        return self.get_redis().zrevrange(key, 0, how_many - 1, withscores=True)

    def last_n(self, counter_id, how_many):
        """
        get the last N entries in this counter
        :param counter_id: identifier for this counter
        :param how_many: how many items to return (top N)
        :return: dictionary containing tuples (id, count)
        """
        key = self.get_key(counter_id)
        return self.get_redis().zrange(key, 0, how_many - 1, desc=False, withscores=True)