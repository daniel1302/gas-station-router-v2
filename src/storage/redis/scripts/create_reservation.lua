-- KEYS[1]: reservation. ARGV: serialized reservation, TTL in milliseconds.
return redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
