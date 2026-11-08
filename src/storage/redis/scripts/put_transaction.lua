-- KEYS[1]: transaction cache. ARGV: backend, effects JSON, confirmed (0/1), TTL ms.
return write_transaction(KEYS[1], ARGV[1], ARGV[2], ARGV[3] == '1', ARGV[4])
