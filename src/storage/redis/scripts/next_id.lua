-- KEYS[1]: persistent ID counter. No arguments.
-- Return GET rather than Lua's INCR number to retain all integer bits.
local current = redis.call('GET', KEYS[1])
if current and (#current > 19 or (#current == 19 and current >= '9223372036854775807')) then
    return false
end
redis.call('INCR', KEYS[1])
return redis.call('GET', KEYS[1])
