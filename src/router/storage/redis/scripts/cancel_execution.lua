-- KEYS[1]: execution. ARGV[1]: owner token.
local record = execution(KEYS[1])
if not record or record.token ~= ARGV[1] or record.phase ~= 'Claimed'
    or tonumber(record.lease_until) <= now_ms() then return 0 end
redis.call('DEL', KEYS[1])
return 1
