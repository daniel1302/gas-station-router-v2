-- KEYS[1]: execution. ARGV: digest, token, lease TTL ms.
local record = execution(KEYS[1])
if not record or record.digest ~= ARGV[1] then return {5, 0} end
if record.phase == 'Claimed' then return {2, 0} end
if tonumber(record.lease_until) > now_ms() then
    if record.token == ARGV[2] then return {1, 0} else return {2, 0} end
end
if record.token == ARGV[2] then return {-1, 0} end
record.token = ARGV[2]
record.lease_until = lease_until(ARGV[3])
update(KEYS[1], record)
return {1, 0}
