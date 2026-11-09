-- KEYS: execution, reservation. ARGV: digest, token, lease TTL ms, record TTL ms.
-- Return {claim outcome, created}; -1 denotes an expired token being reused.
local record = execution(KEYS[1])
if not record then
    local reservation = load(KEYS[2])
    record = {digest=ARGV[1], token=ARGV[2], phase='Claimed',
        lease_until=lease_until(ARGV[3])}
    if reservation then record.backend = reservation.backend end
    redis.call('SET', KEYS[1], cjson.encode(record), 'PX', ARGV[4])
    return {1, 1}
end
if record.digest ~= ARGV[1] then return {5, 0} end
if record.phase == 'Dispatching' then return {3, 0} end
if record.phase == 'Completed' then return {4, 0} end
if tonumber(record.lease_until) > now_ms() then
    if record.token == ARGV[2] then return {1, 0} else return {2, 0} end
end
if record.token == ARGV[2] then return {-1, 0} end
record.token = ARGV[2]
record.lease_until = lease_until(ARGV[3])
update(KEYS[1], record)
return {1, 0}
