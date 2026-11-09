-- KEYS: execution, transaction cache.
-- ARGV: digest, token, backend, effects JSON, confirmed (0/1), cache TTL ms.
local record = execution(KEYS[1])
if not record or record.digest ~= ARGV[1] or record.token ~= ARGV[2]
    or (record.phase ~= 'Dispatching' and record.phase ~= 'Completed') then return {0, 0} end
local result = write_transaction(KEYS[2], ARGV[3], ARGV[4], ARGV[5] == '1', ARGV[6])
if result == 0 then return {0, 0} end
local transitioned = record.phase ~= 'Completed'
record.phase = 'Completed'
record.lease_until = '0'
update(KEYS[1], record)
return {result, transitioned and 1 or 0}
