-- Each data key receives its TTL in the SET that creates it. Existing data keys
-- without TTL fail closed; updates preserve expiry with KEEPTTL.
local function read(key)
    local value = redis.call('GET', key)
    if value and redis.call('PTTL', key) < 0 then
        error('router record is missing its TTL')
    end
    return value
end

local function load(key)
    local value = read(key)
    if value then return cjson.decode(value) end
end

local function update(key, value)
    assert(redis.call('SET', key, cjson.encode(value), 'XX', 'KEEPTTL'),
        'router record disappeared during update')
end
