-- Return 1=inserted, 2=upgraded, 3=unchanged, 0=conflict. Effects stay an opaque
-- canonical JSON string so Lua cannot round u64 object versions or coin values.
local function write_transaction(key, backend, effects, confirmed, ttl)
    local existing = load(key)
    if existing then
        assert(type(existing.effects) == 'string'
            and type(existing.backend) == 'string'
            and type(existing.local_execution_confirmed) == 'boolean',
            'invalid router transaction record')
        if existing.backend ~= backend or existing.effects ~= effects then return 0 end
        if confirmed and not existing.local_execution_confirmed then
            existing.local_execution_confirmed = true
            update(key, existing)
            return 2
        end
        return 3
    end
    local value = cjson.encode({backend=backend, effects=effects,
        local_execution_confirmed=confirmed})
    redis.call('SET', key, value, 'PX', ttl)
    return 1
end
