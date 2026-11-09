local function execution(key)
    local record = load(key)
    if record then
        assert(type(record.digest) == 'string' and type(record.token) == 'string'
            and tonumber(record.lease_until)
            and (record.phase == 'Claimed' or record.phase == 'Dispatching'
                or record.phase == 'Completed'), 'invalid router execution record')
    end
    return record
end

local function now_ms()
    local time = redis.call('TIME')
    return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

local function lease_until(ttl)
    -- Decimal strings avoid cjson's numeric rounding for large durations.
    return string.format('%.0f', now_ms() + tonumber(ttl))
end
