local function key_type(key)
    local t = redis.call('TYPE', key)
    return type(t) == 'table' and t.ok or t
end
local record_type = key_type(KEYS[1])
if record_type == 'none' then return 0 end
if record_type ~= 'hash' then return redis.error_reply('unexpected Redis record type') end
for i = 2, 3 do
    local actual = key_type(KEYS[i])
    if actual ~= 'none' and actual ~= 'set' then
        return redis.error_reply('unexpected Redis index type')
    end
end
local kind = redis.call('HGET', KEYS[1], 'kind')
local ordering = redis.call('HGET', KEYS[1], 'order')
local timestamp = redis.call('HGET', KEYS[1], 'timestamp')
if (kind ~= 'live' and kind ~= 'tombstone') or not ordering or #ordering ~= 48 or not timestamp or #timestamp ~= 24 then
    return redis.error_reply('invalid Redis backend record')
end
if kind == 'live' then
    redis.call('SADD', KEYS[2], KEYS[1])
    redis.call('SREM', KEYS[3], KEYS[1])
else
    redis.call('SADD', KEYS[3], KEYS[1])
    redis.call('SREM', KEYS[2], KEYS[1])
end
return 1
