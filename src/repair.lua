local function key_type(key)
    local t = redis.call('TYPE', key)
    return type(t) == 'table' and t.ok or t
end
local record_type = key_type(KEYS[1])
if record_type == 'none' then return 0 end
if record_type ~= 'hash' then return redis.error_reply('unexpected Redis record type') end
local index_type = key_type(KEYS[2])
if index_type ~= 'none' and index_type ~= 'set' then
    return redis.error_reply('unexpected Redis index type')
end
local ordering = redis.call('HGET', KEYS[1], 'order')
local timestamp = redis.call('HGET', KEYS[1], 'timestamp')
if not ordering or #ordering ~= 48 or not timestamp or #timestamp ~= 24 then
    return redis.error_reply('invalid Redis backend record')
end
redis.call('SADD', KEYS[2], KEYS[1])
return 1
