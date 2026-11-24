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
local fields = redis.call('HMGET', KEYS[1], 'order', 'timestamp', 'encoding_id', 'has_schema')
local ordering, timestamp, encoding_id, has_schema = unpack(fields)
if not ordering or not timestamp or #timestamp ~= 24 or
    not encoding_id or not string.match(encoding_id, '^%d+$') or
    tonumber(encoding_id) > 65535 or
    (has_schema ~= '0' and has_schema ~= '1') or
    redis.call('HEXISTS', KEYS[1], 'payload') == 0 or
    redis.call('HEXISTS', KEYS[1], 'encoding_schema') == 0 then
    return redis.error_reply('invalid Redis backend record')
end
local encoded = {}
for i = 1, #timestamp do
    encoded[i] = string.format('%02x', string.byte(timestamp, i))
end
if ordering ~= table.concat(encoded) or string.sub(timestamp, 9) == string.rep('\0', 16) then
    return redis.error_reply('invalid Redis backend record')
end
redis.call('SADD', KEYS[2], KEYS[1])
return 1
