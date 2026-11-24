local function key_type(key)
    local t = redis.call('TYPE', key)
    return type(t) == 'table' and t.ok or t
end
local record_type = key_type(KEYS[1])
if record_type ~= 'none' and record_type ~= 'hash' then
    return redis.error_reply('unexpected Redis key type for backend record')
end
local previous = redis.call('HGET', KEYS[1], 'order')
if record_type == 'hash' and
    (not previous or #previous ~= 48) then
    return redis.error_reply('invalid Redis backend record')
end
if previous and previous >= ARGV[1] then
    return 0
end
if ARGV[2] == 'put' then
    redis.call('HSET', KEYS[1], 'order', ARGV[1],
        'timestamp', ARGV[3], 'payload', ARGV[4], 'encoding_id', ARGV[5],
        'encoding_schema', ARGV[6], 'has_schema', ARGV[7])
    if previous then return 2 else return 1 end
else
    redis.call('DEL', KEYS[1])
    return 3
end
