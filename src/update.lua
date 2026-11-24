local function key_type(key)
    local t = redis.call('TYPE', key)
    return type(t) == 'table' and t.ok or t
end
for i, expected in ipairs({'hash', 'set', 'set'}) do
    local actual = key_type(KEYS[i])
    if actual ~= 'none' and actual ~= expected then
        return redis.error_reply('unexpected Redis key type for backend record or index')
    end
end
local previous = redis.call('HGET', KEYS[1], 'order')
local old_kind = redis.call('HGET', KEYS[1], 'kind')
if key_type(KEYS[1]) == 'hash' and
    (not previous or #previous ~= 48 or (old_kind ~= 'live' and old_kind ~= 'tombstone')) then
    return redis.error_reply('invalid Redis backend record')
end
if previous and previous >= ARGV[1] then
    return 0
end
if ARGV[2] == 'live' then
    redis.call('HSET', KEYS[1], 'kind', 'live', 'order', ARGV[1],
        'timestamp', ARGV[3], 'payload', ARGV[4], 'encoding_id', ARGV[5],
        'encoding_schema', ARGV[6], 'has_schema', ARGV[7])
    if ARGV[8] == '0' then redis.call('PERSIST', KEYS[1])
    else redis.call('EXPIRE', KEYS[1], ARGV[8]) end
    redis.call('SADD', KEYS[2], KEYS[1])
    redis.call('SREM', KEYS[3], KEYS[1])
    if old_kind == 'live' then return 2 else return 1 end
else
    redis.call('HSET', KEYS[1], 'kind', 'tombstone', 'order', ARGV[1], 'timestamp', ARGV[3])
    redis.call('HDEL', KEYS[1], 'payload', 'encoding_id', 'encoding_schema', 'has_schema')
    redis.call('EXPIRE', KEYS[1], ARGV[8])
    redis.call('SREM', KEYS[2], KEYS[1])
    redis.call('SADD', KEYS[3], KEYS[1])
    return 3
end
