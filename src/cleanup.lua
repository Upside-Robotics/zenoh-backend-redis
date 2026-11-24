local kind = redis.call('HGET', KEYS[1], 'kind')
if kind and kind ~= 'live' and kind ~= 'tombstone' then
    return redis.error_reply('invalid Redis backend record kind')
end
if not kind or kind ~= ARGV[1] then
    return redis.call('SREM', KEYS[2], KEYS[1])
end
return 0
