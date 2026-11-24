if redis.call('EXISTS', KEYS[1]) == 0 then
    return redis.call('SREM', KEYS[2], KEYS[1])
end
return 0
