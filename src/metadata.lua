local exists = redis.call('EXISTS', KEYS[1])
if exists == 0 then return {0, false} end
return {1, redis.call('HGET', KEYS[1], 'timestamp')}
