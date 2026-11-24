local exists = redis.call('EXISTS', KEYS[1])
if exists == 0 then return {0, false, false} end
local metadata = redis.call('HMGET', KEYS[1], 'kind', 'timestamp')
return {1, metadata[1], metadata[2]}
