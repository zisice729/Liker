local countKey = KEYS[1]
local setKey = KEYS[2]
local userId = ARGV[1]

local isExist = redis.call('sismember', setKey, userId)
if isExist == 1 then
    redis.call('srem', setKey, userId)
    redis.call('decr', countKey)
else
    redis.call('sadd', setKey, userId)
    redis.call('incr', countKey)
end
return redis.call('get', countKey)