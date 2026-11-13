local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])


local clear_before = now - window

redis.call('ZREMRANGEBYSCORE', key, 0, clear_before)

local current_requests = redis.call('ZCARD', key)
 

if current_requests < limit then
    local unique_id = now .. '-' .. redis.call('INCR', key .. ':seq')
    redis.call('ZADD', key, now, unique_id)

    redis.call('PEXPIRE', key, window)
    redis.call('PEXPIRE', key .. ':seq', window)

    return {1, limit - current_requests - 1}
else
    return {0, 0}
end