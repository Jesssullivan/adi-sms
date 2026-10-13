if redis.call('XACK', KEYS[1], 'senders', ARGV[1]) == 0 then return 0 end -- not new: count nothing
redis.call('HINCRBY', KEYS[2], 'sent', ARGV[2])
redis.call('HINCRBY', KEYS[2], 'failed', ARGV[3])
redis.call('HINCRBY', KEYS[2], 'attempts', ARGV[4])
redis.call('HINCRBY', KEYS[2], 'time_us', ARGV[5])
redis.call('HINCRBY', KEYS[2], 'done', 1)
redis.call('XDEL', KEYS[1], ARGV[1])
return 1
