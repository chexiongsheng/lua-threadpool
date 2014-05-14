--
--------------------------------------------------------------------------------
--         FILE:  threadpool_epoll.lua
--        USAGE:  ./threadpool_epoll.lua 
--  DESCRIPTION:  
--      OPTIONS:  ---
-- REQUIREMENTS:  ---
--         BUGS:  ---
--        NOTES:  ---
--       AUTHOR:  John (J), <chexiongsheng@qq.com>
--      COMPANY:  
--      VERSION:  1.0
--      CREATED:  2014年05月14日 15时12分52秒 CST
--     REVISION:  ---
--------------------------------------------------------------------------------
--

local threadpool = require 'threadpool'

local threadpool_epoll = setmetatable({}, {__index = threadpool})

local TINY_INTERVAL = 1/1000 --微秒级别

local epoll, threadpool_timer, next_timeout

threadpool_epoll.init = function(cfg)
    epoll = assert(cfg.epoll)
    threadpool_timer = epoll:add_timer(0, 0, function()
        next_timeout = threadpool.check_timeout(epoll:now())
        return next_timeout and math.max(TINY_INTERVAL, next_timeout - epoll:now()) or 0
    end)
    return threadpool.init(cfg)
end

threadpool_epoll.work = function(...)
    local rc, timeout = threadpool.work(...)
    if rc and timeout then
        if (not next_timeout) or timeout < next_timeout then
            next_timeout = timeout
            threadpool_timer:set(math.max(TINY_INTERVAL, timeout - epoll:now()))
        end
    end
    return rc
end

local wait = function(event, interval)
    if interval == nil then
        interval = event
        event = nil
    end
    assert(interval)
    local timeout = interval + epoll:now()
    return threadpool.wait(event, timeout)
end
threadpool_epoll.wait = wait

threadpool_epoll.wait_until = function(cond_func) 
    while not cond_func() do
        wait(0)
    end
end

threadpool_epoll.notify = function(...)
    local rc, timeout = threadpool.notify(...)
    if rc and timeout then
        if (not next_timeout) or timeout < next_timeout then
            next_timeout = timeout
            threadpool_timer:set(math.max(TINY_INTERVAL, timeout - epoll:now()))
        end
    end
    return rc
end

return threadpool_epoll





