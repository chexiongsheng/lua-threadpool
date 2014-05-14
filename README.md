lua-threadpool
==============

threadpool for lua and luajit, threadpool for fend.epoll
### Example

```lua
local dispatcher = require "fend.epoll"
local epoll = dispatcher()

local threadpool = require 'threadpool_epoll'
threadpool.init({
    logger = {
        warn = print,
        error = print,
        debug = print
    },
    growing_thread_num = 10,
    epoll = epoll,
})

local env_seq = 2313141232
local thread_id 
print('start', epoll:now())
epoll:add_timer(2, 0, function()
    print('on timer 1', epoll:now())
    --start a thread 
    threadpool.work(function()
        print('thread 1 started', epoll:now())
        threadpool.wait(2)
        print('thread 1 wait 1', epoll:now())
        threadpool.wait(3)
        thread_id = threadpool.running.id
        print('thread 1 wait 2', epoll:now(), thread_id)
        print(threadpool.wait(env_seq, 100))
        print('thread 1 end', epoll:now())
    end)
    threadpool.work(function()
        print('thread 2 started', epoll:now())
        threadpool.wait(1)
        print('thread 2 wait 1', epoll:now())
        threadpool.wait(3)
        print('thread 2 end', epoll:now())
    end)
    return false
end)

threadpool.work(function()
    print('thread 3 started', epoll:now())
    threadpool.wait(1)
    print('thread 3 wait 1', epoll:now())
    threadpool.wait(3)
    print('thread 3 end', epoll:now())
end)

local cond = false

epoll:add_timer(9, 0, function()
    print('notify thread id=', thread_id)
    --notify a the thread
    threadpool.notify(thread_id, env_seq, 0, 4455667788)
    print('set cond true')
    cond = true
end)

threadpool.work(function()
    print('thread 4 started', epoll:now())
    threadpool.wait_until(function() return cond end)
    print('thread 4 end', epoll:now())
end)

local runing = true
--epoll loop
while runing do
    epoll:dispatch(100, -1, function(e, file , cbs , err , eventtype)
        print(file:getfd(), 'dispatch.onerror, err =', err, debug.traceback())
        local pcall_ret, msg = pcall(e.del_fd, e, file )
        if not pcall_ret then
            print('dispatch.onerror, call del_fd fail, msg = ', msg)
        end
        pcall_ret, msg = pcall( file.close , file )
        file.no_close = true
        if not pcall_ret then
            print('dispatch.onerror, call file.close fail, msg = ', msg)
        end
    end)
    collectgarbage ()
end
```

### Output

```text
tart   800795.83389947
thread 3 started        800795.83396331
thread 4 started        800795.83399728
thread 3 wait 1 800796.83470464
on timer 1      800797.83406606
thread 1 started        800797.83411515
thread 2 started        800797.8341229
thread 2 wait 1 800798.83528458
thread 1 wait 1 800799.83468627
thread 3 end    800799.83598824
thread 2 end    800801.83623277
thread 1 wait 2 800802.83582687 8
notify thread id=       8
0       4455667788
thread 1 end    800804.8341172
set cond true
thread 4 end    800804.83475575
```

