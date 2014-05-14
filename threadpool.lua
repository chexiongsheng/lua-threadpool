local TIMEOUT = 1

local IDLING = 0 --闲
local PROCESSING = 1 --正在干活

local threadpool= {
    IDLING = IDLING,
    PROCESSING = PROCESSING,
    TIMEOUT = TIMEOUT,
} 

local table_push = _G.table.insert
local table_pop  = _G.table.remove
--空闲任务队列
--待执行等函数队列
local idle_thread_stack = {}
local thread_list = {}

local logger, growing_thread_num, upper_thread_num

local function err_handler(e)
    return tostring(e)..'\n'..tostring(debug.traceback())
end

local function xp_warper(func, ...) --在函数和参数之间插入一个err_handler
    return func, err_handler, ...
end

threadpool.grow = function (num)
    assert(num > 0)
    for i = 1, num do 
        local co = coroutine.create(function(tp, thread)
            local pcall_suc, ret
            while true do 
                pcall_suc, ret = xpcall(xp_warper(coroutine.yield()))
                if not pcall_suc then
                    logger.error('error in thread job:'..tostring(ret))
                end
                tp.running = thread.parent
                thread.status = IDLING
                table_push(idle_thread_stack, thread)
            end 
        end)

        thread = {
            co = co,
            id = #thread_list+ 1,
            status = IDLING, --任务状态
            tls = {},
            call = function (self,func,...)
                assert(self.status == IDLING)
                if func and type(func) == "function" then 
                    self.status = PROCESSING
                    return self:resume(func, ...)
                else
                    table_push(idle_thread_stack, thread)
                    assert(false,"func is ".. tostring(func) .. '!')
                end 
            end,
            resume = function (self, ...)
                self.parent = threadpool.running
                threadpool.running = self
                return coroutine.resume(self.co, ...)
            end,
        }

        table_push(thread_list,  thread)
        table_push(idle_thread_stack, thread)
        coroutine.resume(co, threadpool, thread)
    end
end

threadpool.tls_set = function(k, v)
    threadpool.running.tls[k] = v
end

threadpool.tls_get = function(k)
    return threadpool.running.tls[k]
end

threadpool.work = function(...)
    local thread = table_pop(idle_thread_stack)
    if not thread then
        if #thread_list >= upper_thread_num then
            logger.error('reach the upper_thread_num', upper_thread_num, ' #thread_list=', #thread_list)
            return false
        end
        logger.warn('not idle thread, thread count =', #thread_list, 'growing =', growing_thread_num)
        threadpool.grow(math.min(growing_thread_num, upper_thread_num - #thread_list))
        thread = table_pop(idle_thread_stack)
    end
    return thread:call(...)
end

threadpool.init = function(cfg)
    logger = assert(cfg.logger, 'logger must provide!')
    growing_thread_num = assert(cfg.growing_thread_num, 'growing_thread_num must provide!')
    assert(growing_thread_num > 0)
    upper_thread_num = cfg.upper_thread_num or 1000
    local init_thread_num = cfg.init_thread_num or growing_thread_num
    assert(upper_thread_num > init_thread_num and upper_thread_num > growing_thread_num)
    threadpool.grow(init_thread_num)
end

threadpool.wait = function(event, timeout)
    if timeout == nil then
        timeout = event
        event = nil
    end
    assert(timeout)
    threadpool.running._event_ = event
    threadpool.running._timeout_ = timeout
    threadpool.running = threadpool.running.parent
    return coroutine.yield(timeout)
end

threadpool.check_timeout = function(now)
    local thread
    local workingcount = 0 
    local next_timeout
    for i =1, #thread_list do
        thread = thread_list[i]
        local timeout
        if thread.status == PROCESSING then
            assert(thread._timeout_) --假如PROCESS而且让出协程不可能timeout为空
            if thread._timeout_ <= now then
                local resume_ret
                if thread._event_ then 
                    resume_ret, timeout = thread:resume(TIMEOUT, thread._event_)
                else
                    resume_ret, timeout = thread:resume(0)
                end
                if resume_ret then
                    workingcount = workingcount + 1
                else
                    logger.error('threadpool.check_timeout, resume error:', timeout)
                end
           else
               timeout = thread._timeout_
           end
           if timeout then
               next_timeout = (next_timeout == nil) and timeout or math.min(next_timeout, timeout)
           end
        end
    end
    return next_timeout, workingcount
end

threadpool.notify = function(id, event, ...)
    local thread = thread_list[id]
    if thread == nil then
        logger.warn('try to wakeup thread not existed, id='..tostring(id))
    elseif thread.status ~= PROCESSING then
        logger.warn('try to wakeup an idle thread, id='..tostring(id))
    elseif thread._event_ ~= event then
        logger.warn('unexpect event, expect['..tostring(thread._event_)..'], but recv ['..tostring(event)..']')
    else
        return thread:resume(...)
    end
    return false
end

return threadpool

