--===========================================================================--
--                                                                           --
--                            NgxLua.LockManager                             --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2018/04/03                                              --
-- Update Date  :   2018/04/03                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--

PLoop(function(_ENV)
    __Sealed__() class "NgxLua.LockManager" (function (_ENV)
        extend "ILockManager"

        local resty_lock = require "resty.lock"
        local ngxdictname
        local trylockopt = { timeout = 0 }

        export {
            Debug = System.Logger.Default[System.Logger.LogLevel.Debug],
        }

        -----------------------------------------------------------------------
        --                              method                               --
        -----------------------------------------------------------------------
        --- Lock with a key and return a lock object to release
        -- @param   key                 the lock key
        -- @return  object              the lock object
        -- @return  error               the error message if failed
        __Abstract__() function Lock(self, key)
            Debug("[LockManager][Lock] %s", key)
            local lock, err = resty_lock:new(self[1])
            if not lock then return nil, err end

            local elapsed, err = lock:lock(key)
            if not elapsed then return nil, err end

            return lock
        end

        --- Try lock with a key and return a lock object to release
        -- @param   key                 the lock key
        -- @return  object              the lock object
        -- @return  message             the error message if failed
        __Abstract__() function TryLock(self, key)
            Debug("[LockManager][Lock] %s", key)
            local lock, err = resty_lock:new(self[1], trylockopt)
            if not lock then return nil, err end

            local elapsed, err = lock:lock(key)
            if not elapsed then return nil, err end

            return lock
        end

        --- Release the lock object
        -- @param   object              the lock object
        -- @return  bool                true if released
        -- @return  message             the error message if failed
        __Abstract__() function Release(self, obj, key)
            Debug("[LockManager][Release] %s", key)
            return obj:unlock()
        end

        -----------------------------------------------------------------------
        --                            constructor                            --
        -----------------------------------------------------------------------
        __Arguments__{ String }
        function __new(_, name)
            return { name }
        end
    end)
end)