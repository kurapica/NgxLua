--===========================================================================--
--                                                                           --
--                           NgxLua.RedisProvider                            --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2018/09/07                                               --
-- Update Date  :   2019/06/06                                               --
-- Version      :   1.1.0                                                    --
--===========================================================================--
PLoop(function(_ENV)
    namespace "NgxLua"

    __Sealed__() class "Redis" (function(_ENV)
        extend "System.Data.ICache"

        local redis 			= require "resty.redis"

        export {
            State_Closed        = System.Data.ConnectionState.Closed,
            State_Open          = System.Data.ConnectionState.Open,
            State_Connecting    = System.Data.ConnectionState.Connecting,
            State_Executing     = System.Data.ConnectionState.Executing,
            State_Fetching      = System.Data.ConnectionState.Fetching,

            Trace               = System.Logger.Default[System.Logger.LogLevel.Trace],

            error               = error,
            parseValue 			= System.Data.ParseValue,
            strlower            = string.lower,

            serialize           = Serialization.Serialize,
            deserialize         = Serialization.Deserialize,

            stringProvider      = Serialization.StringFormatProvider{ ObjectTypeIgnored = false, Indent = false, LineBreak = "" },

            Date, System.Data.ConnectionState
        }

	    __Sealed__() struct "ConnectionOption" (function(_ENV)
	        --- the host name for the redis server
	        member "host" 		{ type = String, default = "127.0.0.1" }

	        --- the port that the redis server is listening on
	        member "port" 		{ type = Integer, default = 6379 }

	        --- the path of the unix socket file listened by the redis server
	        member "path" 		{ type = String }

	        --- the name for the redis connection pool
	        member "pool" 		{ type = String }

	        --- the authentication for the redis server
	        member "auth" 		{ type = String }
	    end)

        __Arguments__{ NEString } __Abstract__{ Inheritable = true }
        class "RedisScript" (function(_ENV, script)
            local _Script       = script
            local _Sha1

            __Static__()
            function Run(redis, ...)
                local res, err

                if not _Sha1 then
                    -- Init
                    res, err    = redis:Execute("script", "load", _Script)
                    if err then return res, err end
                    _Sha1       = res
                end

                res, err        = redis:Execute("evalsha", _Sha1, ...)

                if err and err:match("NOSCRIPT") then
                    -- Should load the script
                    res, err    = redis:Execute("EVAL", _Script, ...)
                end

                return res, err
            end
        end)

        -----------------------------------------------------------
        --                       property                        --
        -----------------------------------------------------------
        --- The option of the connection
        property "Option"       { field = 2, type = ConnectionOption }

        --- The connection state
        property "State"      	{ field = 3, type = ConnectionState, default = State_Closed }

        --- Keep the connection alive after close it
        property "KeepAlive" 	{ field = 4, type = Boolean, default = true }

        --- The max idle time to keep the connection alive(ms)
        property "MaxIdleTime" 	{ field = 5, type = NaturalNumber, default = 10000 }

        --- The connection pool size
        property "PoolSize" 	{ field = 6, type = NaturalNumber, default = 50 }

        --- The timeout protection for operations(ms)
        property "TimeOut"      { field = 7, type = NaturalNumber, default = 1000, handler = function(self, val) self[1]:set_timeout(val or 1000) end }

        -----------------------------------------------------------
        --                        method                         --
        -----------------------------------------------------------
        --- Closes the connection to the redis.
        function Close(self)
            if self.State == State_Closed then return end

            if self.KeepAlive then
                local ok, err = self[1]:set_keepalive(self.MaxIdleTime, self.PoolSize)
                if not ok then error("Usage: Redis:Close() - " .. (err or "failed"), 2) end
            else
                local ok, err = self[1]:close()
                if not ok then error("Usage: Redis:Close() - " .. (err or "failed"), 2) end
            end

            Trace("[Redis][CLOSE]")

            self.State = State_Closed
        end

        --- Opens a redis connection with the settings specified by the ConnectionString property of the provider-specific Connection object.
        function Open(self)
        	if self.State ~= State_Closed then return end

        	local opt 		= self.Option
        	local ok, err

        	if opt.path then
        		ok, err 	= self[1]:connect(opt.path, opt.pool and { pool = opt.pool })
        	else
        		ok, err 	= self[1]:connect(opt.host, opt.port, opt.pool and { pool = opt.pool })
        	end

            if not ok then
                error("Usage: Redis:Open() - connect failed:" .. (err or "unknown"), 2)
            end

            Trace("[Redis][OPEN]")

            self.State = State_Open

            self[1]:set_timeout(self.TimeOut)
        end

        --- Try sets the the value with non-exist key to the cache, return true if success
        __Arguments__{ NEString, Any, Date }
        function TrySet(self, key, value, expiretime)
            if 1 == self:Execute("setnx", key, serialize(stringProvider, value)) then
                self:Execute("expireat", key, expiretime.Time)
                return true
            end
            return false
        end

        __Arguments__{ NEString, Any, NaturalNumber/nil }
        function TrySet(self, key, value, expiretime)
            if 1 == self:Execute("setnx", key, serialize(stringProvider, value)) then
                if expiretime then self:Execute("expire", key, expiretime) end
                return true
            end
            return false
        end

		--- Set key-value pair to the cache
        __Arguments__{ NEString, Any, Date }
        function Set(self, key, value, expiretime)
            self:Execute("set", key, serialize(stringProvider, value))
            self:Execute("expireat", key, expiretime.Time)
        end

        __Arguments__{ NEString, Any, NaturalNumber/nil }
        function Set(self, key, value, expiretime)
            self:Execute("set", key, serialize(stringProvider, value))
            if expiretime then
                self:Execute("expire", key, expiretime)
            end
        end

        --- Set the expire time for a key
        __Arguments__{ NEString, Date }
        function SetExpireTime(self, key, expiretime)
            self:Execute("expireat", key, expiretime.Time)
        end

        __Arguments__{ NEString, NaturalNumber }
        function SetExpireTime(self, key, expiretime)
            self:Execute("expire", key, expiretime)
        end

        --- Get value for a key
        __Arguments__{ NEString }
        function Get(self, key)
            local value = self:Execute("get", key)
            if value then return deserialize(stringProvider, value) end
        end

        --- Whether the key existed in the cache
        __Arguments__{ NEString }
        function Exist(self, key)
            return self:Execute("exists", key) == 1
        end

        --- Delete a key from the cache
        __Arguments__{ NEString }
        function Delete(self, key)
            self:Execute("del", key)
        end

        --- Execute command and return the result
        __Arguments__{ NEString, Any * 0 }
        function Execute(self, command, ...)
            local cmd = self[1][strlower(command)]
            if cmd then
                local res, err = cmd(self[1], ...)
                if err then error("Redis:Execute(command, ...) - " .. err, 2) end
                return parseValue(res)
            end
        end

        __Arguments__{ NEString, Any * 0 }
        function SafeExecute(self, command, ...)
            local cmd = self[1][strlower(command)]
            if cmd then
                local res, err = cmd(self[1], ...)
                if err then return res, err end
                return parseValue(res)
            end
        end

        __Arguments__{ -RedisScript, NaturalNumber, Any * 0 }
        function RunScript(self, script, numkeys, ...)
            local res, err = script.Run(self, numkeys, ...)
            if err then error("Redis:RunScript(script, ...) - " .. err, 2) end
            return parseValue(res)
        end

        __Arguments__{ -RedisScript, NaturalNumber, Any * 0 }
        function SafeRunScript(self, script, numkeys, ...)
            local res, err = script.Run(self, numkeys, ...)
            if err then return res, err end
            return parseValue(res)
        end

        function FlushScript(self)
            self:SafeExecute("script", "flush")
        end

        -----------------------------------------------------------
        --                      constructor                      --
        -----------------------------------------------------------
        __Arguments__{ ConnectionOption/nil }
        function __new(self, opt)
            local cache, err = redis:new()
            if not cache then throw(err) end
            return { cache, opt or ConnectionOption() }, true
        end
    end)
end)