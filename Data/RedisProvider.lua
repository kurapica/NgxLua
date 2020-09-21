--===========================================================================--
--                                                                           --
--                           NgxLua.RedisProvider                            --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2018/09/07                                               --
-- Update Date  :   2020/06/04                                               --
-- Version      :   2.0.1                                                    --
--===========================================================================--
PLoop(function(_ENV)
    --- The Redis implementation
    __Sealed__() class "NgxLua.Redis" (function(_ENV)
        extend "System.Data.ICache"
        extend "System.Data.IHashCache"
        extend "System.Data.IListCache"
        extend "System.Data.ISetCache"
        extend "System.Data.ISortSetCache"

        extend "System.Message.IPublisher"

        local redis 			= require "resty.redis"

        export {
            State_Closed        = System.Data.ConnectionState.Closed,
            State_Open          = System.Data.ConnectionState.Open,
            State_Connecting    = System.Data.ConnectionState.Connecting,
            State_Executing     = System.Data.ConnectionState.Executing,
            State_Fetching      = System.Data.ConnectionState.Fetching,

            Trace               = System.Logger.Default[System.Logger.LogLevel.Trace],
            Debug               = System.Logger.Default[System.Logger.LogLevel.Debug],

            type                = type,
            select              = select,
            error               = error,
            ipairs              = ipairs,
            rawget              = rawget,
            tonumber            = tonumber,
            max                 = math.max,
            min                 = math.min,
            unpack              = unpack or table.unpack,
            parseNilValue       = System.Data.ParseValue,
            strlower            = string.lower,
            yield               = coroutine.yield,
            safeset             = Toolset.safeset,
            loadsnippet         = Toolset.loadsnippet,
            with                = with,

            serialize           = Serialization.Serialize,
            deserialize         = Serialization.Deserialize,

            stringProvider      = Serialization.StringFormatProvider{ ObjectTypeIgnored = true,  Indent = false, LineBreak = "" },

            Date, System.Data.ConnectionState, XList, Queue, Redis,
        }

        local autoGenParseValues= {}
        local autoGenFromValues = {}

        local function parseValue(value, vtype)
            value               = parseNilValue(value)
            return type(value) == "string" and deserialize(stringProvider, value, vtype) or value
        end

        local function fromValue(value)
            return serialize(stringProvider, value)
        end

        local function parseValues(type, ...)
            local count         = select("#", ...)
            local parse         = autoGenParseValues[count]
            if not parse then
                local args      = XList(1, count):Map('i=>"arg" .. i'):Join(", ")
                parse           = loadsnippet([[
                    local parseValue = ...
                    return function(type, ]] .. args .. [[)
                        ]] .. XList(1, count):Map('i=>"arg" .. i .. " = parseValue(arg" .. i .. ", type)"'):Join("\n") .. [[

                        return ]] .. args .. [[
                    end
                ]], "Redis.parseValues" .. count, _ENV)(parseValue)

                autoGenParseValues = safeset(autoGenParseValues, count, parse)
            end
            return parse(type, ...)
        end

        local function fromValues(...)
            local count         = select("#", ...)
            local parse         = autoGenFromValues[count]
            if not parse then
                local args      = XList(1, count):Map('i=>"arg" .. i'):Join(", ")
                parse           = loadsnippet([[
                    local fromValue = ...
                    return function(]] .. args .. [[)
                        ]] .. XList(1, count):Map('i=>"arg" .. i .. " = fromValue(arg" .. i .. ")"'):Join("\n") .. [[

                        return ]] .. args .. [[
                    end
                ]], "Redis.fromValues" .. count, _ENV)(fromValue)

                autoGenFromValues = safeset(autoGenFromValues, count, parse)
            end
            return parse(...)
        end


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

        --- The timeout protection for operations
        property "Timeout"      { field = 7, type = Number, default = 1, handler = function(self, val) self[1]:set_timeout((val or 1) * 1000) end }

        -----------------------------------------------------------
        --                    method - ICache                    --
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

            self[1]:set_timeout(self.Timeout * 1000)
        end

        --- Try sets the the value with non-exist key to the cache, return true if success
        __Arguments__{ NEString, Any, Date }
        function TrySet(self, key, value, expiretime)
            if 1 == self:Execute("setnx", key, fromValue(value)) then
                self:Execute("expireat", key, expiretime.Time)
                return true
            end
            return false
        end

        __Arguments__{ NEString, Any, NaturalNumber/nil }
        function TrySet(self, key, value, expiretime)
            if 1 == self:Execute("setnx", key, fromValue(value)) then
                if expiretime then self:Execute("expire", key, expiretime) end
                return true
            end
            return false
        end

		--- Set key-value pair to the cache
        __Arguments__{ NEString, Any, Date }
        function Set(self, key, value, expiretime)
            self:Execute("set", key, fromValue(value))
            self:Execute("expireat", key, expiretime.Time)
        end

        __Arguments__{ NEString, Any, NaturalNumber/nil }
        function Set(self, key, value, expiretime)
            self:Execute("set", key, fromValue(value))
            if expiretime then
                self:Execute("expire", key, expiretime)
            end
        end

        --- Increase the value by the given increment
        __Arguments__{ NEString, Any, NaturalNumber/nil }
        function Incr(self, key, increment)
            if not increment then
                return self:Execute("INCR", key)
            else
                return self:Execute("INCRBY", key, increment)
            end
        end

        --- Decrease the value by the given decrement
        __Arguments__{ NEString, Any, NaturalNumber/nil }
        function Decr(self, key, decrement)
            if not decrement then
                return self:Execute("DECR", key)
            else
                return self:Execute("DECRBY", key, decrement)
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
        __Arguments__{ NEString, AnyType/nil }
        function Get(self, key, type)
            return parseValue(self:Execute("get", key), type)
        end

        --- Whether the key existed in the cache
        __Arguments__{ NEString }
        function Exist(self, key)
            return 1 == self:Execute("exists", key)
        end

        --- Delete a key from the cache
        __Arguments__{ NEString }
        function Delete(self, key)
            return 1 == self:Execute("del", key)
        end

        --- Execute command and return the result
        __Arguments__{ NEString, Any * 0 }
        function Execute(self, command, ...)
            local cmd           = self[1][strlower(command)]
            if cmd then
                Debug("[REDIS]%s %s", command, { ... })

                local res, err  = cmd(self[1], ...)
                if err then
                    if err == "timeout" and rawget(self[1], "_subscribed") then return nil, err end
                    error("Redis:Execute(command, ...) - " .. err, 2)
                end
                return parseNilValue(res)
            end
        end

        --- Safe execute the command and return the result, no error will be raised
        __Arguments__{ NEString, Any * 0 }
        function SafeExecute(self, command, ...)
            local cmd           = self[1][strlower(command)]
            if cmd then
                local res, err  = cmd(self[1], ...)
                if err then return res, err end
                return parseNilValue(res)
            end
        end

        --- Process a redis script
        __Arguments__{ -RedisScript, NaturalNumber, Any * 0 }
        function RunScript(self, script, numkeys, ...)
            local res, err      = script.Run(self, numkeys, ...)
            if err then error("Redis:RunScript(script, ...) - " .. err, 2) end
            return parseNilValue(res)
        end

        --- Safe process a redis script
        __Arguments__{ -RedisScript, NaturalNumber, Any * 0 }
        function SafeRunScript(self, script, numkeys, ...)
            local res, err      = script.Run(self, numkeys, ...)
            if err then return res, err end
            return parseNilValue(res)
        end

        --- Flush the script in the redis
        function FlushScript(self)
            self:SafeExecute("script", "flush")
        end

        -----------------------------------------------------------
        --                  method - IHashCache                  --
        -----------------------------------------------------------
        --- Try sets the hash field value if not existed, return true if success
        __Arguments__{ NEString, NEString, Any }
        function HTrySet(self, hash, field, value)
            return 1 == self:Execute("hsetnx", hash, field, fromValue(value))
        end

        --- Sets the hash field value to the cache
        __Arguments__{ NEString, NEString, Any }
        function HSet(self, hash, field, value)
            return self:Execute("hset", hash, field, fromValue(value))
        end

        --- Gets the hash field value
        __Arguments__{ NEString, NEString, AnyType/nil }
        function HGet(self, hash, field, type)
            return parseValue(self:Execute("hget", hash, field), type)
        end

        --- Increase the field value by 1 or given increment
        __Arguments__{ NEString, NEString, NaturalNumber/nil }
        function HIncr(self, hash, field, increment)
            return self:Execute("hincrby", hash, field, increment or 1)
        end

        --- Decrease the field value by 1 or given decrement
        __Arguments__{ NEString, NEString, NaturalNumber/nil }
        function HDecr(self, hash, field, decrement)
            return self:Execute("hincrby", hash, field, - (increment or 1))
        end

        --- Whether the field existed in the hash cache
        __Arguments__{ NEString, NEString }
        function HExist(self, hash, field)
            return 1 == self:Execute("hexists", hash, field)
        end

        --- Delete a field from the hash cache
        __Arguments__{ NEString, NEString }
        function HDelete(self, hash, field)
            return 1 == self:Execute("hdel", hash, field)
        end

        --- Return an iterator to fetch all fields in the hash cache
        __Arguments__{ NEString, AnyType/nil } __Iterator__()
        function HPairs(self, hash, type)
            local result        = self:Execute("hgetall", hash)
            if result and result[1] then
                for i = 1, #result, 2 do
                    yield(result[i], parseValue(result[i + 1], type))
                end
            end
        end

        --- Return an iterator over to scan all hash pairs with pattern settings
        __Arguments__{ NEString, NEString/nil, AnyType/nil } __Iterator__()
        function HScanAll(self, hash, pattern, vtype)
            local result
            local cursor        = 0

            repeat
                if pattern then
                    result      = self:Execute("hscan", hash, cursor, "match", pattern)
                else
                    result      = self:Execute("hscan", hash, cursor)
                end

                if result and result[1] then
                    cursor      = tonumber(result[1]) or 0
                    result      = result[2]

                    if result and type(result) == "table" then
                        for i = 1, #result, 2 do
                            yield(result[i], parseValue(result[i + 1], vtype))
                        end
                    end
                else
                    break
                end
            until cursor == 0
        end

        --- Gets the hash element count
        __Arguments__{ NEString }
        function HCount(self, hash)
            return self:Execute("hlen", hash) or 0
        end

        -----------------------------------------------------------
        --                  method - IListCache                  --
        -----------------------------------------------------------
        --- Insert elements at the head of the list
        __Arguments__{ NEString, Any * 1 }
        function LPush(self, list, ...)
            return self:Execute("lpush", list, fromValues(...))
        end

        --- Try insert elements at the head of the list when the list existed
        __Arguments__{ NEString, Any * 1 }
        function TryLPush(self, list, ...)
            return self:Execute("lpushx", list, fromValues(...))
        end

        --- Insert elements at the tail of the list
        __Arguments__{ NEString, Any * 1 }
        function RPush(self, list, ...)
            return self:Execute("rpush", list, fromValues(...))
        end

        --- Try insert elements at the tail of the list when the list existed
        __Arguments__{ NEString, Any * 1 }
        function TryRPush(self, list, ...)
            return self:Execute("rpushx", list, fromValues(...))
        end

        --- Pop and return the elements from the head of the list with the given count or just the head element
        __Arguments__{ NEString, NaturalNumber, AnyType/nil }
        function LPop(self, list, count, type)
            local result        = self:Execute("lrange", list, 0, count - 1)
            if result and result[1] then
                self:Execute("ltrim", list, count, -1)
                return parseValues(type, unpack(result))
            end
        end

        __Arguments__{ NEString, AnyType/nil }
        function LPop(self, list, type)
            return parseValue(self:Execute("lpop", list), type)
        end

        --- Pop and return the elements from the tail of the list with the given count or just the tail element
        __Arguments__{ NEString, NaturalNumber, AnyType/nil }
        function RPop(self, list, count, type)
            local result        = self:Execute("lrange", list, -count, -1)
            if result and result[1] then
                self:Execute("ltrim", list, 0, -count - 1)
                return parseValues(type, unpack(result))
            end
        end

        __Arguments__{ NEString, AnyType/nil }
        function RPop(self, list, type)
            return parseValue(self:Execute("rpop", list), type)
        end

        --- Sets the value to the list with the given index
        __Arguments__{ NEString, Integer, Any }
        function LSet(self, list, index, value)
            self:Execute("lset", list, index, fromValue(value))
        end

        --- Gets the element from the list with the given index
        __Arguments__{ NEString, Integer, AnyType/nil }
        function LItem(self, list, index, type)
            return parseValue(self:Execute("lindex", list, start > 0 and (start - 1) or start), type)
        end

        --- Gets the elements from the list with the given start index(1-base) and the count(default 1)
        __Arguments__{ NEString, Integer, NaturalNumber, AnyType/nil } __Iterator__()
        function LPairs(self, list, start, count, type)
            local stop

            if start < 0 then
                stop        = min(-1, start + count)
            else
                start       = start - 1
                stop        = start + count - 1
            end

            local result    = self:Execute("lrange", list, start, stop)
            if result and result[1] then
                for i, element in ipairs(result) do
                    yield(i, parseValue(element, type))
                end
            end
        end

        --- Gets the list length
        __Arguments__{ NEString }
        function LLength(self, list)
            return self:Execute("llen", list) or 0
        end

        -----------------------------------------------------------
        --                  method - ISetCache                   --
        -----------------------------------------------------------
        --- Gets the element count in the set
        __Arguments__{ NEString }
        function SCount(self, set)
            return self:Execute("scard", set) or 0
        end

        --- Add an element to the set
        __Arguments__{ NEString, Any }
        function SAdd(self, set, element)
            self:Execute("sadd", set, fromValue(element))
        end

        --- Remove an element from the set
        __Arguments__{ NEString, Any }
        function SRemove(self, set, element)
            return self:Execute("srem", set, fromValue(element))
        end

        --- Whether the element is in the set
        __Arguments__{ NEString, Any }
        function SExist(self, set, element)
            return 1 == self:Execute("sismember", set, fromValue(element))
        end

        --- returns an iterator to get all the elements from the set
        __Arguments__{ NEString, AnyType/nil } __Iterator__()
        function SPairs(self, set, type)
            local result        = self:Execute("smembers", set)
            if result and result[1] then
                for _, element in ipairs(result) do
                    yield(parseValue(element, type))
                end
            end
        end

        --- Gets the diff elements of the two sets
        __Arguments__{ NEString, NEString, AnyType/nil } __Iterator__()
        function SDiff(self, set1, set2, type)
            local result        = self:Execute("sdiff", set1, set2)
            if result and result[1] then
                for _, element in ipairs(result) do
                    yield(parseValue(element, type))
                end
            end
        end

        --- Gets the same elements of the two sets
        __Arguments__{ NEString, NEString, AnyType/nil } __Iterator__()
        function SInter(self, set1, set2, type)
            local result        = self:Execute("sinter", set1, set2)
            if result and result[1] then
                for _, element in ipairs(result) do
                    yield(parseValue(element, type))
                end
            end
        end

        --- Gets the union elements of the two sets
        __Arguments__{ NEString, NEString, AnyType/nil } __Iterator__()
        function SUnion(self, set1, set2, type)
            local result        = self:Execute("sunion", set1, set2)
            if result and result[1] then
                for _, element in ipairs(result) do
                    yield(parseValue(element, type))
                end
            end
        end

        -----------------------------------------------------------
        --                method - ISortSetCache                 --
        -----------------------------------------------------------
        --- Gets the elements count from the sorted set
        __Arguments__{ NEString }
        function ZCount(self, zset)
            return self:Execute("zcard", zset) or 0
        end

        --- Add an element to the sorted list with score
        __Arguments__{ NEString, Any, Number }
        function ZAdd(self, zset, element, score)
            self:Execute("ZADD", zset, score, fromValue(element))
        end

        --- Remove an element from the sorted set
        __Arguments__{ NEString, Any }
        function ZRemove(self, zset, element)
            return 1 == self:Execute("zrem", zset, fromValue(element))
        end

        --- Remove elements from the sorted set by the score
        __Arguments__{ NEString, String + Number, String + Number }
        function ZRemoveByScore(self, zset, min, max)
            return self:Execute("zremrangebyscore", zset, min, max)
        end

        --- Remove elements from the sorted set by the rank
        function ZRemoveByRank(self, zset, start, count)
            local stop

            if start < 0 then
                stop            = min(-1, start + count)
            else
                start           = start - 1
                stop            = start + count - 1
            end

            return self:Execute("zremrangebyrank", zset, start, stop)
        end

        --- Increase an element's score in the sorted list
        __Arguments__{ NEString, Any, Number/nil }
        function ZIncr(self, zset, element, increment)
            return self:Execute("zincrby", zset, increment or 1, fromValue(element))
        end

        --- Decrease an element's score in the sorted list
        __Arguments__{ NEString, Any, Number/nil }
        function ZDecr(self, zset, element, decrement)
            return self:Execute("zincrby", zset, -(decrement or 1), fromValue(element))
        end

        --- Gets the index of a given element in the sorted set
        __Arguments__{ NEString, Any }
        function ZRank(self, zset, element)
            return self:Execute("zrank", zset, fromValue(element))
        end

        --- Gets the rev-index of the given element in the sorted set(from largest to smallest)
        function ZRevRank(self, zset, element)
            return self:Execute("zrevrank", zset, fromValue(element))
        end

        --- Gets the score of a given element in the sorted set
        function ZScore(self, zset, element)
            return self:Execute("zscore", zset, fromValue(element))
        end

        --- Return an iterator to get elements from the given start and count by order(from smallest to largest)
        __Arguments__{ NEString, Number/nil, NaturalNumber/nil, AnyType/nil } __Iterator__()
        function ZPairs(self, zset, start, count, type)
            local stop

            if start < 0 then
                stop            = min(-1, start + count)
            else
                start           = start - 1
                stop            = start + count - 1
            end

            local result        = self:Execute("zrange", zset, start, stop)
            if result and result[1] then
                for i, element in ipairs(result) do
                    yield(i, parseValue(element, type))
                end
            end
        end

        --- Return an iterator to get elements from the given start and count by order(from largest to smallest)
        __Arguments__{ NEString, Number/nil, NaturalNumber/nil, AnyType/nil } __Iterator__()
        function ZPairsDesc(self, zset, start, count, type)
            local stop

            if start < 0 then
                stop            = min(-1, start + count)
            else
                start           = start - 1
                stop            = start + count - 1
            end

            local result        = self:Execute("zrevrange", zset, start, stop)
            if result and result[1] then
                for i, element in ipairs(result) do
                    yield(i, parseValue(element, type))
                end
            end
        end

        --- Return an iterator to get elements with scores from the given start and count by order(from smallest to largest)
        __Arguments__{ NEString, Number/nil, NaturalNumber/nil, AnyType/nil } __Iterator__()
        function ZSPairs(self, zset, start, count, type)
            local stop

            if start < 0 then
                stop            = min(-1, start + count)
            else
                start           = start - 1
                stop            = start + count - 1
            end

            local result        = self:Execute("zrange", zset, start, stop, "withscores")
            if result and result[1] then
                for i = 1, #result, 2 do
                    yield(parseValue(result[i], type), result[i + 1])
                end
            end
        end

        --- Return an iterator to get elements with scores from the given start and count by order(from largest to smallest)
        __Arguments__{ NEString, Number/nil, NaturalNumber/nil, AnyType/nil } __Iterator__()
        function ZSPairsDesc(self, zset, start, count, type)
            local stop

            if start < 0 then
                stop            = min(-1, start + count)
            else
                start           = start - 1
                stop            = start + count - 1
            end

            local result        = self:Execute("zrevrange", zset, start, stop, "withscores")
            if result and result[1] then
                for i = 1, #result, 2 do
                    yield(parseValue(result[i], type), result[i + 1])
                end
            end
        end

        -----------------------------------------------------------
        --                   Message Publisher                   --
        -----------------------------------------------------------
        --- The operation to be delayed when still waiting the receiving messages
        property "DelayMessageOperations"   { set = false, default = function() return Queue() end }

        --- Whether the redis is still busy in receiving message
        property "InMessageReading"         { type = Boolean, default = false }

        --- Subscribe a message filter, topic-based, return true if successful, otherwise false and error code is needed
        __Arguments__{ NEString }
        function SubscribeTopic(self, filter)
            if self.InMessageReading then
                self.DelayMessageOperations:Enqueue("SubscribeTopic", 1, filter)
                return true
            end

            local result

            if filter:find("*", 1, true) then
                result          = self:Execute("psubscribe", filter)
            else
                result          = self:Execute("subscribe", filter)
            end

            if result then self.TopicSubscribed = true end

            return result and true or false
        end

        --- Unsubscribe a message filter, topic-based, return true if successful, otherwise false and error code is needed
        __Arguments__{ NEString/nil }
        function UnsubscribeTopic(self, filter)
            if self.InMessageReading then
                if filter then
                    self.DelayMessageOperations:Enqueue("UnsubscribeTopic", 1, filter)
                else
                    self.DelayMessageOperations:Enqueue("UnsubscribeTopic", 0)
                end
                return true
            end

            local result

            if filter and filter:find("*", 1, true) then
                result          = self:Execute("punsubscribe", filter)
            else
                result          = self:Execute("unsubscribe", filter)
            end

            local subscribed    = rawget(self[1], "_subscribed")
            if not subscribed then
                self.TopicSubscribed = false
            end

            return result and true or false
        end

        --- Publish the msssage, it'd give a topic if it's topic-based message
        __Arguments__{ NEString, NEString }
        function PublishMessage(self, topic, message)
            if rawget(self[1], "_subscribed") then
                return with(Redis(self.Option))(function(cache)
                    return cache:Execute("publish", topic, message) == 1
                end)
            else
                return self:Execute("publish", topic, message) == 1
            end
        end

        --- Receive and return the published message
        function ReceiveMessage(self)
            local subscribed        = rawget(self[1], "_subscribed")
            if not subscribed then
                self.TopicSubscribed= false
                return
            end

            self.InMessageReading   = true

            local result            = self:Execute("read_reply")

            self.InMessageReading   = false

            local queue             = self.DelayMessageOperations

            while queue.Count > 0 do
                self[queue:Dequeue()](self, queue:Dequeue(queue:Dequeue()))
            end

            if result and result[1] == "message" then
                return result[2], result[3]
            end
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

    --- A session storage provider based on the ngx.shared.DICT
    __Sealed__() class "System.Context.RedisSessionStorageProvider" (function (_ENV)
        extend "System.Context.ICacheSessionStorageProvider"

        export { NgxLua.Redis }

        -----------------------------------------------------------------------
        --                          inherit method                           --
        -----------------------------------------------------------------------
        function GetCache(self) return Redis(self.ConnectionOption) end

        -----------------------------------------------------------------------
        --                             property                              --
        -----------------------------------------------------------------------
        --- the redis connection option
        property "ConnectionOption" { type = Redis.ConnectionOption }
    end)
end)