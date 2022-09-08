--===========================================================================--
--                                                                           --
--                     NgxLua.Net.MQTT.RedisMQTTPublisher                    --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2020/09/09                                               --
-- Update Date  :   2020/09/09                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--
require "PLoop.System.Net.MQTT"

PLoop(function(_ENV)
    __Sealed__() class "NgxLua.Net.MQTT.RedisMQTTPublisher" (function(_ENV)
        inherit (NgxLua.Redis)
        extend "System.Net.MQTT.IMQTTPublisher"

        export {
            RedisMQTTPublisher, System.Net.MQTT.QosLevel, NgxLua.Redis, Queue,

            pairs               = pairs,
            next                = next,
            tonumber            = tonumber,
            yield               = coroutine.yield,
            strtrim             = Toolset.trim,
            with                = with,
        }

        local function parseToRedisPattern(pattern)
            pattern             = strtrim(pattern)
            -- The pattern can't be empty and not started with $
            if pattern == "" or pattern:match("^%$") then return end

            return pattern:gsub("(.?)%+(.?)", function(a, b)
                if a and a ~= "/" or b and b ~= "/" then
                    return a .. "%+" .. b
                else
                    return a .. "*" .. b
                end
            end):gsub("(.?)#$", function(a)
                if not a or a == "" or a == "/" then
                    return a .. "*"
                end
            end)
        end

        -----------------------------------------------------------
        --                    static property                    --
        -----------------------------------------------------------
        --- The redis key to keep the retain messages
        __Static__() property "RetainMessageKey" { type = NEString, default = "NGXLUA:MQTT:RETAIN" }

        -----------------------------------------------------------
        --                        method                         --
        -----------------------------------------------------------
        --- Save the retain message for a topic
        function SaveRetainMessage(self, topic, message)
            with(Redis(self.Option))(function(cache)
                cache:HSet(RedisMQTTPublisher.RetainMessageKey, topic, message)
            end)
        end

        --- Delete the retain message from a topic
        function DeleteRetainMessage(self, topic)
            return with(Redis(self.Option))(function(cache)
                -- Remove the retain message
                return cache:HDelete(RedisMQTTPublisher.RetainMessageKey, topic)
            end)
        end

        --- Return an iterator to get all retain messages for based on a topic filter
        __Iterator__() function GetRetainMessages(self, filter)
            with(Redis(self.Option))(function(cache)
                for topic, message in cache:HScanAll(RedisMQTTPublisher.RetainMessageKey, filter and parseToRedisPattern(filter)) do
                    yield(topic, message)
                end
            end)
        end

        --- Subscribe a message filter, topic-based, return true if successful, otherwise false and error code is needed
        function SubscribeTopic(self, filter)
            return super.SubscribeTopic(self, parseToRedisPattern(filter))
        end

        --- Unsubscribe a message filter, topic-based, return true if successful, otherwise false and error code is needed
        function UnsubscribeTopic(self, filter)
            return super.UnsubscribeTopic(self, parseToRedisPattern(filter))
        end
    end)


    __Sealed__() class "NgxLua.Net.MQTT.ShareDictMQTTPublisher" (function(_ENV)
        extend "System.Net.MQTT.IMQTTPublisher"

        export {
            ShareDictMQTTPublisher, Guid,

            ipairs              = ipairs,
            next                = next,
            tonumber            = tonumber,
            safeset             = Toolset.safeset,
            yield               = coroutine.yield,
            strtrim             = Toolset.trim,
            with                = with,
            shared              = _G.ngx.shared,
            sleep               = _G.ngx.sleep,
        }

        local function parseToLuaPattern(pattern)
            pattern                     = strtrim(pattern)
            -- The pattern can't be empty and not started with $
            if pattern == "" or pattern:match("^%$") then return end

            return "^" .. pattern:gsub("(.?)%+(.?)", function(a, b)
                if a and a ~= "/" or b and b ~= "/" then
                    return a .. "%+" .. b
                else
                    return a .. "[^/]*" .. b
                end
            end):gsub("(.?)#$", function(a)
                if not a or a == "" or a == "/" then
                    return a .. ".*"
                end
            end) .. "$"
        end

        -----------------------------------------------------------
        --                    static property                    --
        -----------------------------------------------------------
        --- The redis key to keep the retain messages
        __Static__() property "RetainShareTable" { type = NEString }

        --- The redis key to keep the retain messages
        __Static__() property "MessageShareTable"{ type = NEString }

        --- The interval to check the new subscribed topic messages
        __Static__() property "Interval"        { type = NaturalNumber, default = 1 }

        --- The topic filters
        property "RegisteredFilters"            { default = function() return {} end }

        -----------------------------------------------------------
        --                        method                         --
        -----------------------------------------------------------
        --- Subscribe a message filter, topic-based, return true if successful, otherwise false and error code is needed
        __Arguments__{ NEString }
        function SubscribeTopic(self, filter)
            self.RegisteredFilters[parseToLuaPattern(filter)] = true
            return true
        end

        --- Unsubscribe a message filter, topic-based, return true if successful, otherwise false and error code is needed
        __Arguments__{ NEString/nil }
        function UnsubscribeTopic(self, filter)
            self.RegisteredFilters[parseToLuaPattern(filter)] = nil
            return true
        end

        --- Publish the msssage, it'd give a topic if it's topic-based message
        __Arguments__{ NEString, NEString }
        function PublishMessage(self, topic, message)
            local table         = shared[ShareDictMQTTPublisher.MessageShareTable]
            table:set(topic, Toolset.tostring({ content = message, token = Guid.New() }))
        end

        --- Receive and return the published message
        function ReceiveMessage(self)
            if not next(self.RegisteredFilters) then return end
            local table         = shared[ShareDictMQTTPublisher.MessageShareTable]

            for _, topic in ipairs(table:get_keys()) do
                local message   = table:get(topic)
                if message and self[topic] ~= message then
                    for pattern in pairs(self.RegisteredFilters) do
                        if topic:match(pattern) then
                            self[topic] = message
                            return topic, Toolset.parsestring(message).content
                        end
                    end
                end
            end

            sleep(ShareDictMQTTPublisher.Interval)
        end

--[[        --- Save the retain message for a topic
        function SaveRetainMessage(self, topic, message)
            local table         = shared[ShareDictMQTTPublisher.RetainShareTable]
            table:set(topic, message)
        end

        --- Delete the retain message from a topic
        function DeleteRetainMessage(self, topic)
            local table         = shared[ShareDictMQTTPublisher.RetainShareTable]
            table:delete(topic)
        end

        --- Return an iterator to get all retain messages for based on a topic filter
        __Iterator__() function GetRetainMessages(self, filter, luaPattern)
            Debug("[GetRetainMessages] %q", luaPattern)
            local table         = shared[ShareDictMQTTPublisher.RetainShareTable]
            for _, topic in ipairs(table:get_keys()) do
                yield(topic, table:get(topic))
            end-
        end--]]
    end)
end)