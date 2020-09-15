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
                if not a or a == "/" then
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
end)