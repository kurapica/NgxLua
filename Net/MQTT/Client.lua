--===========================================================================--
--                                                                           --
--                          NgxLua.Net.MQTT.Client                           --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2020/09/15                                               --
-- Update Date  :   2020/09/15                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--
require "PLoop.System.Net.MQTT"

PLoop(function(_ENV)
    __Sealed__() class "NgxLua.Net.MQTT.Client" (function(_ENV)
        inherit "System.Net.MQTT.Client"

        export {
            System.Net.MQTT.ClientState, System.Net.MQTT.PacketType, NgxLua.Net.Socket, NgxLua.Net.MQTT.RedisMQTTPublisher, Queue,

            with                = with,
            th_spawn            = ngx.thread.spawn,
            th_wait             = ngx.thread.wait,
            th_kill             = ngx.thread.kill,
            yield               = coroutine.yield,
            status              = coroutine.status,
            req_socket          = ngx.req.socket,
            _SendPacket         = System.Net.MQTT.Client.SendPacket,
            running             = coroutine.running,

            Debug               = Logger.Default[Logger.LogLevel.Debug]
        }

        -- Process the message publisher
        local function processMessagePublisher(self)
            local publisher     = self.MessagePublisher

            while publisher.TopicSubscribed do
                -- Check the published message
                local topic, message, qos = publisher:ReceiveMessage()

                if topic and message then
                    self:Publish(topic, message, qos)
                end
            end
        end

        -----------------------------------------------------------------------
        --                              method                               --
        -----------------------------------------------------------------------
        function Process(self)
            -- Need get the CONNECT packet first
            local ptype, packet = self:ParsePacket()

            self.Coroutine      = running()

            if ptype and ptype == PacketType.CONNECT then
                self:ProcessPacket(ptype, packet)
            end

            -- CLose if not connected
            if self.State ~= ClientState.CONNECTED then return end

            with(self)(function()
                local thMessage

                if self.IsServerSide and self.MessagePublisher then
                    self.MessagePublisher.OnTopicSubscribed = function()
                        thMessage   = th_spawn(processMessagePublisher, self)

                        self.MessageCoroutine = thMessage
                    end
                end

                -- Start the main processing
                while self.State ~= ClientState.CLOSED do
                    local ptype, packet = self:ParsePacket()

                    if ptype then
                        self:ProcessPacket(ptype, packet)
                    elseif self.PacketQueue.Count > 0 then
                        _SendPacket(self, self.PacketQueue:Dequeue(2))
                    end
                end

                -- kill the message publisher if not dead
                if thMessage and status(thMessage) ~= "dead" then
                    th_kill(thMessage)
                end
            end, function(err)
                Debug("[Client]%s closed: %s", self.ClientID, err)
            end)
        end

        -- Check the coroutine to send the packet or queue the packet to be send in the main coroutine
        function SendPacket(self, ptype, packet)
            local current       = running()
            if current == self.Coroutine or current == self.MessageCoroutine then
                _SendPacket(self, ptype, packet)
            else
                self.PacketQueue:Enqueue(ptype, packet)
            end
        end

        -----------------------------------------------------------------------
        --                             property                              --
        -----------------------------------------------------------------------
        --- Whether the client is server side
        property "IsServerSide"     { type = Boolean, default = true }

        --- The socket object
        property "Socket"           { type = System.Net.ISocket, default = function(self) return Socket(req_socket(true)) end }

        --- The packet queue to be used in coroutine can't use the cosocket
        property "PacketQueue"      { set = false, default = function(self) return Queue() end }

    end)
end)