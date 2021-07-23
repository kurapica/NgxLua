--===========================================================================--
--                                                                           --
--                          NgxLua.Net.MQTT.Proxy                            --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2021/07/23                                               --
-- Update Date  :   2021/07/23                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--
require "PLoop.System.Net.MQTT"

PLoop(function(_ENV)
    --- Use openresty as MQTT Proxy
    __Sealed__() class "NgxLua.Net.MQTT.Proxy" (function(_ENV)
        inherit "System.Net.MQTT.Client"

        export {
            System.Net.MQTT.ClientState, System.Net.MQTT.PacketType, NgxLua.Net.Socket, Queue,
            System.Net.MQTT.Client,

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
        local function processProxy(self, retryTimes)
            self.Coroutine      = running()

            with(self)(function(proxy)
                local connected = false
                for i = 1, retryTimes do
                    local ok, err = pcall(self.Socket.Connect, self.Socket, self.Address, self.Port)

                    if not ok then
                        if i == retryTimes then
                            Debug("[Proxy]Connect server failed - %s", err)
                        end
                    else
                        connected = true
                        break
                    end
                end

                while connected do
                    local ptype, packet = self:ParsePacket()

                    if ptype then
                        self.Client:SendPacket(ptype, packet)

                        if ptype == PacketType.DISCONNECT then
                            break
                        end
                    elseif self.PacketQueue.Count > 0 then
                        _SendPacket(self, self.PacketQueue:Dequeue(2))
                    end
                end
            end, function(err)
                Debug("[Proxy]Connect server failed - %s", err)
            end)

            self.Coroutine      = false
        end
        -----------------------------------------------------------------------
        --                              method                               --
        -----------------------------------------------------------------------
        --- The method used to filter the packets, return false if failed, nil or true pass
        __Abstract__() function Filter(self, ptype, packet)
        end

        --- Works as a proxy with connection arguments self:Process("127.0.0.1", 1883)
        function Process(self, address, port)
            self.Coroutine              = running()

            -- Need get the CONNECT packet first
            local ptype, packet         = self:ParsePacket()
            local proxy

            if ptype and ptype == PacketType.CONNECT then
                self.ClientID           = packet.clientID
                self.ProtocolLevel      = packet.level

                -- Create proxy
                proxy                   = Client{
                    Address             = address,
                    Port                = port,
                    ProtocolLevel       = packet.level,
                    Socket              = Socket(),

                    PacketQueue         = Queue(),
                    SendPacket          = self.SendPacket,

                    Client              = self,
                }

                proxy.ConnectTimeout    = self.ConnectTimeout
                proxy.ReceiveTimeout    = self.ReceiveTimeout
                proxy.SendTimeout       = self.SendTimeout

                proxy.PacketQueue:Enqueue(ptype, packet)
            else
                return self:CloseClient()
            end

            with(self)(function()
                local thProxy           = th_spawn(processProxy, proxy, self.RetryTimes)

                while proxy.Coroutine do
                    ptype, packet       = self:ParsePacket()

                    if ptype then
                        if self:Filter(ptype, packet) ~= false then
                            proxy:SendPacket(ptype, packet)
                        end
                    elseif self.PacketQueue.Count > 0 then
                        _SendPacket(self, self.PacketQueue:Dequeue(2))
                    end
                end

                -- kill the message publisher if not dead
                if thProxy and status(thProxy) ~= "dead" then
                    th_kill(thProxy)
                end

                -- Try Send the rest queue packet, only send the disconnect packet
                while self.PacketQueue.Count > 0 do
                    local ptype, packet = self.PacketQueue:Dequeue(2)
                    if ptype == PacketType.DISCONNECT then
                        _SendPacket(self, ptype, packet)
                        break
                    end
                end
            end, function(err)
                Debug("[Proxy] %s", err)
            end)
        end

        -- Check the coroutine to send the packet or queue the packet to be send in the main coroutine
        function SendPacket(self, ptype, packet)
            if running() == self.Coroutine then
                _SendPacket(self, ptype, packet)
            else
                self.PacketQueue:Enqueue(ptype, packet)
            end
        end

        -----------------------------------------------------------------------
        --                             property                              --
        -----------------------------------------------------------------------
        --- Gets or sets a value that specifies the amount of time after which a synchronous Connect call will time out(in seconds)
        property "ConnectTimeout"   { type = Number, default = 1 }

        --- Whether the client is server side
        property "IsServerSide"     { type = Boolean, default = true }

        --- The socket object
        property "Socket"           { type = System.Net.ISocket, default = function(self) return Socket(req_socket(true)) end }

        --- The packet queue to be used in coroutine can't use the cosocket
        property "PacketQueue"      { set = false, default = function(self) return Queue() end }

        --- Retry connect to the server
        property "RetryTimes"       { type = Number, default = 1 }
    end)
end)