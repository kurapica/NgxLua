--===========================================================================--
--                                                                           --
--                           NgxLua.Net.WebSocket                            --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2020/09/15                                               --
-- Update Date  :   2020/09/15                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--
require "PLoop.System.Net"

PLoop(function(_ENV)
    --- The socket implementation based on ngx.req.socket
    __Sealed__() class "NgxLua.Net.WebSocket" (function(_ENV)
        extend "System.Net.ISocket"

        export {
            WebSocket, System.Net.SocketShutdown, System.Net.TimeoutException, System.Net.SocketException,
            System.Text.StringReader, Queue,

            throw               = throw,
            strchar             = string.char,
            strbyte             = string.byte,
            yield               = coroutine.yield,
            Trace               = Logger.Default[Logger.LogLevel.Trace],
            server              = require "resty.websocket.server",
        }

        ---------------------------------------------------
        --                   property                    --
        ---------------------------------------------------
        --- Gets or sets a value that specifies the amount of time after which a synchronous Accept call will time out(in seconds)
        property "AcceptTimeout"     { type = Number, default = 1 }

        --- Gets or sets a value that specifies the amount of time after which a synchronous Receive call will time out(in seconds)
        property "ReceiveTimeout"    { type = Number, default = 5 }

        --- Gets or sets a value that specifies the amount of time after which a synchronous Send call will time out(in seconds)
        property "SendTimeout"       { type = Number, default = 1 }

        --- Gets or sets a value that specifies the amount of time after which a synchronous Connect call will time out(in seconds)
        property "ConnectTimeout"    { type = Number, default = 1 }

        --- The buffer queu
        property "Buffer"            { set = false, default = function() return Queue() end }

        ---------------------------------------------------
        --                    method                     --
        ---------------------------------------------------
        --- Closes the Socket connection and releases all associated resources
        function Close(self)
            return self[0].send_close and self[0]:send_close()
        end

        --- Receives data from a bound Socket
        __Arguments__{ Integer }
        function Receive(self, length)
            if self.Buffer.Count >= length then
                return strchar(self.Buffer:Dequeue(length))
            end

            self[0]:set_timeout(self.ReceiveTimeout * 1000)

            while true do
                local data, typ, err= self[0]:recv_frame()

                if typ == "close" then
                    -- send a close frame back
                    self[0]:send_close(1000)
                    return
                elseif typ == "ping" then
                    self[0]:send_pong(data)
                elseif typ == "text" or typ == "binary" then
                    self.Buffer:Enqueue(strbyte(data, 1, -1))

                    if self.Buffer.Count >= length then
                        return strchar(self.Buffer:Dequeue(length))
                    end
                elseif err and (err == "timeout" or err:match("failed to receive")) then
                    throw(TimeoutException())
                elseif err then
                    error(err)
                end
            end
        end

        --- Sends data to a connected Socket
        function Send(self, ...)
            self[0]:set_timeout(self.SendTimeout * 1000)

            return self[0]:send_binary(...)
        end

        ---------------------------------------------------
        --                  constructor                  --
        ---------------------------------------------------
        __Arguments__{ NaturalNumber/nil, Boolean/nil }
        function __ctor(self, maxpayload, masked)
            local tcp, err      = server:new{ max_payload_len = maxpayload, send_masked = masked }
            if not tcp then throw(SocketException(err)) end

            self[0]             = tcp
        end

        __Arguments__{ Userdata + Table }
        function __ctor(self, sock)
            self[0]             = sock
        end
    end)
end)