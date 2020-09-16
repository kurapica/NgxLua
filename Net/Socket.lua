--===========================================================================--
--                                                                           --
--                            NgxLua.Net.Socket                              --
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
    __Sealed__() class "NgxLua.Net.Socket" (function(_ENV)
        extend "System.Net.ISocket"

        export {
            Socket, System.Net.SocketShutdown, System.Net.TimeoutException, System.Net.SocketException,

            throw               = throw,
            yield               = coroutine.yield,
            socket              = ngx.socket,
        }

        __Sealed__() struct "ConnectionOptions" {
            { name = "pool",        type = String },
            { name = "pool_size",   type = NaturalNumber },
            { name = "backlog",     type = NaturalNumber },
        }

        ---------------------------------------------------
        --                   property                    --
        ---------------------------------------------------
        --- Gets or sets a value that specifies the amount of time after which a synchronous Accept call will time out(in seconds)
        property "AcceptTimeout"     { type = Number, default = 1 }

        --- Gets or sets a value that specifies the amount of time after which a synchronous Receive call will time out(in seconds)
        property "ReceiveTimeout"    { type = Number, default = 1 }

        --- Gets or sets a value that specifies the amount of time after which a synchronous Send call will time out(in seconds)
        property "SendTimeout"       { type = Number, default = 1 }

        --- Gets or sets a value that specifies the amount of time after which a synchronous Connect call will time out(in seconds)
        property "ConnectTimeout"    { type = Number, default = 1 }

        ---------------------------------------------------
        --                    method                     --
        ---------------------------------------------------
        --- Establishes a connection to a remote host
        __Arguments__{ NEString, NaturalNumber/nil, ConnectionOptions/nil }
        function Connect(self, address, port, options)
            self[0]:settimeout(self.ConnectTimeout * 1000)

            local ret, err      = self[0]:connect(address, port, options)
            if err == "timeout" then throw(TimeoutException()) end
            if not ret then throw(SocketException(err)) end
            return true
        end

        --- Do the SSL/TLS handshake on the currently established connection
        -- @todo
        function SSLHandShake(self) end

        --- Closes the Socket connection and releases all associated resources
        function Close(self)
            return self[0].close and self[0]:close()
        end

        --- Receives data from a bound Socket
        function Receive(self, ...)
            self[0]:settimeout(self.ReceiveTimeout * 1000)
            local ret, err     = self[0]:receive(...)
            if not ret then
                if err == "timeout" then throw(TimeoutException()) end
                throw(SocketException(err))
            end

            return ret
        end

        --- Receives any data with a max limit
        __Arguments__{ NaturalNumber }
        function ReceiveAny(self, max)
            self[0]:settimeout(self.ReceiveTimeout * 1000)
            local ret, err     = self[0]:receiveany(max)
            if not ret then
                if err == "timeout" then throw(TimeoutException()) end
                throw(SocketException(err))
            end

            return ret
        end

        --- Return an iterator that can be used to read the data stream
        -- until meet the specified pattern or an error occurs
        __Iterator__() __Arguments__{ NEString, Boolean/nil }
        function ReceiveUntil(pattern, inclusive)
            self[0]:settimeout(self.ReceiveTimeout * 1000)
            local reader        = self[0]:receiveuntil(pattern, { inclusive = inclusive or false })
            local data, err, partial = reader()

            while data do
                yield(data, partial)
                data, err, partial = reader()
            end

            if not ret then
                if err == "timeout" then throw(TimeoutException()) end
                throw(SocketException(err))
            end
        end

        --- Sends data to a connected Socket
        function Send(self, ...)
            self[0]:settimeout(self.SendTimeout * 1000)

            return self[0]:send(...)
        end

        --- Puts the current socket's connection into the connection pool
        __Arguments__{ NaturalNumber/nil, NaturalNumber/nil }
        function SetKeepAlive(timeout, size)
            return self[0]:setkeepalive(timeout, size)
        end

        -- Sleep for several seconds
        __Arguments__{ Number }
        function Sleep(self, time)
            socket.sleep(time)
        end

        ---------------------------------------------------
        --                  constructor                  --
        ---------------------------------------------------
        __Arguments__{}
        function __ctor(self)
            local tcp, err      = socket.tcp()
            if not tcp then throw(SocketException(err)) end

            self[0]             = tcp
        end

        __Arguments__{ Userdata + Table }
        function __ctor(self, sock)
            self[0]             = sock
        end
    end)
end)