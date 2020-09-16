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
			System.Net.MQTT.ClientState, System.Net.MQTT.PacketType, NgxLua.Net.Socket, NgxLua.Net.MQTT.RedisMQTTPublisher,

			with                = with,
			th_spawn 			= ngx.thread.spawn,
			th_wait             = ngx.thread.wait,
			th_kill             = ngx.thread.kill,
			yield               = coroutine.yield,
			status 				= coroutine.status,
			req_socket 			= ngx.req.socket,

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

		-- Process the client
		local function processClientPackets(self)
            while self.State ~= ClientState.CLOSED do
                local ptype, packet = self:ParsePacket()

                if ptype then
                    self:ProcessPacket(ptype, packet)
                end
            end
		end

	    -----------------------------------------------------------------------
	    --                              method                               --
	    -----------------------------------------------------------------------
	    function Process(self)
	    	-- Need get the CONNECT packet first
	    	local ptype, packet = self:ParsePacket()

	    	if ptype and ptype == PacketType.CONNECT then
	    		self:ProcessPacket(ptype, packet)
	    	end

	    	-- CLose if not connected
	    	if self.State ~= ClientState.CONNECTED then return end

	    	if self.KeepAlive then
	    		-- Keep the receive waiting for a proper time
	    		self.ReceiveTimeout = self.KeepAlive
	    	end

	    	with(self)(function()
		    	local thMessage

		    	if self.IsServerSide and self.MessagePublisher then
		    		self.MessagePublisher.OnTopicSubscribed = function()
		    			thMessage 	= th_spawn(processMessagePublisher, self)
		    		end
		    	end

		    	-- Start the main processing
		    	processClientPackets(self)

		    	-- kill the message publisher if not dead
		    	if thMessage and status(thMessage) ~= "dead" then
		    		th_kill(thMessage)
		    	end
	    	end, function(err)
	    		Debug("[Client]%s closed: %s", self.ClientID, err)
	    	end)
        end

	    -----------------------------------------------------------------------
	    --                             property                              --
	    -----------------------------------------------------------------------
	    --- Whether the client is server side
	    property "IsServerSide"     { type = Boolean, default = true }

	    --- The socket object
	    property "Socket"           { type = System.Net.ISocket, default = function(self) return Socket(req_socket(true)) end }
	end)
end)