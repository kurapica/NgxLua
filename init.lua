--===========================================================================--
--                                                                           --
--                            PLoop for Openresty                            --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2015/10/22                                               --
-- Update Date  :   2018/04/03                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--
require "PLoop.System.Web"
require "PLoop.System.Data"

-- Loading modules
require "NgxLua.Data.ShareDictProvider"
require "NgxLua.Data.RedisProvider"
require "NgxLua.Data.MySQLProvider"

require "NgxLua.Http.HttpFiles"
require "NgxLua.Http.HttpRequest"
require "NgxLua.Http.HttpResponse"
require "NgxLua.Http.HttpContext"
require "NgxLua.Http.SessionStorageProvider"
require "NgxLua.Http.JWTSessionProvider"

require "NgxLua.Thread.LockManager"

-- NgxLua.Net
require "NgxLua.Net.Socket"
require "NgxLua.Net.MQTT.MessagePublisher"
require "NgxLua.Net.MQTT.Client"

import "NgxLua"

PLoop(function(_ENV)
    export { ngx = ngx, next = next, pairs = pairs, tostring = tostring }

    local function getLogHandler(lvl)
        return function(msg) return ngx.log(lvl, msg) end
    end

    -----------------------------------------------------------------------
    --                        System.Date Modify                         --
    -----------------------------------------------------------------------
    System.Date.GetTimeOfDay = ngx.time

    -----------------------------------------------------------------------
    --                          Logger Binding                           --
    -----------------------------------------------------------------------
    System.Logger.Default:AddHandler(getLogHandler(ngx.CRIT),    System.Logger.LogLevel.Fatal)
    System.Logger.Default:AddHandler(getLogHandler(ngx.ERR),     System.Logger.LogLevel.Error)
    System.Logger.Default:AddHandler(getLogHandler(ngx.WARN),    System.Logger.LogLevel.Warn)
    System.Logger.Default:AddHandler(getLogHandler(ngx.NOTICE),  System.Logger.LogLevel.Info)
    System.Logger.Default:AddHandler(getLogHandler(ngx.INFO),    System.Logger.LogLevel.Debug)
    System.Logger.Default:AddHandler(getLogHandler(ngx.DEBUG),   System.Logger.LogLevel.Trace)

    -----------------------------------------------------------------------
    --                      Global Context Handler                       --
    -----------------------------------------------------------------------
    --- the handler to send cookies
    IHttpContextHandler {
        ProcessPhase    = IHttpContextHandler.ProcessPhase.Head,
        Priority        = IHttpContextHandler.HandlerPriority.Lowest,
        AsGlobalHandler = true,
        Process = function(self, context, phase)
            if not context.IsInnerRequest then
                local cookies = context.Response.Cookies
                if next(cookies) then
                    local cache = {}
                    local cnt = 1
                    for name, cookie in pairs(cookies) do
                        cache[cnt] = tostring(cookie)
                        cnt = cnt + 1
                    end
                    ngx.header['Set-Cookie'] = cache
                end
            end
        end,
    }
end)