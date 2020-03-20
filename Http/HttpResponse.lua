--===========================================================================--
--                                                                           --
--                            NgxLua.HttpResponse                            --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2015/10/22                                              --
-- Update Date  :   2020/03/20                                               --
-- Version      :   1.1.0                                                    --
--===========================================================================--

PLoop(function(_ENV)
    class "NgxLua.HttpResponse" (function (_ENV)
        inherit (System.Web.HttpResponse)

        local BUFF_SIZE = 4096

        export { ngx = _G.ngx }

        -----------------------------------------------------------
        --                       property                        --
        -----------------------------------------------------------
        __Indexer__(String)
        property "Header"           { set = function(self, key, value) ngx.header[key] = value end }

        property "Write"            { set = false , default = function (self)
                local cache     = {}
                local index     = 1
                local length    = 0

                -- Register for finish
                self._Cache     = cache

                return function (text)
                    if text then
                        cache[index] = text
                        length  = length + #text
                        if length >= BUFF_SIZE then
                            -- Send out the buff
                            ngx.print(cache)
                            ngx.flush()

                            -- Use a new buff
                            cache   = {}
                            index   = 1
                            length  = 0
                            self._Cache = cache
                        else
                            index   = index + 1
                        end
                    end
                end
            end
        }

        property "StatusCode"   { type = HTTP_STATUS, handler = function (self, value) ngx.status = value end }

        -----------------------------------------------------------
        --                        method                         --
        -----------------------------------------------------------
        function SendHeaders(self)
            return ngx.send_headers()
        end

        function Close(self)
            if self._Cache and self._Cache[1] then ngx.print(self._Cache) ngx.flush() end
            return ngx.eof()
        end
    end)
end)