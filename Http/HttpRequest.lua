--===========================================================================--
--                                                                           --
--                            NgxLua.HttpRequest                             --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2015/10/22                                               --
-- Update Date  :   2020/08/06                                               --
-- Version      :   1.1.1                                                    --
--===========================================================================--

PLoop(function(_ENV)
    __Sealed__() class "NgxLua.HttpRequest" (function (_ENV)
        inherit (System.Web.HttpRequest)

        import "System.IO"

        export {
            ngx                 = _G.ngx,
            pcall               = pcall,
            strtrim             = Toolset.trim,
            ParseJson           = System.Web.ParseJson,

            HttpMethod, NgxLua.HttpFiles
        }

        -----------------------------------------------------------
        --                       property                        --
        -----------------------------------------------------------
        property "Headers"      { set = false, default = function() return ngx.req.get_headers() end }

        property "Cookies"      { set = false, default = function()
                local cookies   = {}
                local _cookie   = ngx.var.http_cookie

                if _cookie then
                    _cookie:gsub("([^;=]*)=([^;]*)", function(key, value)
                        key     = strtrim(key)
                        value   = strtrim(value)
                        if key ~= "" then cookies[key] = value end
                    end)
                end

                return cookies
            end
        }

        property "Form"         { set = false, default = function(self)
                local ctype     = self.ContentType or "application/x-www-form-urlencoded"
                if ctype:match("application/json") then
                    local body  = self.Body
                    local ok, rs= pcall(ParseJson, body)
                    return ok and rs or {}
                elseif ctype:match("application/x-www-form-urlencoded") then
                    ngx.req.read_body()
                    return ngx.req.get_post_args() or {}
                end

                return {}
            end
        }

        property "HttpMethod"   { set = false, default = function() return HttpMethod[ngx.var.request_method] end }

        property "IsSecureConnection" { set = false, default = function() return ngx.var.https == "on" end }

        property "QueryString"  { set = false, default = function() return ngx.req.get_uri_args() or {} end }

        property "RawUrl"       { set = false, default = function() return ngx.var.request_uri end }

        property "Root"         { set = false, default = function() return ngx.var.document_root end }

        property "Url"          { set = false, default = function(self) return self.Context.Application:Url2Path(ngx.var.uri) end }

        property "Files"        { set = false, default = function(self) return HttpFiles() end }

        property "Body"         { set = false, default = function(self)
                                        ngx.req.read_body()
                                        local data     = ngx.req.get_body_data()

                                        if data == nil then
                                            local file = ngx.req.get_body_file()

                                            if file then
                                                file   = FileReader(file, "rb")
                                                return with(file)(function(reader) reader:ReadToEnd() end)
                                            end
                                        end

                                        -- Block the next retrieve
                                        return data or ""
                                    end
                                }
    end)
end)