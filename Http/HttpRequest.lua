--===========================================================================--
--                                                                           --
--                            NgxLua.HttpRequest                             --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2015/10/22                                              --
-- Update Date  :   2019/04/22                                               --
-- Version      :   1.0.1                                                    --
--===========================================================================--

PLoop(function(_ENV)
    __Sealed__() class "NgxLua.HttpRequest" (function (_ENV)
        inherit (System.Web.HttpRequest)

        export {
            ngx                 = _G.ngx,
            strtrim             = function(s) return s and (s:gsub("^%s*(.-)%s*$", "%1")) or "" end,

            HttpMethod, NgxLua.HttpFiles
        }

        -----------------------------------------------------------
        --                       property                        --
        -----------------------------------------------------------
        property "ContentLength"{ set = false, default = function() return ngx.var.content_length end }
        property "ContentType"  { set = false, default = function() return ngx.var.content_type end }
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

        property "Headers"      { set = false, default = function() return ngx.req.get_headers() end }

        property "Form"         { set = false, default = function() ngx.req.read_body() return ngx.req.get_post_args() or {} end }

        property "HttpMethod"   { set = false, default = function() return HttpMethod[ngx.var.request_method] end }

        property "IsSecureConnection" { set = false, default = function() return ngx.var.https == "on" end }

        property "QueryString"  { set = false, default = function() return ngx.req.get_uri_args() or {} end }

        property "RawUrl"       { set = false, default = function() return ngx.var.request_uri end }

        property "Root"         { set = false, default = function() return ngx.var.document_root end }

        property "Url"          { set = false, default = function(self) return self.Context.Application:Url2Path(ngx.var.uri) end }

        property "Files"        { set = false, default = function(self) return HttpFiles() end }
    end)
end)