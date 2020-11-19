--===========================================================================--
--                                                                           --
--                            NgxLua.HttpRequest                             --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2015/10/22                                               --
-- Update Date  :   2020/09/01                                               --
-- Version      :   1.1.2                                                    --
--===========================================================================--

PLoop(function(_ENV)
    __Sealed__() class "NgxLua.HttpRequest" (function (_ENV)
        inherit (System.Web.HttpRequest)

        import "System.IO"

        export {
            ngx                 = _G.ngx,
            with                = with,
            pcall               = pcall,
            strtrim             = Toolset.trim,
            ParseJson           = System.Web.ParseJson,

            HttpMethod, NgxLua.HttpFiles, FileReader
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

        property "Form"         { default = function(self)
                local ctype     = self.ContentType
                if ctype and ctype:find("application/json", 1, true) then
                    local body  = self.Body
                    local ok, rs= pcall(ParseJson, body)
                    return ok and rs or {}
                elseif ctype and ctype:find("multipart/form-data", 1, true) then
                    -- Normally the data should be handled by the HttpFiles,
                    -- Since the code access the Form first, all files will be skipped
                    local form  = {}

                    for name, file in self.Files:GetIterator(form) do
                        file:Skip()
                    end

                    return form
                else
                    ngx.req.read_body()
                    return ngx.req.get_post_args() or {}
                end
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
                                                return with(file)(function(reader) return reader:ReadToEnd() end)
                                            end
                                        end

                                        -- Block the next retrieve
                                        return data or ""
                                    end
                                }
    end)
end)