--===========================================================================--
--                                                                           --
--                         NgxLua.JWTSessionProvider                         --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2019/07/26                                               --
-- Update Date  :   2019/07/26                                               --
-- Version      :   1.0.0                                                    --
-- Dependencies :   https://github.com/SkyLothar/lua-resty-jwt               --
--===========================================================================--

PLoop(function(_ENV)
    __Sealed__() class "NgxLua.JWTSessionIDManager" (function (_ENV)
        extend "ISessionIDManager"

        export{ "type", "tonumber", Date, HttpCookie.SameSiteValue, System.Web.HttpSession }

        local jwt               = require "resty.jwt"

        -----------------------------------------------------------------------
        --                         inherit property                          --
        -----------------------------------------------------------------------
        property "Priority"     { set = false, default = IHttpContextHandler.HandlerPriority.Lowest }

        -----------------------------------------------------------------------
        --                             property                              --
        -----------------------------------------------------------------------
        --- The time out field name
        property "TimeoutField" { type = String, default = "_timeout"}

        --- The cookie name that to be saved into cookies
        property "CookieName"   { type = String, default = "_JWToken" }

        --- The secret key
        property "SecretKey"    { type = String, default = Guid.New():gsub("-", "") }

        --- The hashing algorithm
        property "HashAlgorithm"{ type = String, default = "HS256" }

        --- The samesite settings of the cookie
        property "SameSite"     { type = SameSiteValue, default = SameSiteValue.Lax }

        -----------------------------------------------------------------------
        --                              method                               --
        -----------------------------------------------------------------------
        --- Gets the json web token from the context of the current HTTP request.
        function GetSessionID(self, context)
            local token         = context.Request.Cookies[self.CookieName]
            if token then
                local jwtObj    = jwt:verify(self.SecretKey, token)
                if type(jwtObj) == "table" and jwtObj.verified then
                    local time  = jwtObj.payload[self.TimeoutField]
                    if (tonumber(time) or 0) <= Date.Now.Time and not jwtObj.payload[HttpSession.TemporaryField] then
                        return nil
                    end
                    jwtObj.context = context
                    return jwtObj
                end
            end
        end

        --- Since there is nothing saved in the server, we don't really need an ID.
        function CreateSessionID(self, context)
            return { context = context }
        end

        --- Deletes the session identifier in the current HTTP response.
        function RemoveSessionID(self, context)
            local cookie        = context.Response.Cookies[self.CookieName]
            if not cookie.Value then cookie.Value = "none" end
            cookie.Expires      = Date.Now:AddMinutes(-1)
        end

        --- Saves a newly created session identifier to the HTTP response.
        function SaveSessionID(self, context, session)
            local cookie        = context.Response.Cookies[self.CookieName]
            local items         = session.RawItems
            local timeout       = not session.IsTemporary and (session.Timeout or Date.Now:AddMinutes(self.TimeoutMinutes)) or nil

            items[self.TimeoutField] = timeout and timeout.Time

            cookie.Value        = jwt:sign(self.SecretKey, {
                header          = { typ = "JWT", alg = self.HashAlgorithm },
                payload         = items,
            })

            cookie.HttpOnly     = true
            cookie.SameSite     = self.SameSite
            cookie.Expires      = timeout
        end
    end)

    --- Represents the interface of sessio1n storage provider
    __Sealed__() class "NgxLua.JWTSessionStorageProvider" (function (_ENV)
        extend "ISessionStorageProvider"

        -----------------------------------------------------------------------
        --                              method                               --
        -----------------------------------------------------------------------
        --- Whether the session ID existed in the storage.
        function Contains(self, id) return id end

        --- Get session item
        function GetItems(self, id) return id.payload end

        --- Try sets the item with an un-existed key, return true if success
        function TrySetItems(self, id, item, timeout) id.payload = item return true end

        --- Update the item with current session data
        function SetItems(self, id, item, timeout) id.context.Session.IsNewSession = true end

        --- Update the item's timeout
        function ResetItems(self, id, timeout) id.context.Session.IsNewSession = true end
    end)
end)