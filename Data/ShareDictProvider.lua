--===========================================================================--
--                                                                           --
--                         NgxLua.ShareDictProvider                          --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2018/09/07                                               --
-- Update Date  :   2018/09/07                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--
PLoop(function(_ENV)
    namespace "NgxLua"

    __Sealed__() class "ShareDict" (function(_ENV)
        extend "System.Data.ICache"

        export {
            pairs               = pairs,
            type                = type,

            serialize           = Serialization.Serialize,
            deserialize         = Serialization.Deserialize,
            ngx                 = _G.ngx,

            stringProvider      = Serialization.StringFormatProvider{ ObjectTypeIgnored = false, Indent = false, LineBreak = "" },

            Date,
        }
        -----------------------------------------------------------
        --                       property                        --
        -----------------------------------------------------------
        --- the shared table provided by the openresty
        property "Storage" { set = false, field = 1 }

        -----------------------------------------------------------
        --                        method                         --
        -----------------------------------------------------------
        --- Try sets the the value with non-exist key to the cache, return true if success
        function TrySet(self, key, value, expiretime)
            local ok, err       = self[1]:add(key, serialize(stringProvider, value), expiretime and (expiretime - Date.Now) or 0)
            return ok
        end

		--- Set key-value pair to the cache
        __Arguments__{ Any, Any, Date/nil }
        function Set(self, key, value, expiretime)
            local ok, err       = self[1]:set(key, serialize(stringProvider, value), expiretime and (expiretime - Date.Now) or 0)
            if not ok then error("Usage: ShareDict:Set(key, value, expire) - " .. err, 2) end
        end

        --- Set the expire time for a key
        __Arguments__{ Any, Date }
        function SetExpireTime(self, key, expiretime)
            self[1]:set(key, self[1]:get(key) or "", expiretime - Date.Now)
        end

        --- Get value for a key
        function Get(self, key)
            local value         = self[1]:get(key)
            if value then return deserialize(stringProvider, value) end
            return value
        end

        --- Whether the key existed in the cache
        function Exist(self, key)
            return self[1]:get(key) ~= nil
        end

        --- Delete a key from the cache
        function Delete(self, key)
            self[1]:delete(key)
        end

        -----------------------------------------------------------
        --                      constructor                      --
        -----------------------------------------------------------
        __Arguments__{ String }
        function __new(self, storage) return { ngx.shared[storage] }, true end

        __Arguments__{ Table }
        function __new(self, storage) return { storage }, true end
    end)
end)