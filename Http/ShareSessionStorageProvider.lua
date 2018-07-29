--===========================================================================--
--                                                                           --
--                  NgxLua.ShareSessionStorageProvider                   --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2016/03/15                                               --
-- Update Date  :   2018/04/02                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--

PLoop(function(_ENV)
    --- A session storage provider based on the ngx.shared.DICT
    __Sealed__() class "NgxLua.ShareSessionStorageProvider" (function (_ENV)
        extend "ISessionStorageProvider"

        export {
            ostime              = os.time,
            pairs               = pairs,
            type                = type,

            serialize           = Serialization.Serialize,
            deserialize         = Serialization.Deserialize,
            ngx                 = _G.ngx,

            stringProvider      = Serialization.StringFormatProvider{ ObjectTypeIgnored = true, Indent = false, LineBreak = "" },
        }

        -----------------------------------------------------------------------
        --                          inherit method                           --
        -----------------------------------------------------------------------
        function Contains(self, id)
            return self.Storage:get(id) and true or false
        end

        function GetItems(self, id)
            local item = self.Storage:get(id)
            if item then return deserialize(stringProvider, item) end
        end

        function RemoveItems(self, id)
            self.Storage:delete(id)
        end

        function CreateItems(self, id, timeout)
            self.Storage:set(id, "", timeout and (timeout.time - ostime()) or 0)
            return {}
        end

        function SetItems(self, id, item, timeout)
            if type(item) ~= "table" then return end
            self.Storage:set(id, serialize(stringProvider, item), timeout and (timeout.time - ostime()) or 0)
        end

        function ResetItems(self, id, timeout)
            self.Storage:set(id, self.Storage:get(id) or "", timeout and (timeout.time - ostime()) or 0)
        end

        -----------------------------------------------------------------------
        --                            constructor                            --
        -----------------------------------------------------------------------
        __Arguments__{ String, Application/nil }
        function __ctor(self, storage, app)
            rawset(self, "Storage", ngx.shared[storage])
            self.Application = app
        end
    end)
end)