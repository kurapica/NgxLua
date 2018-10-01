--===========================================================================--
--                                                                           --
--                    NgxLua.ShareSessionStorageProvider                     --
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
        extend "System.Web.ICacheSessionStorageProvider"

        export {
            ngx                 = _G.ngx,

            ShareDict,
        }

        -----------------------------------------------------------------------
        --                              method                               --
        -----------------------------------------------------------------------
        function GetCacheObject(self) return ShareDict(self.Storage) end

        -----------------------------------------------------------------------
        --                             property                              --
        -----------------------------------------------------------------------
        --- the share dictionary
        property "Storage"      { field = 1, type = Table }

        -----------------------------------------------------------------------
        --                            constructor                            --
        -----------------------------------------------------------------------
        __Arguments__{ String, Application/nil }
        function __ctor(self, storage, app)
            self.Storage        = ngx.shared[storage]
            self.Application    = app
        end
    end)
end)