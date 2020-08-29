--===========================================================================--
--                                                                           --
--                       NgxLua SessionStorageProvider                       --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2018/10/01                                               --
-- Update Date  :   2018/10/01                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--

PLoop(function(_ENV)
    --- A session storage provider based on the ngx.shared.DICT
    __Sealed__() class "NgxLua.RedisSessionStorageProvider" {
        System.Context.RedisSessionStorageProvider,
        System.Web.ISessionStorageProvider
    }

    --- A session storage provider based on the ngx.shared.DICT
    __Sealed__() class "NgxLua.ShareSessionStorageProvider" {
        System.Context.ShareSessionStorageProvider,
        System.Web.ISessionStorageProvider
    }
end)