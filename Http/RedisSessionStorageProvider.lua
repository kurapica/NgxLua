--===========================================================================--
--                                                                           --
--                  NgxLua.RedisSessionStorageProvider                   --
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
    __Sealed__() class "NgxLua.RedisSessionStorageProvider" (function (_ENV)
        extend "ICacheSessionStorageProvider"

        export { Redis }

        -----------------------------------------------------------------------
        --                              method                               --
        -----------------------------------------------------------------------
        function GetCacheObject(self) return Redis(self.ConnectionOption) end

        -----------------------------------------------------------------------
        --                             property                              --
        -----------------------------------------------------------------------
        --- the share dictionary
        property "ConnectionOption"	{ field = 1, type = Redis.ConnectionOption }

        -----------------------------------------------------------------------
        --                            constructor                            --
        -----------------------------------------------------------------------
        __Arguments__{ Redis.ConnectionOption/nil, Application/nil }
        function __ctor(self, option, app)
            self.ConnectionOption 	= option
            self.Application 		= app
        end
    end)
end)