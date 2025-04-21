--===========================================================================--
--                                                                           --
--                          NgxLua.HttpContext                           --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2015/10/22                                               --
-- Update Date  :   2018/04/03                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--

PLoop(function(_ENV)
    --- Used to store the context in ngx
    __Sealed__()
    interface "NgxLua.IContextHolder"   (function(_ENV)
        export { ngx = ngx, Context }

        function __init(self)
            ngx.ctx[Context]            = self
        end
    end)

    __Sealed__()
    class "NgxLua.HttpContext"          (function (_ENV)
        inherit (System.Web.HttpContext)
        extend  (NgxLua.IContextHolder)

        export { NgxLua.HttpRequest, NgxLua.HttpResponse }

        -----------------------------------------------------------
        --                       property                        --
        -----------------------------------------------------------
        property "Request"  { set = false, default = function (self) return HttpRequest(self)  end }
        property "Response" { set = false, default = function (self) return HttpResponse(self) end }
    end)
end)