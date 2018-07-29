--===========================================================================--
--                                                                           --
--                             NgxLua.HttpFiles                              --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2016/04/24                                               --
-- Update Date  :   2018/04/03                                               --
-- Version      :   1.0.0                                                    --
--===========================================================================--

PLoop(function(_ENV)
    __Sealed__() class "NgxLua.HttpFiles" (function (_ENV)
        extend "Iterable"

        __AutoIndex__()
        enum "FileHash" {
            "NONE",
            "MD5",
            "SHA",
            "SHA1",
            "SHA224",
            "SHA256",
            "SHA384",
            "SHA512",
        }

        export {
            type                = type,
            tblconat            = table.concat,
            yield               = coroutine.yield,
            RestyUpload         = require "resty.upload",
            GetContextFromStack = Context.GetContextFromStack,

            HttpFiles
        }

        local MAX_SIZE          = {}

        class "HttpFile" (function(_ENV)

            export {
                System.IO.FileWriter, System.IO.Path, FileHash, GetPhysicalPath = Web.GetPhysicalPath,
                max = math.max, type = type, error = error
            }

            local strlib            = require("resty.string")

            FileHashHandler         = {
                [FileHash.MD5]      =  require("resty.md5"),
                [FileHash.SHA]      =  require("resty.sha"),
                [FileHash.SHA1]     =  require("resty.sha1"),
                [FileHash.SHA224]   =  require("resty.sha224"),
                [FileHash.SHA256]   =  require("resty.sha256"),
                [FileHash.SHA384]   =  require("resty.sha384"),
                [FileHash.SHA512]   =  require("resty.sha512"),
            }

            -----------------------------------------------------------
            --                       property                        --
            -----------------------------------------------------------
            --- the upload file's file name
            property "Name" { set = false, get = function(self) return self[1] end }

            --- the file's hash result
            property "Hash" { set = false, get = function(self) return self[3] or nil end }

            --- the file's size
            property "Size" { set = false, get = function(self) return self[2] or nil end }

            -----------------------------------------------------------
            --                        method                         --
            -----------------------------------------------------------
            __Arguments__{ String + System.IO.TextWriter, FileHash/FileHash.NONE, NaturalNumber/nil }
            function Save(self, target, hash, maxsize)
                local writer

                if type(target) == "string" then
                    local realpath  = GetPhysicalPath(target) or target
                    writer          = FileWriter(realpath, "wb")
                else
                    writer          = target
                end
                local finished      = false
                local failmsg

                with(writer)(function()
                    local size      = 0
                    local form      = self[0]
                    maxsize         = maxsize or self[-1] or 1024^3

                    if hash == FileHash.NONE then
                        while true do
                            local typ, res, err = form:read()
                            if not typ or typ == "eof" then break end

                            if typ == "body" then
                                size = size + #res
                                if size > maxsize then
                                    failmsg     = "The uploaded file is too bigger to be saved"
                                    return
                                end
                                writer:Write(res)
                            elseif typ == "part_end" then
                                self[2] = size
                                finished = true
                                break
                            else
                                -- skip
                            end
                        end
                    else
                        local handler   = FileHashHandler[hash]:new()
                        if not handler then
                            failmsg     = "Failed to create " .. FileHash(hash) .. " object"
                            return
                        end

                        while true do
                            local typ, res, err = form:read()
                            if not typ or typ == "eof" then break end

                            if typ == "body" then
                                size = size + #res
                                if size > maxsize then
                                    failmsg     = "The uploaded file is too bigger to be saved"
                                    return
                                end
                                writer:Write(res)
                                handler:update(res)
                            elseif typ == "part_end" then
                                self[2] = size
                                self[3] = strlib.to_hex(handler:final())
                                finished = true
                                handler:reset()
                                break
                            else
                                -- skip
                            end
                        end
                    end
                end)

                return finished, failmsg
            end

            function Skip(self)
                local form          = self[0]
                local finished      = false

                while true do
                    local typ, res, err = form:read()
                    if not typ or typ == "eof" then return end

                    if typ == "part_end" then
                        finished = true
                        break
                    else
                        -- skip
                    end
                end

                return finished
            end

            -----------------------------------------------------------
            --                      constructor                      --
            -----------------------------------------------------------
            function __new(_, file, form, maxsize)
                return { [-1] = maxsize, [0] = form, Path.GetFileName(file), false, false }, false
            end
        end)

        -----------------------------------------------------------
        --                       method                        --
        -----------------------------------------------------------
        __Iterator__()
        function GetIterator(self)
            local form, err     = RestyUpload:new(self.ChunkSize)
            if not form then return end

            form:set_timeout(self.TimeOut)

            local context       = GetContextFromStack(2)
            local maxsize       = context.Application[MAX_SIZE] or HttpFiles.MaxSize

            local name, file, temp
            while true do
                local typ, res, err = form:read()
                if not typ or typ == "eof" then break end

                if typ == "header" then
                    if res[1] == "Content-Disposition" then
                        temp    = nil
                        name    = res[2]:match("%Wname%s*=%s*(%b\"\")")
                        file    = res[2]:match("filename%s*=%s*(%b\"\")")

                        name    = name and name:sub(2, -2)
                        file    = file and file:sub(2, -2)

                        if name and file and name ~= "" and file ~= "" then
                            yield(name, HttpFile(file, form, maxsize))
                        end
                    end
                elseif typ == "body" then
                    if name and (not file or file == "") then
                        if not temp then
                            temp = res
                        elseif type(temp) == "string" then
                            temp = { temp, res }
                        else
                            temp[#temp + 1] = res
                        end
                    end
                elseif typ == "part_end" then
                    if name and temp and (not file or file == "") then
                        if type(temp) == "table" then
                            temp = tblconat(temp, "")
                        end

                        context.Request.Form[name] = temp
                    end
                    name        = nil
                    file        = nil
                    temp        = nil
                end
            end
        end

        -----------------------------------------------------------
        --                   static property                     --
        -----------------------------------------------------------
        --- the max file size
        __Static__() property "MaxSize"      { type = NaturalNumber }

        --- the max file size for application
        __Indexer__() __Static__()
        property "AppMaxSize"   {
            type                = NaturalNumber,
            get                 = function(self, app) return app[MAX_SIZE] end,
            set                 = function(self, app, val) app[MAX_SIZE]   = val end,
        }

        -----------------------------------------------------------
        --                       property                        --
        -----------------------------------------------------------
        --- the chunk size
        property "ChunkSize"    { default = 4096, type = NaturalNumber }

        --- the timeout of the cosocket in milliseconds
        property "TimeOut"      { default = 1000, type = NaturalNumber }

        -----------------------------------------------------------
        --                      constructor                      --
        -----------------------------------------------------------
        __Arguments__{ NaturalNumber/nil, NaturalNumber/nil }
        function __ctor(self, chunksize, timeout)
            self.ChunkSize      = chunksize
            self.TimeOut        = timeout
        end
    end)

    -----------------------------------------------------------
    --                     Configuration                     --
    -----------------------------------------------------------
    import "System.Configuration"

    __ConfigSection__(Web.ConfigSection.File, {
        MaxSize                 = Number,
    })
    function setWebConfig(config)
        HttpFiles.MaxSize       = config.MaxSize
    end

    __ConfigSection__(Application.ConfigSection.File, {
        MaxSize                 = Number,
    })
    function setAppConfig(config, app)
        HttpFiles.AppMaxSize[app] = config.MaxSize
    end
end)