--===========================================================================--
--                                                                           --
--                           NgxLua.MySQLProvider                            --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2018/06/07                                               --
-- Update Date  :   2020/07/24                                               --
-- Version      :   1.2.3                                                    --
--===========================================================================--
PLoop(function(_ENV)
    namespace "NgxLua.MySQL"

    import "System.Data"

    class "MySQLConnection" {}

    export { List, DBNull, "type", "tostring", "select", "error", "ipairs", quote_sql_str = ngx.quote_sql_str, parseindex = Toolset.parseindex, parseValue = System.Data.ParseValue }

    System.Web.SetValueString(ngx.null, "")
    System.Data.AddNullValue(ngx.null)

    function escape(val)
        if parseValue(val) == nil then return "NULL" end

        local vtype = type(val)

        if vtype == "boolean" then
            return val and "1" or "0"
        elseif vtype == "string" then
            return quote_sql_str(val)
        elseif vtype == "table" then
            local tmp       = List()
            for i, v in ipairs(val) do
                v           = escape(v)
                if v then
                    tmp:Insert(v)
                end
            end
            return tmp:Join(", ")
        else
            return tostring(val)
        end
    end

    function parseSql(sql, ...)
        if select("#", ...) == 0 then return sql end

        local index         = 1
        local fail
        local args          = { ... }

        sql                 = sql:gsub("%%[%+%-%d%.]*%w", function(word)
            local val       = args[index]
            index           = index + 1

            if val == nil then fail = fail or (index - 1) return end

            return escape(val)
        end)

        if fail then error("the sql's " .. parseindex(fail) .. " parameter can't be nil", 3) end

        return sql
    end

    __Sealed__() struct "ConnectionOption" (function(_ENV)
        --- the host name for the MySQL server
        member "host"           { type = String, default = "127.0.0.1" }

        --- the port that the MySQL server is listening on
        member "port"           { type = Integer, default = 3306 }

        --- the path of the unix socket file listened by the MySQL server
        member "path"           { type = String }

        --- the MySQL database name
        member "database"       { type = String }

        --- the MySQL account name for login
        member "user"           { type = String }

        --- MySQL account password for login
        member "password"       { type = String }

        --- the character set used on the MySQL connection
        member "charset"        { type = String , default = "utf8mb4" }

        --- the upper limit for the reply packets sent from the MySQL server
        member "max_packet_size"{ type = Integer }

        --- whether use the SSL to connect to the MySQL server
        member "ssl"            { type = Boolean }

        --- whether verifies the validity of the server SSL certificate
        member "ssl_verify"     { type = Boolean }

        --- the name for the MySQL connection pool
        member "pool"           { type = String }
    end)

    __Sealed__() class "MySQLBuilder" (function(_ENV)
        extend "ISqlBuilder"

        export {
            SQLTYPE_SELECT      = 1,
            SQLTYPE_UPDATE      = 2,
            SQLTYPE_DELETE      = 3,
            SQLTYPE_INSERT      = 4,

            FIELD_SQLTYPE       = 1,
            FIELD_SELECT        = 2,
            FIELD_UPDATE        = 3,
            FIELD_INSERT        = 4,
            FIELD_FROM          = 5,
            FIELD_WHERE         = 6,
            FIELD_ORDERBY       = 7,
            FIELD_LOCK          = 8,
            FIELD_LIMIT         = 9,
            FIELD_OFFSET        = 10,
            FIELD_ALLROW        = 11,

            escape              = escape,
            parseSql            = parseSql,
            type                = type,
            tblconcat           = table.concat,
            tostring            = tostring,
            pairs               = pairs,
            select              = select,
        }

        -----------------------------------------------------------
        --                        method                         --
        -----------------------------------------------------------
        function Select(self, fields)
            self[FIELD_SQLTYPE] = SQLTYPE_SELECT

            if type(fields) == "table" then
                fields          = tblconcat(fields, ",")
            else
                fields          = type(fields) == "string" and fields or nil
            end

            self[FIELD_SELECT]  = fields and fields ~= "" and fields or nil

            return self
        end

        function Count(self)
            self[FIELD_SQLTYPE] = SQLTYPE_SELECT
            self[FIELD_SELECT]  = "COUNT(*)"
            return self
        end

        function Lock(self)
            self[FIELD_LOCK]    = true

            return self
        end

        function Insert(self, map)
            if type(map) == "table" then
                self[FIELD_SQLTYPE] = SQLTYPE_INSERT

                local fields    = {}
                local values    = {}
                local index     = 1

                for fld, val in pairs(map) do
                    fields[index] = fld
                    values[index] = escape(val)
                    index       = index + 1
                end

                fields          = tblconcat(fields, ",")
                values          = tblconcat(values, ",")

                if fields ~= "" then
                    self[FIELD_SELECT] = fields
                    self[FIELD_INSERT] = values
                end
            end

            return self
        end

        function Update(self, map)
            self[FIELD_SQLTYPE] = SQLTYPE_UPDATE

            if type(map) == "table" then
                local temp      = {}
                local index     = 1
                local first     = true

                for fld, val in pairs(map) do
                    if not first then
                        temp[index] = ","
                        index   = index + 1
                    end
                    first       = false

                    temp[index] = fld
                    index       = index + 1

                    temp[index] = "="
                    index       = index + 1

                    temp[index] = escape(val)
                    index       = index + 1
                end

                map             = tblconcat(temp, "")
            else
                map             = tostring(map)
            end

            self[FIELD_UPDATE]  = map ~= "" and map or nil

            return self
        end

        function UpdateAll(self, map)
            self:Update(map)
            self[FIELD_ALLROW]  = true
            return self
        end

        function Delete(self)
            self[FIELD_SQLTYPE] = SQLTYPE_DELETE
            return self
        end

        function DeleteAll(self)
            self:Delete()
            self[FIELD_ALLROW]  = true
            return self
        end

        function From(self, name)
            self[FIELD_FROM]    = name
            return self
        end

        function Where(self, condition, ...)
            local ty            = type(condition)

            if ty == "table" then
                local temp      = {}
                local index     = 1
                local first     = true

                for fld, val in pairs(condition) do
                    if not first then
                        temp[index] = " AND "
                        index   = index + 1
                    end
                    first       = false

                    temp[index] = fld
                    index       = index + 1

                    temp[index] = "="
                    index       = index + 1

                    temp[index] = escape(val)
                    index       = index + 1
                end

                condition       = tblconcat(temp, "")
            elseif ty == "string" then
                condition       = parseSql(condition, ...)
            else
                error("Usage: MySQLBuilder:Where(condition[, ...]) - the condition can only be table or string", 2)
            end

            self[FIELD_WHERE]   = condition ~= "" and condition or nil

            return self
        end

        function OrderBy(self, field, desc)
            if desc then field  = field .. " DESC" end

            if self[FIELD_ORDERBY] then
                self[FIELD_ORDERBY] = self[FIELD_ORDERBY] .. "," .. field
            else
                self[FIELD_ORDERBY] = field
            end

            return self
        end

        function Limit(self, limit)
            self[FIELD_LIMIT]   = limit
            return self
        end

        function Offset(self, offset)
            self[FIELD_OFFSET]  = offset
            return self
        end

        function ToSql(self)
            local temp          = {}

            local sqltype       = self[FIELD_SQLTYPE] or SQLTYPE_SELECT
            local index         = 1

            if not sqltype then return end

            if sqltype == SQLTYPE_SELECT then
                temp[index]     = "SELECT";                     index = index + 1
                temp[index]     = self[FIELD_SELECT] or "*";    index = index + 1
                temp[index]     = "FROM";                       index = index + 1

                if not self[FIELD_FROM] then return end
                temp[index]     = self[FIELD_FROM];             index = index + 1

                if self[FIELD_WHERE] then
                    temp[index] = "WHERE";                      index = index + 1
                    temp[index] = self[FIELD_WHERE];            index = index + 1
                end

                if self[FIELD_ORDERBY] then
                    temp[index] = "ORDER BY";                   index = index + 1
                    temp[index] = self[FIELD_ORDERBY];          index = index + 1
                end

                if self[FIELD_LIMIT] then
                    temp[index] = "LIMIT";                      index = index + 1
                    temp[index] = tostring(self[FIELD_LIMIT]);  index = index + 1
                end

                if self[FIELD_OFFSET] then
                    temp[index] = "OFFSET";                     index = index + 1
                    temp[index] = tostring(self[FIELD_OFFSET]); index = index + 1
                end

                if self[FIELD_LOCK] then
                    temp[index] = "FOR UPDATE";                 index = index + 1
                end
            elseif sqltype == SQLTYPE_UPDATE then
                temp[index]     = "UPDATE";                     index = index + 1

                if not self[FIELD_FROM] then return end
                temp[index]     = self[FIELD_FROM];             index = index + 1
                temp[index]     = "SET";                        index = index + 1

                if not self[FIELD_UPDATE] then return end
                temp[index]     = self[FIELD_UPDATE];           index = index + 1

                if self[FIELD_WHERE] then
                    temp[index] = "WHERE";                      index = index + 1
                    temp[index] = self[FIELD_WHERE];            index = index + 1
                elseif not self[FIELD_ALLROW] then
                    error("Usage: MySQLBuilder should use UpdateAll to delete all rows, otherwise need set the Where condition")
                end
            elseif sqltype == SQLTYPE_DELETE then
                temp[index]     = "DELETE FROM";                index = index + 1

                if not self[FIELD_FROM] then return end
                temp[index]     = self[FIELD_FROM];             index = index + 1

                if self[FIELD_WHERE] then
                    temp[index] = "WHERE";                      index = index + 1
                    temp[index] = self[FIELD_WHERE];            index = index + 1
                elseif not self[FIELD_ALLROW] then
                    error("Usage: MySQLBuilder should use DeleteAll to delete all rows, otherwise need set the Where condition")
                end
            elseif sqltype == SQLTYPE_INSERT then
                temp[index]     = "INSERT INTO";                index = index + 1

                if not self[FIELD_FROM] then return end
                temp[index]     = self[FIELD_FROM];             index = index + 1

                if not self[FIELD_SELECT] then return end

                temp[index]     = "(";                          index = index + 1
                temp[index]     = self[FIELD_SELECT];           index = index + 1
                temp[index]     = ") VALUES (";                 index = index + 1

                if not self[FIELD_INSERT] then return end

                temp[index]     = self[FIELD_INSERT];           index = index + 1
                temp[index]     = ")";                          index = index + 1
            end

            return tblconcat(temp, " ")
        end

        -----------------------------------------------------------
        --                      constructor                      --
        -----------------------------------------------------------
        function __new(self)
            return {
                [FIELD_SQLTYPE] = false,
                [FIELD_SELECT]  = false,
                [FIELD_UPDATE]  = false,
                [FIELD_INSERT]  = false,
                [FIELD_FROM]    = false,
                [FIELD_WHERE]   = false,
                [FIELD_ORDERBY] = false,
                [FIELD_LOCK]    = false,
                [FIELD_LIMIT]   = false,
                [FIELD_OFFSET]  = false,
            }, true
        end
    end)

    __Sealed__() class "MySQLTransaction" (function(_ENV)
        extend "IDbTransaction"

        local ISOLATION_QUERY = {
            [TransactionIsolation.REPEATABLE_READ]  = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            [TransactionIsolation.READ_UNCOMMITTED] = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            [TransactionIsolation.READ_COMMITTED]   = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            [TransactionIsolation.SERIALIZABLE]     = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        }

        -----------------------------------------------------------
        --                        method                         --
        -----------------------------------------------------------
        --- Begin the transaction
        function Begin(self)
            self.Connection:Execute(ISOLATION_QUERY[self.Isolation])
            self.Connection:Execute("BEGIN")
        end

        --- Commits the database transaction
        function Commit(self)
            self.Connection:Execute("COMMIT")
        end

        --- Rolls back a transaction from a pending state
        function Rollback(self)
            self.Connection:Execute("ROLLBACK")
        end

        -----------------------------------------------------------
        --                      constructor                      --
        -----------------------------------------------------------
        __Arguments__{ MySQLConnection, TransactionIsolation/TransactionIsolation.REPEATABLE_READ }
        function __ctor(self, conn, isolation)
            self.Connection = conn
            self.Isolation  = isolation
        end
    end)

    __Sealed__() class "MySQLConnection" (function(_ENV)
        extend "IDbConnection"

        local mysql = require "resty.mysql"

        export {
            State_Closed        = ConnectionState.Closed,
            State_Open          = ConnectionState.Open,
            State_Connecting    = ConnectionState.Connecting,
            State_Executing     = ConnectionState.Executing,
            State_Fetching      = ConnectionState.Fetching,

            Trace               = System.Logger.Default[System.Logger.LogLevel.Trace],

            parseindex          = Toolset.parseindex,
            parseSql            = parseSql,
            error               = error,
            pairs               = pairs,
            type                = type,
            tonumber            = tonumber,

            MySQLTransaction,
        }

        -----------------------------------------------------------
        --                       property                        --
        -----------------------------------------------------------
        --- The query builder class
        property "SqlBuilder"   { set = false, default = MySQLBuilder }

        --- The option of the connection
        property "Option"       { type = ConnectionOption, field = 1 }

        --- Keep the connection alive after close it
        property "KeepAlive"    { type = Boolean, default = true }

        --- The max idle time to keep the connection alive(ms)
        property "MaxIdleTime"  { type = Integer, default = 10000 }

        --- The connection pool size
        property "PoolSize"     { type = Integer, default = 50 }

        --- The timeout protection for operations(ms)
        property "TimeOut"      { type = NaturalNumber, default = 1000, handler = function(self, val) self[0]:set_timeout(val or 1000) end }

        -----------------------------------------------------------
        --                        method                         --
        -----------------------------------------------------------
        --- Begins a database transaction.
        __Arguments__{ TransactionIsolation/TransactionIsolation.REPEATABLE_READ }
        function NewTransaction(self, isolation)
            return MySQLTransaction(self, isolation)
        end

        --- Closes the connection to the database.
        function Close(self, errmsg)
            if self.State == State_Closed then return end

            if errmsg == nil and self.KeepAlive then
                local ok, err = self[0]:set_keepalive(self.MaxIdleTime, self.PoolSize)
                if not ok then error("Usage: MySQLConnection:Close() - " .. (err or "failed"), 2) end
            else
                local ok, err = self[0]:close()
                if not ok then error("Usage: MySQLConnection:Close() - " .. (err or "failed"), 2) end
            end

            Trace("[Database][CLOSE]")

            self.State = State_Closed
        end

        --- Opens a database connection with the settings specified by the ConnectionString property of the provider-specific Connection object.
        function Open(self)
            if not self.Option then error("Usage: MySQLConnection:Open() - The MySQLConnection object have no connect option settings.", 2) end

            if self.State ~= State_Closed then return end

            local ok, err, errcode, sqlstate = self[0]:connect(self.Option)

            if not ok then
                error("Usage: MySQLConnection:Open() - connect failed:" .. (err or "unknown") .. ":" .. (errcode or -1) .. " " .. (sqlstate or ""), 2)
            end

            Trace("[Database][OPEN]")

            self.State = State_Open

            self[0]:set_timeout(self.TimeOut)
        end

        --- Sends the query sql to the remote MySQL server
        function Query(self, sql, ...)
            sql                 = parseSql(sql, ...)

            if self.State == State_Closed then
                error("Usage: MySQLConnection:Query(sql) - not connected", 2)
            elseif self.State ~= State_Open then
                error("Usage: MySQLConnection:Query(sql) - an operation is still processing", 2)
            end

            self.State          = State_Executing

            Trace("[SQL][Query]%q", sql)

            local bytes, err    = self[0]:send_query(sql)
            if not bytes then
                self.State      = State_Open
                error("Usage: MySQLConnection:Query(sql) - " .. (err or "failed"), 2)
            end

            self.State          = State_Fetching

            local res, err, errcode, sqlstate = self[0]:read_result()
            if not res then
                self.State      = State_Open
                error("Usage: MySQLConnection:Query(sql) - query failed:" .. (err or "unknown") .. ":" .. (errcode or -1) .. " " .. (sqlstate or ""), 2)
            end

            if err == "again" then
                local i, nres   = 2
                res             = { res }

                -- Multi-query
                while err == "again" do
                    nres, err, errcode, sqlstate = db:read_result()
                    if not nres then
                        self.State  = State_Open
                        error("Usage: MySQLConnection:Query(sql) - query failed:" .. (err or "unknown") .. ":" .. (errcode or -1) .. " " .. (sqlstate or ""), 2)
                    end

                    res[i], i   = nres, i + 1
                end
            end

            self.State          = State_Open

            return res
        end

        --- Sends the query sql and return the row count
        function Count(self, sql, ...)
            local res           = self:Query(sql, ...)

            if res then
                res             = res[1]

                if type(res) == "table" then
                    for k, v in pairs(res) do
                        return tonumber(v) or 0
                    end
                end
            end

            return 0
        end

        --- Sends the insert sql to the remote MySQL server
        function Insert(self, sql, ...)
            sql = parseSql(sql, ...)

            if self.State == State_Closed then
                error("Usage: MySQLConnection:Insert(sql) - not connected", 2)
            elseif self.State ~= State_Open then
                error("Usage: MySQLConnection:Insert(sql) - an operation is still processing", 2)
            end

            self.State = State_Executing

            Trace("[SQL][Insert]%q", sql)

            local bytes, err    = self[0]:send_query(sql)
            if not bytes then
                self.State      = State_Open
                error("Usage: MySQLConnection:Insert(sql) - " .. (err or "failed"), 2)
            end

            self.State = State_Fetching

            local res, err, errcode, sqlstate = self[0]:read_result()
            if not res then
                self.State      = State_Open
                error("Usage: MySQLConnection:Insert(sql) - insert failed:" .. (err or "unknown") .. ":" .. (errcode or -1) .. " " .. (sqlstate or ""), 2)
            end

            self.State = State_Open

            return res.insert_id
        end

        --- Sends the update sql to the remote MySQL server
        function Update(self, sql, ...)
            sql = parseSql(sql, ...)

            if self.State == State_Closed then
                error("Usage: MySQLConnection:Update(sql) - not connected", 2)
            elseif self.State ~= State_Open then
                error("Usage: MySQLConnection:Update(sql) - an operation is still processing", 2)
            end

            self.State = State_Executing

            Trace("[SQL][Update]%q", sql)

            local bytes, err    = self[0]:send_query(sql)
            if not bytes then
                self.State      = State_Open
                error("Usage: MySQLConnection:Update(sql) - " .. (err or "failed"), 2)
            end

            self.State = State_Fetching

            local res, err, errcode, sqlstate = self[0]:read_result()
            if not res then
                self.State      = State_Open
                error("Usage: MySQLConnection:Update(sql) - update failed:" .. (err or "unknown") .. ":" .. (errcode or -1) .. " " .. (sqlstate or ""), 2)
            end

            self.State = State_Open

            return res.affected_rows
        end

        function Delete(self, sql, ...)
            sql = parseSql(sql, ...)

            if self.State == State_Closed then
                error("Usage: MySQLConnection:Delete(sql) - not connected", 2)
            elseif self.State ~= State_Open then
                error("Usage: MySQLConnection:Delete(sql) - an operation is still processing", 2)
            end

            self.State = State_Executing

            Trace("[SQL][Delete]%q", sql)

            local bytes, err    = self[0]:send_query(sql)
            if not bytes then
                self.State      = State_Open
                error("Usage: MySQLConnection:Delete(sql) - " .. (err or "failed"), 2)
            end

            self.State = State_Fetching

            local res, err, errcode, sqlstate = self[0]:read_result()
            if not res then
                self.State      = State_Open
                error("Usage: MySQLConnection:Delete(sql) - delete failed:" .. (err or "unknown") .. ":" .. (errcode or -1) .. " " .. (sqlstate or ""), 2)
            end

            self.State = State_Open

            return res.affected_rows
        end

        function Execute(self, sql, ...)
            sql = parseSql(sql, ...)

            if self.State == State_Closed then
                error("Usage: MySQLConnection:Execute(sql) - not connected", 2)
            elseif self.State ~= State_Open then
                error("Usage: MySQLConnection:Execute(sql) - an operation is still processing", 2)
            end

            self.State = State_Executing

            Trace("[SQL][Execute]%q", sql)

            local bytes, err    = self[0]:send_query(sql)
            if not bytes then
                self.State      = State_Open
                error("Usage: MySQLConnection:Execute(sql) - " .. (err or "failed"), 2)
            end

            self.State = State_Fetching

            local res, err, errcode, sqlstate = self[0]:read_result()
            if not res then
                self.State      = State_Open
                error("Usage: MySQLConnection:Execute(sql) - execute failed:" .. (err or "unknown") .. ":" .. (errcode or -1) .. " " .. (sqlstate or ""), 2)
            end

            if err == "again" then
                local i, nres   = 2
                res             = { res }

                -- Multi-query
                while err == "again" do
                    nres, err, errcode, sqlstate = self[0]:read_result()
                    if not nres then
                        self.State  = State_Open
                        error("Usage: MySQLConnection:Query(sql) - query failed:" .. (err or "unknown") .. ":" .. (errcode or -1) .. " " .. (sqlstate or ""), 2)
                    end

                    res[i], i   = nres, i + 1
                end
            end

            self.State = State_Open

            return res
        end

        -----------------------------------------------------------
        --                      constructor                      --
        -----------------------------------------------------------
        __Arguments__{ ConnectionOption }
        function __new(self, opt)
            local db, err = mysql:new()

            if not db then throw(err) end

            return { [0] = db, [1] = opt }, true
        end
    end)

    -----------------------------------------------------------
    --                  DataBase Operations                  --
    -----------------------------------------------------------
    class "MySQLConnection" (function(_ENV)
        export {
            List, Enum, Class, Struct, Attribute, Property, Namespace,
            __DataField__, __DataTable__, IDataContext, IDataEntity,
            Number, String, Date, NaturalNumber, Boolean, Integer,

            pairs               = pairs,
            ipairs              = ipairs,
            max                 = math.max,
            min                 = math.min,
            tinsert             = table.insert,
            tsort               = table.sort,
            tconcat             = table.concat,
            Debug               = Logger.Default[Logger.LogLevel.Debug],
        }

        local function parseEntityCls(cache, entityCls)
            if cache[entityCls] then return cache[entityCls] end

            local fields            = {}

            for name, ftr in Class.GetFeatures(entityCls) do
                if Property.Validate(ftr) and not Property.IsStatic(ftr) then
                    local dfield    = Attribute.GetAttachedData(__DataField__, ftr, entityCls)
                    if dfield then
                        local ptype = ftr:GetType()
                        if dfield.foreign then
                            for k, v in pairs(dfield.foreign.map) do
                                local org = fields[k]
                                fields[k] = { name = k, class = ptype, field = v, notnull = dfield.notnull, unique = dfield.unique, fieldindex = dfield.fieldindex }
                                if org then fields[k].fieldindex = org.fieldindex end
                            end
                        else
                            local dtype = dfield.type

                            if not dtype then
                                if Struct.IsSubType(ptype, Number) then
                                    if dfield.autoincr or Struct.IsSubType(ptype, NaturalNumber) then
                                        dtype = "INT UNSIGNED"
                                    elseif Struct.IsSubType(ptype, Integer) then
                                        dtype = "INT"
                                    else
                                        dtype = "FLOAT"
                                    end
                                elseif Struct.IsSubType(ptype, String) then
                                    dtype = "VARCHAR(128)"
                                elseif ptype == Date then
                                    dtype = "DATETIME"
                                elseif Struct.IsSubType(ptype, Boolean) then
                                    dtype = "TINYINT"
                                elseif Enum.Validate(ptype) then
                                    local maxnumber, minnumber = 0, 0
                                    local maxlength
                                    for k, v in Enum.GetEnumValues(ptype) do
                                        if type(v) == "number" then
                                            maxnumber = max(maxnumber, v)
                                            minnumber = min(minnumber, v)
                                        else
                                            maxlength = #v
                                        end
                                    end
                                    if maxlength then
                                        if maxlength <= 255 then
                                            dtype = "CHAR(" .. maxlength .. ")"
                                        else
                                            dtype = "VARCHAR(" .. maxlength .. ")"
                                        end
                                    else
                                        if minnumber == 0 then
                                            if maxnumber <= 255 then
                                                dtype = "TINYINT UNSIGNED"
                                            elseif maxnumber <= 65535 then
                                                dtype = "SMALLINT UNSIGNED"
                                            elseif maxnumber <= 16777215 then
                                                dtype = "MEDIUMINT UNSIGNED"
                                            elseif maxnumber <= 4294967295 then
                                                dtype = "INT UNSIGNED"
                                            else
                                                dtype = "BIGINT UNSIGNED"
                                            end
                                        else
                                            if minnumber >= -128 and maxnumber <= 127 then
                                                dtype = "TINYINT"
                                            elseif minnumber >= -32768 and maxnumber <= 32767 then
                                                dtype = "SMALLINT"
                                            elseif minnumber >= -8388608 and maxnumber <= 8388607 then
                                                dtype = "MEDIUMINT"
                                            elseif minnumber >= -2147483648 and maxnumber <= 2147483647 then
                                                dtype = "INT"
                                            else
                                                dtype = "BIGINT"
                                            end
                                        end
                                    end
                                end
                            end

                            if dtype then
                                if fields[name] then
                                    if fields[name].fieldindex > dfield.fieldindex then
                                        fields[name].fieldindex = dfield.fieldindex
                                    end
                                else
                                    fields[name] = { name = name, type = dtype, autoincr = dfield.autoincr, notnull = dfield.notnull, unique = dfield.unique, fieldindex = dfield.fieldindex }
                                end
                            end
                        end
                    end
                end
            end

            local temp              = {}

            for name, field in pairs(fields) do
                tinsert(temp, field)
            end

            tsort(temp, function(a, b) return a.fieldindex < b.fieldindex end)

            for name, field in pairs(fields) do
                temp[name]          = field
            end

            cache[entityCls]        = temp
        end

        -----------------------------------------------------------
        --                    DataBase Method                    --
        -----------------------------------------------------------
        --- Drop all data tables in the database
        function DropAllTables(self)
            local sql           = ("SELECT table_name AS name FROM information_schema.`TABLES` WHERE table_schema='%s'"):format(self.Option.database)

            for _, table in List(self:Query(sql)):GetIterator() do
                self:Execute("DROP TABLE " .. table.name .. ";")
            end
        end

        --- Scan the data context in a namespace, and create all non-existed
        -- data tables in the namespace, child namespace won't be checked
        __Arguments__{ NamespaceType }
        function CreateNonExistTables(self, ns)
            local cache         = {}

            if Class.Validate(ns) and Class.IsSubType(ns, IDataContext) then
                for name, entityCls in Namespace.GetNamespaces(ns) do
                    if Class.Validate(entityCls) and Class.IsSubType(entityCls, IDataEntity) then
                        parseEntityCls(cache, entityCls)
                    end
                end
            else
                for name, contextCls in Namespace.GetNamespaces(ns) do
                    if Class.Validate(contextCls) and Class.IsSubType(contextCls, IDataContext) then
                        for name, entityCls in Namespace.GetNamespaces(contextCls) do
                            if Class.Validate(entityCls) and Class.IsSubType(entityCls, IDataEntity) then
                                parseEntityCls(cache, entityCls)
                            end
                        end
                    end
                end
            end

            for entityCls in pairs(cache) do
                local set       = Attribute.GetAttachedData(__DataTable__, entityCls)
                local info      = cache[entityCls]
                local sql       = {}

                tinsert(sql, "CREATE TABLE IF NOT EXISTS `" .. set.name .. "`(")

                for _, fset in ipairs(info) do
                    fset.type   = fset.type or cache[fset.class][fset.field].type
                    tinsert(sql, ("`%s` %s %s %s,"):format(
                            fset.name,
                            fset.type,
                            fset.autoincr and "AUTO_INCREMENT" or fset.notnull and "NOT NULL" or "",
                            fset.unique and "UNIQUE" or ""
                        )
                    )
                end

                if set.indexes then
                    local needsep   = false
                    for _, index in ipairs(set.indexes) do
                        if needsep then tinsert(sql, ", ") end
                        local temp  = {}

                        for _, fld in ipairs(index.fields) do
                            tinsert(temp, "`" .. fld .. "`")
                        end

                        tinsert(sql, ("%s %s(%s)"):format(
                            index.primary and "PRIMARY KEY" or index.unique and "UNIQUE" or index.fulltext and "FULLTEXT" or "INDEX",
                            index.primary and "" or index.name or ("IDX_" .. tconcat(index.fields, "_")),
                            tconcat(temp, ",")
                        ))

                        needsep = true
                    end
                end

                tinsert(sql, (")ENGINE=%s;"):format(set.engine or "InnoDB"))
                self:Execute(tconcat(sql, ""))
            end
        end

        --- Create non-existed data tables for the data collection, useful for dynamic tables
        __Arguments__{ DataCollection }
        function CreateNonExistTables(self, col)
            local cache         = {}

            parseEntityCls(cache, col:GetDataEntity())

            for entityCls in pairs(cache) do
                local set       = Attribute.GetAttachedData(__DataTable__, entityCls)
                local info      = cache[entityCls]
                local sql       = {}

                tinsert(sql, "CREATE TABLE IF NOT EXISTS `" .. col:GetTableName() .. "`(")

                for _, fset in ipairs(info) do
                    fset.type   = fset.type or cache[fset.class][fset.field].type
                    tinsert(sql, ("`%s` %s %s %s,"):format(
                            fset.name,
                            fset.type,
                            fset.autoincr and "AUTO_INCREMENT" or fset.notnull and "NOT NULL" or "",
                            fset.unique and "UNIQUE" or ""
                        )
                    )
                end

                if set.indexes then
                    local needsep   = false
                    for _, index in ipairs(set.indexes) do
                        if needsep then tinsert(sql, ", ") end
                        local temp  = {}

                        for _, fld in ipairs(index.fields) do
                            tinsert(temp, "`" .. fld .. "`")
                        end

                        tinsert(sql, ("%s %s(%s)"):format(
                            index.primary and "PRIMARY KEY" or index.unique and "UNIQUE" or index.fulltext and "FULLTEXT" or "INDEX",
                            index.primary and "" or index.name or ("IDX_" .. tconcat(index.fields, "_")),
                            tconcat(temp, ",")
                        ))

                        needsep = true
                    end
                end

                tinsert(sql, (")ENGINE=%s;"):format(set.engine or "InnoDB"))
                self:Execute(tconcat(sql, ""))
            end
        end
    end)
end)