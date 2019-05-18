--===========================================================================--
--                                                                           --
--                           NgxLua.MySQLProvider                            --
--                                                                           --
--===========================================================================--

--===========================================================================--
-- Author       :   kurapica125@outlook.com                                  --
-- URL          :   http://github.com/kurapica/PLoop                         --
-- Create Date  :   2018/06/07                                               --
-- Update Date  :   2019/05/17                                               --
-- Version      :   1.1.0                                                    --
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
                fields          = tostring(fields)
            end

            self[FIELD_SELECT]  = fields ~= "" and fields or nil

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

        function Delete(self)
            self[FIELD_SQLTYPE] = SQLTYPE_DELETE

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

        function ToSql(self)
            local temp          = {}

            local sqltype       = self[FIELD_SQLTYPE]

            if not sqltype then return end

            if sqltype == SQLTYPE_SELECT then
                temp[1]         = "SELECT"
                temp[2]         = self[FIELD_SELECT] or "*"
                temp[3]         = "FROM"

                if not self[FIELD_FROM] then return end
                temp[4]         = self[FIELD_FROM]

                if self[FIELD_WHERE] then
                    temp[5]     = "WHERE"
                    temp[6]     = self[FIELD_WHERE]
                else
                    temp[5]     = ""
                    temp[6]     = ""
                end

                if self[FIELD_ORDERBY] then
                    temp[7]     = "ORDER BY"
                    temp[8]     = self[FIELD_ORDERBY]
                else
                    temp[7]     = ""
                    temp[8]     = ""
                end

                if self[FIELD_LOCK] then
                    temp[9]     = "FOR UPDATE"
                end
            elseif sqltype == SQLTYPE_UPDATE then
                temp[1]         = "UPDATE"

                if not self[FIELD_FROM] then return end
                temp[2]         = self[FIELD_FROM]
                temp[3]         = "SET"

                if not self[FIELD_UPDATE] then return end
                temp[4]         = self[FIELD_UPDATE]

                if self[FIELD_WHERE] then
                    temp[5]     = "WHERE"
                    temp[6]     = self[FIELD_WHERE]
                else
                    return
                end
            elseif sqltype == SQLTYPE_DELETE then
                temp[1]         = "DELETE FROM"

                if not self[FIELD_FROM] then return end
                temp[2]         = self[FIELD_FROM]

                if self[FIELD_WHERE] then
                    temp[3]     = "WHERE"
                    temp[4]     = self[FIELD_WHERE]
                else
                    return
                end
            elseif sqltype == SQLTYPE_INSERT then
                temp[1]         = "INSERT INTO"

                if not self[FIELD_FROM] then return end
                temp[2]         = self[FIELD_FROM]

                if not self[FIELD_SELECT] then return end

                temp[3]         = "("
                temp[4]         = self[FIELD_SELECT]
                temp[5]         = ") VALUES ("

                if not self[FIELD_INSERT] then return end

                temp[6]         = self[FIELD_INSERT]
                temp[7]         = ")"
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
        function Close(self)
            if self.State == State_Closed then return end

            if self.KeepAlive then
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
            sql = parseSql(sql, ...)

            if self.State == State_Closed then
                error("Usage: MySQLConnection:Query(sql) - not connected", 2)
            elseif self.State ~= State_Open then
                error("Usage: MySQLConnection:Query(sql) - an operation is still processing", 2)
            end

            self.State = State_Executing

            Trace("[SQL][Query]%q", sql)

            local bytes, err    = self[0]:send_query(sql)
            if not bytes then
                self.State      = State_Open
                error("Usage: MySQLConnection:Query(sql) - " .. (err or "failed"), 2)
            end

            self.State = State_Fetching

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

            self.State = State_Open

            return res
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
end)