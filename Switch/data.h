// A simple mySQL client
#pragma once
#include "ext/tl/optional.hpp"
#include "switch.h"
#include <mysql/errmsg.h>
#include <mysql/mysql.h>
#include <mysql/mysqld_error.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>

#define SET_TIMEOUT_ONCE 1

struct SqlRow final
{
        MYSQL_ROW r;
        MYSQL_FIELD *fields;
        const unsigned long *rl;
        uint32_t n;

        explicit SqlRow(MYSQL_ROW row, const unsigned long *const lengths, const uint32_t cnt, MYSQL_FIELD *f)
            : r{row}, fields{f}, rl{lengths}, n{cnt}
        {
        }

        constexpr SqlRow(const SqlRow &o)
            : r(o.r), fields{o.fields}, rl{o.rl}, n{o.n}
        {
        }

        constexpr SqlRow(SqlRow &&o)
            : r(o.r), fields{o.fields}, rl{o.rl}, n{o.n}
        {
                o.r = nullptr;
                o.rl = nullptr;
                o.n = 0;
                o.fields = nullptr;
        }

        SqlRow()
            : r{nullptr}
        {
        }

        operator bool() const noexcept
        {
                return r;
        }

        inline strwlen32_t operator[](const uint32_t idx) const
        {
                if (unlikely(idx >= n))
                        throw Switch::exception("Attempted to acces SqlRow column ", idx, " past ", n);
                else
                        return strwlen32_t(r[idx], rl[idx]);
        }

        auto &operator=(const SqlRow &o)
        {
                r = o.r;
                rl = o.rl;
                n = o.n;
                fields = o.fields;

                return *this;
        }

        auto size() const noexcept
        {
                return n;
        }

        auto width() const noexcept
        {
                return n;
        }

        auto mysql_row() const noexcept
        {
                return r;
        }

        auto mysql_field_lengths() const noexcept
        {
                return rl;
        }
};

class SqlResult
{
      private:
        MYSQL_RES *res;

      public:
        struct iterator
        {
                SqlResult *const rset;
                SqlRow row;

                iterator(SqlResult *const rs, SqlRow const first)
                    : rset(rs), row(first)
                {
                }

                bool operator!=(const struct iterator &o) const noexcept
                {
                        return row.r != o.row.r;
                }

                inline iterator &operator++()
                {
                        row = rset->next();
                        return *this;
                }

                auto &operator*() noexcept
                {
                        return row;
                }
        };

      public:
        inline iterator begin()
        {
                return iterator(this, next());
        }

        inline iterator end()
        {
                return iterator(this, {});
        }

        SqlResult(MYSQL_RES *r)
            : res(r)
        {
        }

        SqlResult()
            : res{nullptr}
        {
        }

        SqlResult(SqlResult &&o)
            : res(o.res)
        {
                o.res = nullptr;
        }

        auto &operator=(SqlResult &&o)
        {
                if (res)
                        mysql_free_result(res);

                res = o.res;
                o.res = nullptr;
                return *this;
        }

        bool InMemory() const noexcept
        {
                // Will be nullptr if mysql_use_result() was used
                return res->data_cursor;
        }

        [[gnu::always_inline]] ~SqlResult()
        {
                if (likely(res))
                        mysql_free_result(res);
        }

        inline SqlRow next()
        {
                if (auto row = mysql_fetch_row(res))
                        return SqlRow(row, mysql_fetch_lengths(res), mysql_num_fields(res), mysql_fetch_fields(res));
                else
                        return SqlRow(nullptr, nullptr, 0, nullptr);
        }

        void drain()
        {
                if (InMemory())
                {
                        while (next())
                                continue;
                }
        }

        inline uint32_t size() const
        {
                return mysql_num_rows(res);
        }

        inline bool empty() const
        {
                return !size();
        }

        operator bool() const noexcept
        {
                return res;
        }
};

class MysqlClient final
{
      private:
        MYSQL *handle;
        uint64_t blacklistedUntilTs{0};
        uint64_t connectedSince{0};
        char hostName[128], curDb[128];
        char userName[128], password[128];
        char *socket;
        int port;
        bool inTransaction, pendingResults;

        struct
        {
                bool enabled{false};

                struct
                {
                        const char *key{nullptr};
                        const char *cert{nullptr};
                        const char *ca{nullptr};
                        const char *capath{nullptr};
                } paths;
                const char *cipher{nullptr};
        } ssl;

        /* Whenever an error occurs, we can either not continue ( say, because the statement is incorrect or the databases
		   structure will not permit it ) or  the connection was lost or the statement resulted in a deadlock. In the last case,
		   on failure we can check if 'transaction can be retried' and if so, try again. This variable serves as a flag.
		 */
        bool canRetryTransaction;
        uint64_t lastTransactionMutationStatemenTS{0};
        uint32_t nStatementsInTransaction;
        Buffer queryBuffer; /* VERY useful - avoids constant realloc(s)! */
#ifdef SWITCH_DATA_ENABLE_STMT_HISTORY
        Buffer prevQuery;
#endif
        static int __dummy;

      private:
        auto store_result_impl()
        {
                return mysql_store_result(handle);
        }

      public:
        static strwlen32_t cmdFromStatement(const strwlen32_t stmt);

        void enable_ssl(const char *key = nullptr, const char *cert = nullptr, const char *ca = nullptr, const char *capath = nullptr, const char *cipher = nullptr)
        {
                ssl.enabled = true;
                ssl.paths.key = key;
                ssl.paths.cert = cert;
                ssl.paths.ca = ca;
                ssl.paths.capath = capath;
                ssl.cipher = cipher;
        }

        inline operator MYSQL *()
        {
                return handle;
        }

        void considerError();

        bool logQueryFail(const strwlen32_t, const bool);

        bool isBlacklisted()
        {
                if (blacklistedUntilTs)
                {
                        if (blacklistedUntilTs > Timings::Milliseconds::Tick())
                                return true;

                        blacklistedUntilTs = 0;
                }
                return false;
        }

        static char *GetClientInfo()
        {
                return (char *)mysql_get_client_info();
        }

        char *GetServerInfo()
        {
                return connectedSince ? (char *)mysql_get_server_info(handle) : nullptr;
        }

        static auto GetClientVersion()
        {
                return mysql_get_client_version();
        }

        uint32_t GetServerVersion()
        {
                if (connectedSince)
                {
                        // mysql_get_server_version() is not supported by the current client library releasee

                        const char *p = mysql_get_server_info(handle);

                        if (p)
                        {
                                uint32_t major, minor, revision;

                                if (sscanf(p, "%u.%u.%u", &major, &minor, &revision) == 3)
                                        return major * 10000 + minor * 100 + revision;
                        }
                }

                return 0;
        }

        void GetClientVersion(uint32_t &major, uint32_t &minor, uint32_t &revision)
        {
                auto version = mysql_get_client_version();

                major = version / 10000;
                version -= (major * 10000);

                minor = version / 100;
                revision = version - (minor * 100);
        }

        void GetServerVersion(uint32_t &major, uint32_t &minor, uint32_t &revision)
        {
                if (unlikely(!connectedSince))
                {
                        major = minor = revision = 0;
                        return;
                }

                if (const char *const p = mysql_get_server_info(handle))
                {
                        if (sscanf(p, "%u.%u.%u", &major, &minor, &revision) == 3)
                                return;
                }

                major = minor = revision = 0;
        }

        static bool IsThreadSafe()
        {
                return mysql_thread_safe();
        }

        inline auto GetObjectSize()
        {
                return sizeof(*this) + queryBuffer.reserved();
        }

        MysqlClient()
        {
                hostName[0] = '\0';
                userName[0] = '\0';
                password[0] = '\0';
                curDb[0] = '\0';
                socket = nullptr;
                handle = nullptr;

                inTransaction = false;
                canRetryTransaction = false;
                nStatementsInTransaction = 0;
                pendingResults = false;
                port = 0;
        }

        MysqlClient(const char *const hostName, const char *const userName = nullptr, const char *const passwd = nullptr, const char *const curDb = nullptr, const uint16_t port = 0)
            : MysqlClient()
        {
                if (hostName)
                        Connect(hostName, userName, passwd, curDb, port);
        }

        ~MysqlClient()
        {
                if (socket)
                        std::free(socket);
                Shutdown();

                if (likely(handle))
                        mysql_close(handle);
        }

        void ShutdownIfDisconnected()
        {
                /* No need to keep the connection in CLOSE_WAIT state; if we are connected but in fact we are not, shutdown */

                if (connectedSince)
                {
                        if (unlikely(mysql_real_query(handle, "SELECT 1", 8)))
                                Shutdown();
                        else if (auto res = store_result_impl())
                                mysql_free_result(res);
                }
        }

        inline bool Reconnect()
        {
                return Connect(hostName, userName, password, curDb, port, socket);
        }

        inline char *GetHostname()
        {
                return hostName;
        }

        inline uint16_t GetPort()
        {
                return port;
        }

        void Shutdown()
        {
                if (connectedSince)
                {
                        // Keep the handly handy; useful for extracting errors
                        if (handle)
                        {
                                mysql_close(handle);
                                handle = nullptr;
                        }

                        connectedSince = 0;
                        inTransaction = false;
                        canRetryTransaction = false;
                        nStatementsInTransaction = 0;
                        pendingResults = false;
#ifdef SWITCH_DATA_ENABLE_STMT_HISTORY
                        prevQuery.Flush();
#endif
                }
        }

        inline bool IsConnected() const
        {
                return connectedSince;
        }

        // You can chain calls like so
        // MysqlClient().connect("legolas.lan").exec("INSERT INTO table").insert_id()
        auto &connect(const char *hostname = "", const char *username = "", const char *pass = "", const char *db = nullptr, int _port = 0, const char *_socket = nullptr, uint32_t flags = 0)
        {
                if (false == Connect(hostname, username, pass, db, _port, _socket, flags))
                        throw Switch::data_error("Connection failed");

                return *this;
        }

        void init_handle();

        bool Connect(const char *hostname = "", const char *username = "", const char *pass = "", const char *db = nullptr, int _port = 0, const char *_socket = nullptr, uint32_t flags = 0);

        bool SetCurrentDb(const char *db);

        auto &use_database(const char *const name)
        {
                if (!SetCurrentDb(name))
                        throw Switch::data_error("Failed to set current database");
                return *this;
        }

        [[gnu::always_inline]] bool InTransaction() const
        {
                return inTransaction;
        }

        bool in_transaction() const noexcept
        {
                return inTransaction;
        }

        bool Rollback()
        {
                if (inTransaction)
                {
                        inTransaction = false;
                        canRetryTransaction = false;
                        nStatementsInTransaction = 0;

                        if (unlikely(!exec_stmt(false, "ROLLBACK", 8)))
                                return false;

#if !defined(SET_TIMEOUT_ONCE)
                        exec_stmt(false, "SET SESSION wait_timeout = 8", 28);
#endif
                }

                return true;
        }

        bool Begin();

        auto &begin()
        {
                if (!Begin())
                        throw Switch::exception("Unable to execute mySQL query");
                return *this;
        }

        auto &commit()
        {
                if (!Commit())
                        throw Switch::exception("Unable to execute mySQL query");
                return *this;
        }

        auto &rollback()
        {
                if (!Rollback())
                        throw Switch::exception("Unable to execute mySQL query");
                return *this;
        }

        inline void EnsureInTransaction()
        {
                if (!inTransaction)
                        Begin();
        }

        auto &begin_if_not_in_transaction()
        {
                if (!inTransaction)
                        begin();
                return *this;
        }

        bool CommitIfInTransaction()
        {
                if (inTransaction)
                        return Commit();
                return true;
        }

        bool Commit()
        {
                if (inTransaction)
                {
                        if (unlikely(!exec_stmt(false, "COMMIT", 6)))
                                return false;

#if !defined(SET_TIMEOUT_ONCE)
                        exec_stmt(false, "SET SESSION wait_timeout = 8", 28);
#endif

                        inTransaction = false;
                        canRetryTransaction = false;
                        nStatementsInTransaction = 0;
                }

                return true;
        }

        bool pending_results() const noexcept
        {
                return pendingResults;
        }

        inline bool HavePendingResults() const
        {
                return pendingResults;
        }

        inline std::unique_ptr<SqlResult> Res()
        {
                return std::unique_ptr<SqlResult>(new SqlResult(MySQLResult()));
        }

        inline auto result()
        {
                return SqlResult(MySQLResult());
        }

        inline auto rows()
        {
                return SqlResult(MySQLResult());
        }

        inline MYSQL_RES *MySQLResult()
        {
                if (likely(connectedSince))
                {
                        auto r = store_result_impl();

                        pendingResults = false;
                        if (unlikely(r == nullptr))
                                return nullptr;

                        return r;
                }

                return nullptr;
        }

        inline bool Query(const bool bailoutIfError, Buffer &buf)
        {
                return exec_stmt(bailoutIfError, buf.data(), buf.size());
        }

        inline bool Query(const bool bailoutIfError, Buffer *buf)
        {
                return exec_stmt(bailoutIfError, buf->data(), buf->size());
        }

        inline bool exec_stmt(const bool bailoutIfError, const char *data)
        {
                return exec_stmt(bailoutIfError, data, strlen(data));
        }

        bool exec_stmt(const bool bailoutIfError, const char *data, const int len);

        bool exec_stmt(const bool bailoutIfError, Buffer &b)
        {
                return exec_stmt(bailoutIfError, b.data(), b.size());
        }

        bool exec_stmt(const bool bailoutIfError, Buffer *b)
        {
                return exec_stmt(bailoutIfError, b->data(), b->size());
        }

        template <typename... Args>
        bool Exec(const bool bailoutIfError, Args... args)
        {
                queryBuffer.clear();
                ToBuffer(queryBuffer, args...);
                return exec_stmt(bailoutIfError, queryBuffer.data(), queryBuffer.size());
        }

        template <typename... Args>
        auto select(Args &&... args)
        {
                queryBuffer.clear();
                ToBuffer(queryBuffer, std::forward<Args>(args)...);

                if (!exec_stmt(false, queryBuffer.data(), queryBuffer.size()))
                        throw Switch::exception("Unable to execute mySQL query");

                return SqlResult(MySQLResult());
        }

        template <typename... Arg>
        tl::optional<SqlResult> try_select(Arg &&... args)
        {
                queryBuffer.clear();
                ToBuffer(queryBuffer, std::forward<Arg>(args)...);

                if (!exec_stmt(false, queryBuffer.data(), queryBuffer.size()))
                        return tl::nullopt;

                return tl::make_optional<SqlResult>(MySQLResult());
        }

        template <typename... Args>
        auto &exec(Args &&... args)
        {
                queryBuffer.clear();
                ToBuffer(queryBuffer, std::forward<Args>(args)...);

                if (!exec_stmt(false, queryBuffer.data(), queryBuffer.size()))
                        throw Switch::exception("Unable to execute mySQL query");

                return *this;
        }

        template <typename... Args>
        auto &query(Args &&... args)
        {
                queryBuffer.clear();
                ToBuffer(queryBuffer, std::forward<Args>(args)...);

                if (!exec_stmt(false, queryBuffer.data(), queryBuffer.size()))
                        throw Switch::exception("Unable to execute mySQL query");

                return *this;
        }

        inline char *GetErrString()
        {
                return handle ? (char *)mysql_error(handle) : nullptr;
        }

        inline int GetErrNo()
        {
                return handle ? mysql_errno(handle) : 0;
        }

        bool CanRetryTransaction() const
        {
                return connectedSince && canRetryTransaction;
        }

        my_ulonglong insert_id() const
        {
                return connectedSince ? mysql_insert_id(handle) : 0;
        }

        inline bool IsConnectionConfigured() const
        {
                return *hostName || (socket && *socket);
        }

        int ping()
        {
                if (IsConnectionConfigured())
                {
                        if (mysql_ping(handle) == 0)
                                return 0;
                }

                return CR_UNKNOWN_ERROR;
        }

        void AssertReadyForOperations();

        inline MYSQL *GetHandle() const
        {
                return handle;
        }

        inline int GetRowsAffected() const
        {
                return mysql_affected_rows(handle);
        }

        auto affected_rows() const
        {
                return mysql_affected_rows(handle);
        }

	auto _GetQueryBuffer() 
	{
		return &queryBuffer;
        }

        auto internal_buffer()
        {
                return &queryBuffer;
        }
};
