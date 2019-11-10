#include "data.h"
#include <signal.h>

struct tls_mysql
{
        tls_mysql()
        {
                signal(SIGPIPE, SIG_IGN); // Check : http://sunsite.mff.cuni.cz/MIRRORS/ftp.mysql.com/doc/en/Threaded_clients.html

                // This will be invoked once
                mysql_thread_init();
        }

        ~tls_mysql()
        {
                mysql_thread_end();
        }

        void notify()
        {
                //NO-OP, but we need it so that the runtime will instantiate and destroy the thread local storage state
        }
};

static thread_local tls_mysql _tls;

strwlen32_t MysqlClient::cmdFromStatement(const strwlen32_t stmt)
{
        const char *p = stmt.p, *const e = p + stmt.len;
        strwlen32_t cmd;

        while (p != e && isspace(*p))
                ++p;

        cmd.p = p;

        if (p != e && *p == '(')
        {
                // (SELECT ...) UNION
                for (++p; p != e && isspace(*p); ++p)
                        continue;
                cmd.p = p;
        }

        while (p != e && isalpha(*p))
                ++p;

        cmd.SetEnd(p);
        return cmd;
}

static void _LogError(Buffer &buf)
{
        Print("Failed:", buf.as_s32(), "\n");
}

template <typename... Args>
static void logError(Args &&... args)
{
        Buffer b;

        ToBuffer(b, std::forward<Args>(args)...);
        _LogError(b);
}

bool MysqlClient::logQueryFail(const strwlen32_t q, const bool bailoutIfError)
{
        if (inTransaction)
        {
                if (bailoutIfError)
                {
                        auto s{q};
                        strwlen32_t s2;

                        if (s.size() > 1024)
                        {
                                s.len = 1024;
                                s2.Set(_S("..."));
                        }

                        throw Switch::exception("Query failed while transaction was active");
                }
                else
                {
                        canRetryTransaction = true;

#ifdef SWITCH_DATA_ENABLE_STMT_HISTORY
                        prevQuery.clear();
                        prevQuery.Append(data, len);
#endif

                        logError("(warning) Query [", q, "] failed: ", mysql_error(handle), ". Aborted because transaction was active\n");
                        return false;
                }
        }
        else
        {
                if (handle)
                        logError(!bailoutIfError ? "(warning)" : "", "Query [", q, "] failed: ", mysql_error(handle), "\n");
                else
                        logError(!bailoutIfError ? "(warning)" : "", "Query [", q, "] failed: (no handle)\n");

                if (bailoutIfError)
                        throw Switch::exception("Failed to execute mySQL query");
                else
                        return false;
        }
}

void MysqlClient::considerError()
{
        switch (mysql_errno(handle))
        {
                case CR_SERVER_LOST:
                case CR_SERVER_LOST_EXTENDED:
                case CR_SERVER_GONE_ERROR:
                        // Connection _was_ established, but we failed there (maybe timed-out)
                        // let's not blacklist it just yet, let's try again
                        break;

                case CR_SOCKET_CREATE_ERROR:
                case CR_CONNECTION_ERROR:
                case CR_CONN_HOST_ERROR:
                case CR_TCP_CONNECTION:
                case CR_LOCALHOST_CONNECTION:
                case CR_SSL_CONNECTION_ERROR:
                case CR_MALFORMED_PACKET:
                        blacklistedUntilTs = Timings::Milliseconds::Tick() + Timings::Seconds::ToMillis(4);
                        break;

                default:
                        break;
        }
}

bool MysqlClient::SetCurrentDb(const char *db)
{
        if (connectedSince)
        {
                try
                {
                        exec("USE ", db);
                        return true;
                }
                catch (...)
                {
                        return false;
                }
        }

        return false;
}

bool MysqlClient::exec_stmt(const bool bailoutIfError, const char *data, const int len)
{
        require(len >= 0);
        canRetryTransaction = false;

        _tls.notify();

        if (isBlacklisted())
                return false;

        if (unlikely(!connectedSince))
        {
                if (IsConnectionConfigured())
                {
                        if (!Reconnect())
                        {
                                if (bailoutIfError)
                                        throw Switch::exception("MysqlClient handle not valid; cannot establish connection");

                                return false;
                        }
                }
                else if (bailoutIfError)
                        throw Switch::exception("MysqlClient not connected, unable to execute query");
                else
                        return false;
        }

        uint8_t efforts{0};

tryAgain:

        Drequire(handle);


        if (unlikely(mysql_real_query(handle, data, len)))
        {
                const auto err = mysql_errno(handle);

                considerError();

                switch (err)
                {
                        case ER_LOCK_WAIT_TIMEOUT:
                        case ER_LOCK_DEADLOCK:
                                if (inTransaction)
                                        return logQueryFail(strwlen32_t(data, len), bailoutIfError);
                                else if (++efforts < 8)
                                {
                                        Timings::Milliseconds::Sleep(100);
                                        goto tryAgain;
                                }
                                else
                                        return logQueryFail(strwlen32_t(data, len), bailoutIfError);

                        default:
                                if (blacklistedUntilTs)
                                {
                                mayTryAgain:
                                        if (connectedSince)
                                        {
                                                Shutdown();
                                                if (inTransaction && nStatementsInTransaction)
                                                        return logQueryFail(strwlen32_t(data, len), bailoutIfError);

                                                canRetryTransaction = true;
                                        }

                                        if (efforts++ == 0 && Reconnect())
                                                goto tryAgain;
                                        else
                                                return logQueryFail(strwlen32_t(data, len), bailoutIfError);
                                }
                                else if (err == CR_SERVER_LOST || err == CR_SERVER_GONE_ERROR || err == CR_SERVER_LOST_EXTENDED)
                                {
                                        goto mayTryAgain;
                                }
                                else
                                        return logQueryFail(strwlen32_t(data, len), bailoutIfError);
                                break;
                }
        }

#ifdef SWITCH_DATA_ENABLE_STMT_HISTORY
        prevQuery.clear();
        prevQuery.Append(data, len);
#endif

        const auto cmd = cmdFromStatement(strwlen32_t(data, len));

        if (inTransaction && (cmd.EqNoCase(_S("INSERT")) || cmd.EqNoCase(_S("DELETE")) || cmd.EqNoCase(_S("UPDATE")) || cmd.EqNoCase(_S("ALTER"))))
        {
                lastTransactionMutationStatemenTS = Timings::Microseconds::Tick();
                ++nStatementsInTransaction;
        }
#if defined(ENABLE_RESULTS_SANITY_CHECKS)
        else if (cmd.EqNoCase(_S("SELECT")) || cmd.EqNoCase(_S("SHOW")) || cmd.EqNoCase(_S("DESCRIBE")))
        {
                pendingResults = true;

                if (__data != queryBuffer.data())
                {
                        queryBuffer.clear();
                        queryBuffer.Append(data, len);
                }
        }
#endif

        return true;
}

void MysqlClient::init_handle()
{
        if (!handle)
                handle = mysql_init(nullptr);
}

bool MysqlClient::Connect(const char *hostname, const char *username, const char *pass, const char *db, int _port, const char *_socket, uint32_t flags)
{
        void *fl[32];
        uint8_t flSize{0};

        if (isBlacklisted())
                return false;

        flags |= CLIENT_REMEMBER_OPTIONS; // Rationale: http://dev.mysql.com/doc/refman/5.0/en/mysql-real-connect.html
                                          // Solution identified by Gabriel

        // Now using aditional options
        flags |= CLIENT_COMPRESS | CLIENT_IGNORE_SIGPIPE;

        if (!hostname || strcasecmp(this->hostName, hostname))
                strcpy(this->hostName, hostname ? hostname : "");

        if (!username || strcasecmp(this->userName, username))
                strcpy(userName, username ? username : "");

        if (!pass || strcasecmp(this->password, pass))
                strcpy(password, pass ? pass : "");

        // Reconnect() provides us with this->socket so we need to copy first
        // then free it because _socket may have been == this->socket
        auto *const ns = _socket ? strdup(_socket) : nullptr;

        if (socket)
                free(socket);
        socket = ns;

        port = _port;

        pendingResults = false;
        inTransaction = false;
        nStatementsInTransaction = 0;

        while (flSize)
                free(fl[--flSize]);

        if (!db || strcmp(curDb, db))
        {
                if (db)
                        strwlen32_t(db).ToCString(curDb, sizeof(curDb));
                else
                        curDb[0] = '\0';
        }

        if (connectedSince || handle)
        {
                mysql_close(handle);
                connectedSince = 0;
                handle = nullptr;
        }

        handle = mysql_init(nullptr);

        require(handle);
        if (ssl.enabled)
        {
                // https://dev.mysql.com/doc/refman/5.7/en/mysql-ssl-set.html
                //
                // > It is optional to call mysql_ssl_set() to obtain an encrypted connectionm because by default, mySQL programs attempt to connect using encryption if the
                // > server supports encrypted connections, falling back to an unencrypted connection if an encrypted connection cannot be established.
                // > mysql_ssl_set() may be used to applications that must specifiy particular certificate and key files, encryption ciphers, and so forth.

                mysql_ssl_set(handle,
                              ssl.paths.key,
                              ssl.paths.cert,
                              ssl.paths.ca,
                              ssl.paths.capath,
                              ssl.cipher);
        }

        if (handle && GetClientVersion() >= 50000)
                mysql_options(handle, MYSQL_SET_CHARSET_NAME, "binary");

        my_bool reconnect{0};
        uint32_t timeout{48};

        mysql_options(handle, MYSQL_OPT_RECONNECT, &reconnect);

#if defined(SET_TIMEOUT_ONCE)
        mysql_options(handle, MYSQL_OPT_WRITE_TIMEOUT, (const char *)&timeout);
#endif

        mysql_options(handle, MYSQL_OPT_COMPRESS, nullptr);

        for (;;)
        {
                if (!mysql_real_connect(handle,
                                        this->hostName[0] ? this->hostName : nullptr,
                                        this->userName, this->password,
                                        this->curDb, this->port, (!this->socket || !*this->socket) ? nullptr : this->socket, flags))
                {
                        logError("Failed to connect to ", this->curDb, "@", this->hostName, ":", this->port, ": ", mysql_error(handle), "(", mysql_errno(handle), ")\n");

                        considerError();
                        mysql_close(handle);
                        handle = nullptr;
                        return false;
                }
                else
                        break;
        }

        connectedSince = Timings::Milliseconds::SysTime();

        [[maybe_unused]] const auto clientVersion = GetClientVersion();
        [[maybe_unused]] const auto serverVersion = GetServerVersion();

#if defined(SET_TIMEOUT_ONCE)
        return exec_stmt(false, _S("SET SESSION wait_timeout = 64"));
#else
        return true;
#endif
}

bool MysqlClient::Begin(void)
{
        if (unlikely(inTransaction))
        {
                if (nStatementsInTransaction)
                        throw Switch::data_error("Attempting to begin new transaction while another transaction is active");
        }

        if (unlikely(!exec_stmt(false, _S("BEGIN"))))
                return false;

        inTransaction = true;
        nStatementsInTransaction = 0;
        canRetryTransaction = false;

        return true;
}
