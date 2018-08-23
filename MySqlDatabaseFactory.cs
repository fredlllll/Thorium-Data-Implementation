using System;
using System.Collections.Generic;
using System.Text;
using MySql.Data.MySqlClient;
using Thorium.Config;

namespace Thorium.Data.Implementation
{
    public class MySqlDatabaseFactory : ARawDatabaseFactory
    {
        readonly string host;
        readonly ushort port;
        readonly string user;
        readonly string password;
        readonly string db;
        readonly string tablePrefix;

        MySqlConnection GetNewConnection(string host, ushort port, string user, string password, string db)
        {
            //reference https://dev.mysql.com/doc/connector-net/en/connector-net-connection-options.html
            return new MySqlConnection("SERVER=" + host + ";PORT=" + port + ";DATABASE=" + db + ";USER=" + user + ";PASSWORD=" + password + ";SslMode=None;Allow User Variables=True");
        }

        public MySqlDatabaseFactory()
        {
            dynamic config = ConfigFile.GetConfig("mysql");

            host = config.DatabaseHost;
            port = config.DatabasePort;
            user = config.DatabaseUser;
            password = config.DatabasePassword;
            db = config.DatabaseName;
            tablePrefix = config.TablePrefix;
        }

        protected override IRawDatabase GetNewDatabaseInstance()
        {
            var conn = GetNewConnection(host, port, user, password, db);
            conn.Open();

            return new MySqlDatabase(conn)
            {
                TablePrefix = tablePrefix
            };
        }
    }
}
