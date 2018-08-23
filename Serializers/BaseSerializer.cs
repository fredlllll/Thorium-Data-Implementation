﻿using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Thorium.Data.Implementation.Serializers
{
    public abstract class BaseSerializer<TKey, TValue> : ISerializer<TKey, TValue>
    {
        public abstract ARawDatabaseFactory DatabaseFactory { get; }
        public abstract string Table { get; }
        public abstract string KeyColumn { get; }

        public abstract void CreateConstraints();
        public abstract void CreateTable();
        public abstract TValue Load(TKey key);
        public abstract void Save(TKey key, TValue value);

        public void Delete(TKey key)
        {
            string sql = SqlBuilder.DeleteWhere(Table, new SqlCondition(KeyColumn, key));
            //string sql = "DELETE FROM " + Table + " WHERE " + KeyColumn + "=@0;";
            DatabaseFactory.GetDatabase().ExecuteNonQueryTransaction(sql, key);
        }

        public DbDataReader SelectStarWhereKey(TKey key)
        {
            string sql = SqlBuilder.SelectFrom(new string[] { "*" }, Table, new string[] { KeyColumn });
            //string sql = "SELECT * FROM " + Table + " WHERE " + KeyColumn + " = @0;";
            DbDataReader reader = DatabaseFactory.GetDatabase().ExecuteQuery(sql, key);
            if(!reader.HasRows)
            {
                reader.Close();
                throw new KeyNotFoundException("no element with key '" + key + "' found in database");
            }
            return reader;
        }

        public DbDataReader SelectStarWhere(params SqlCondition[] conditions)
        {
            string sql = SqlBuilder.SelectFrom(new string[] { "*" }, Table, conditions);

            /*StringBuilder sql = new StringBuilder("SELECT * FROM " + Table);
            if(conditions.Length > 0)
            {
                sql.Append(" WHERE");
                for(int i = 0; i < conditions.Length; i++)
                {
                    var cond = conditions[i];
                    sql.Append(" ");
                    sql.Append(cond.Item1);
                    sql.Append(" = ");
                    sql.Append("@" + i);
                    if(i < conditions.Length - 1)
                    {
                        sql.Append(" AND ");
                    }
                }
            }
            sql.Append(";");*/

            DbDataReader reader = DatabaseFactory.GetDatabase().ExecuteQuery(sql.ToString(), conditions.Select(x => x.ShouldBe).ToArray());
            return reader;
        }

        public IEnumerable<TValue> LoadAll()
        {
            string sql = SqlBuilder.SelectFrom(new string[] { KeyColumn }, Table);
            //string sql = "SELECT " + KeyColumn + " FROM " + Table;
            List<TKey> keys = new List<TKey>();
            using(var reader = DatabaseFactory.GetDatabase().ExecuteQuery(sql))
            {
                if(reader.HasRows)
                {
                    while(reader.Read())
                    {
                        keys.Add((TKey)reader[KeyColumn]);
                    }
                }
            }
            foreach(var key in keys)
            {
                yield return Load(key);
            }
        }

        public IEnumerable<TValue> LoadWhere<TWhere>(string column, TWhere shouldBe)
        {
            return LoadWhere(new SqlCondition(column, shouldBe));
        }

        public IEnumerable<TValue> LoadWhere(params SqlCondition[] conditions)
        {
            string sql = SqlBuilder.SelectFrom(new string[] { KeyColumn }, Table, conditions);

            /*StringBuilder sql = new StringBuilder("SELECT " + KeyColumn + " FROM " + Table);
            if(conditions.Length > 0)
            {
                sql.Append(" WHERE");
                for(int i = 0; i < conditions.Length; i++)
                {
                    var cond = conditions[i];
                    sql.Append(" ");
                    sql.Append(cond.Item1);
                    sql.Append(" = ");
                    sql.Append("@" + i);
                    if(i < conditions.Length - 1)
                    {
                        sql.Append(" AND ");
                    }
                }
            }
            sql.Append(";");*/

            List<TKey> keys = new List<TKey>();
            using(var reader = DatabaseFactory.GetDatabase().ExecuteQuery(sql.ToString(), conditions.Select(x => x.ShouldBe).ToArray()))
            {
                if(reader.HasRows)
                {
                    while(reader.Read())
                    {
                        keys.Add((TKey)reader[KeyColumn]);
                    }
                }
            }

            foreach(var key in keys)
            {
                yield return Load(key);
            }
        }

        public void DeleteWhere<TWhere>(string column, TWhere whereis)
        {
            string sql = SqlBuilder.DeleteWhere(Table, new string[] { column });
            //string sql = "DELETE FROM " + Table + " WHERE " + column + " = @0";
            DatabaseFactory.GetDatabase().ExecuteNonQueryTransaction(sql, whereis);
        }

        public void DeleteWhere(params SqlCondition[] conditions)
        {
            string sql = SqlBuilder.DeleteWhere(Table, conditions);

            /*StringBuilder sql = new StringBuilder("DELETE FROM " + Table);
            if(conditions.Length > 0)
            {
                sql.Append(" WHERE");
                for(int i = 0; i < conditions.Length; i++)
                {
                    var cond = conditions[i];
                    sql.Append(" ");
                    sql.Append(cond.Item1);
                    sql.Append(" = ");
                    sql.Append("@" + i);
                    if(i < conditions.Length - 1)
                    {
                        sql.Append(" AND ");
                    }
                }
            }
            sql.Append(";");*/

            DatabaseFactory.GetDatabase().ExecuteNonQueryTransaction(sql.ToString(), conditions.Select(x => x.ShouldBe).ToArray());
        }
    }
}
