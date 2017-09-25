using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;

using YdUtilities.Logger;
using YdUtilities.Constants;

namespace YdUtilities.Sql
{
    public class SqlContext : IDisposable
    {
        public SqlConnection conn = null;
        public SqlTransaction transaction = null;
        public string stacktrack = string.Empty;
        public string errMsg = string.Empty;
        public bool errorFlag = false;

        public int Commit()
        {
            return SqlHelper.CommitContextTransaction(this);
        }

        public int Rollback()
        {
            return SqlHelper.RollbackContextTransaction(this);
        }
        public int FinishTransaction()
        {
            return SqlHelper.FinishContextTransaction(this);
        }
        public void Dispose()
        {
            SqlHelper.DisposeContext(this);
        }
    }

    public static class SqlHelper
    {
        //Database connection strings
        public static string ConnectionString = "";

        #region Transaction Context

        public static SqlContext BeginContext()
        {
            SqlContext context = new SqlContext();
            try
            {
                context.conn = new SqlConnection(SqlHelper.ConnectionString);
                context.conn.Open();
            }
            catch (Exception e)
            {
                LogHelper.logError("Error at Open connection:" + SqlHelper.ConnectionString, e);
                context = null;
            }
            return context;
        }

        public static SqlContext BeginContextWithTransaction()
        {
            SqlContext context = BeginContext();

            try
            {
                context.transaction = context.conn.BeginTransaction(IsolationLevel.ReadCommitted);
                LogHelper.logInfo("Transaction started");
            }
            catch (Exception e)
            {
                LogHelper.logError("Error at Begin Transaction", e);
                context = null;
            }
            return context;
        }

        public static int CommitContextTransaction(SqlContext context)
        {
            int ret = ConstantManagement.ERROR;

            try
            {
                context.transaction.Commit();
                ret = ConstantManagement.OK;
                LogHelper.logInfo("Transaction committed! with stacktrack:" + context.stacktrack);
            } 
            catch(Exception e)
            {
                LogHelper.logError("Error at commit transaction!" + context.stacktrack, e);
                context.Rollback();
                context.Dispose();
            }
            
            return ret;
        }

        public static int FinishContextTransaction(SqlContext context)
        {
            if(context.transaction == null)
            {
                LogHelper.logError("Program error, commit a context without transaction", new Exception("Placeholder"));
                return ConstantManagement.OK;
            }

            int ret = ConstantManagement.OK;
            if(context.errorFlag || SqlHelper.CommitContextTransaction(context) != ConstantManagement.OK)
            {
                ret = ConstantManagement.DB_ERROR;
                SqlHelper.RollbackContextTransaction(context);
            }
            return ret;
        }

        public static int RollbackContextTransaction(SqlContext context)
        {
            int ret = ConstantManagement.ERROR;

            try
            {
                context.transaction.Rollback();
                ret = ConstantManagement.OK;
                LogHelper.logInfo("Transaction rollback!");
            }
            catch (Exception e)
            {
                LogHelper.logError("Error at rollback transaction!", e);
                context.Dispose();
            }

            return ret;
        }

        public static void DisposeContext(SqlContext context)
        {
            if (context.conn != null && context.conn.State != ConnectionState.Closed)
                context.conn.Close();
        }

        #endregion

        #region ExecuteNonQuery
        /// <summary>
        /// 执行sql命令
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="commandType"></param>
        /// <param name="commandText">sql语句/参数化sql语句/存储过程名</param>
        /// <param name="commandParameters"></param>
        /// <returns>受影响的行数</returns>
        public static int ExecuteNonQuery(CommandType commandType, string commandText, SqlParameter[] commandParameters = null)
        {

            using(SqlContext context = SqlHelper.BeginContext()) {
                if (context == null)
                    return ConstantManagement.DB_OPEN_CONNECTION_FAIL;

                return ExecuteNonQuery(context, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// 执行sql命令
        /// </summary>
        /// <param name="conn">sqlconnection</param>
        /// <param name="commandType"></param>
        /// <param name="commandText">sql语句/参数化sql语句/存储过程名</param>
        /// <param name="commandParameters"></param>
        /// <returns>受影响的行数</returns>
        public static int ExecuteNonQuery(SqlContext context, CommandType commandType, string commandText, SqlParameter[] commandParameters = null)
        {
            context.stacktrack += "<br>ExecuteNonQuery:" + commandText;
            SqlCommand cmd = new SqlCommand();
            int ret;

            try
            {
                PrepareCommand(cmd, commandType, context, commandText, commandParameters);
                ret = cmd.ExecuteNonQuery();
                LogHelper.logInfo(new { commandText = commandText, parameter = LogHelper.Enumerable2String(commandParameters) });
            }
            catch (Exception e)
            {
                context.errorFlag = true;
                context.errMsg = e.Message;
                LogHelper.logError(new { commandType = commandType, commandText = commandText, parameter = LogHelper.Enumerable2String(commandParameters) }, e);
                ret = -1;
            }

            return ret;
        }


        /*
        /// <summary>
        /// 执行Sql Server存储过程
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="spName">存储过程名</param>
        /// <param name="parameterValues"></param>
        /// <returns>受影响的行数</returns>
        public static int ExecuteNonQuery(string connectionString, string spName, params object[] parameterValues)
        {
            int ret;

            if (string.IsNullOrWhiteSpace(connectionString))
                connectionString = SqlHelper.ConnectionString;

            try
            {
                using (SqlContext context = SqlHelper.BeginContext())
                {
                    if (context == null)
                        return ConstantManagement.DB_OPEN_CONNECTION_FAIL;

                    SqlCommand cmd = new SqlCommand();

                    PrepareCommand(cmd, context, spName, parameterValues);
                    ret = cmd.ExecuteNonQuery();
                    LogHelper.logInfo(new { spName = spName, parameters = LogHelper.Enumerable2String(parameterValues) });
                }
            }
            catch (Exception e)
            {
                LogHelper.logError(new { spName = spName, parameters = LogHelper.Enumerable2String(parameterValues) }, e);
                ret = ConstantManagement.DB_ERROR;
            }
            return ret;
        }*/
        #endregion

        #region ExecuteReader
        /// <summary>
        ///  执行sql命令
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="commandParameters"></param>
        /// <returns>SqlDataReader 对象</returns>
        public static SqlDataReader ExecuteReader(CommandType commandType, string commandText, SqlParameter[] commandParameters = null)
        {
            SqlContext context = SqlHelper.BeginContext();
            if (context == null)
                return null;

            SqlDataReader rdr = null;
            try
            {
                SqlCommand cmd = new SqlCommand();
                PrepareCommand(cmd, commandType, context, commandText, commandParameters);
                rdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (Exception e)
            {
                LogHelper.logError(new { commandType = commandType, commandText = commandText, parameter = LogHelper.Enumerable2String(commandParameters) }, e);
                rdr = null;
            }
            return rdr;
        }

        /*
        public static SqlDataReader ExecuteReader(string connectionString, string spName, params SqlParameter[] parameterValues)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                connectionString = SqlHelper.ConnectionString;

            SqlConnection conn = new SqlConnection(connectionString);
            SqlDataReader rdr = null;
            try
            {
                SqlCommand cmd = new SqlCommand();

                PrepareCommand(cmd, conn, spName, parameterValues);
                rdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (Exception e)
            {
                LogHelper.logError(new { spName = spName, parameters = LogHelper.Enumerable2String(parameterValues) }, e);
                rdr = null;
            }
            return rdr;
        }*/
        #endregion

        #region ExecuteDataset
        public static DataSet ExecuteDataset(string spName, object[] parameterValues = null)
        {
            using (SqlContext context = SqlHelper.BeginContext())
            {
                if (context == null)
                    return null;

                return ExecuteDataset(context, spName, parameterValues);
            }
        }

        public static DataSet ExecuteDataset(SqlContext context, string spName, object[] parameterValues = null)
        {
            context.stacktrack += "<br>ExecuteDataset:" + spName;
            SqlCommand cmd = new SqlCommand();
            DataSet ds = new DataSet();

            try
            {
                PrepareCommand(cmd, context, spName, parameterValues);
                SqlDataAdapter da = new SqlDataAdapter(cmd);

                da.Fill(ds);
            }
            catch (Exception e)
            {
                context.errorFlag = true;
                context.errMsg = e.Message;
                LogHelper.logError(new { spName = spName, parameters = LogHelper.Enumerable2String(parameterValues) }, e);
                ds = null;
            }
            return ds;
        }


        public static DataSet ExecuteDataset(CommandType commandType, string commandText, SqlParameter[] commandParameters = null)
        {
            using (SqlContext context = SqlHelper.BeginContext())
            {
                if (context == null)
                    return null;

                return ExecuteDataset(context, commandType, commandText, commandParameters);
            }
        }

        public static DataSet ExecuteDataset(SqlContext context, CommandType commandType, string commandText, SqlParameter[] commandParameters = null)
        {
            context.stacktrack += "<br>ExecuteDataset:" + commandText;
            SqlCommand cmd = new SqlCommand(); ;
            DataSet ds = new DataSet();

            try
            {
                PrepareCommand(cmd, commandType, context, commandText, commandParameters);
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(ds);
            }
            catch (Exception e)
            {
                context.errorFlag = true;
                context.errMsg = e.Message;
                LogHelper.logError(new { commandType = commandType, commandText = commandText, parameters = LogHelper.Enumerable2String(commandParameters) }, e);
                ds = null;
            }

            return ds;
        }

        #endregion

        #region ExecuteScalar
        /// <summary>
        /// 执行Sql 语句
        /// </summary>
        /// <param name="connectionString">连接字符串</param>
        /// <param name="spName">Sql 语句/参数化的sql语句</param>
        /// <param name="parameterValues">参数</param>
        /// <returns>执行结果对象</returns>
        public static object ExecuteScalar(CommandType commandType, string commandText, SqlParameter[] commandParameters = null)
        {
            using (SqlContext context = SqlHelper.BeginContext())
            {
                if (context == null)
                    return null;

                return ExecuteScalar(context, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// 执行Sql 语句
        /// </summary>
        /// <param name="SqlConnection">sqlconnection</param>
        /// <param name="spName">Sql 语句/参数化的sql语句</param>
        /// <param name="parameterValues">参数</param>
        /// <returns>执行结果对象</returns>
        public static object ExecuteScalar(SqlContext context, CommandType commandType, string commandText, SqlParameter[] commandParameters = null)
        {
            context.stacktrack += "<br>ExecuteScalar:" + commandText;
            SqlCommand cmd = new SqlCommand();
            object val = null;

            try
            {
                PrepareCommand(cmd, commandType, context, commandText, commandParameters);
                val = cmd.ExecuteScalar();
            }
            catch (Exception e)
            {
                context.errorFlag = true;
                context.errMsg = e.Message;
                LogHelper.logError(new
                {
                    commandType = commandType,
                    commandText = commandText,
                    parameters = LogHelper.Enumerable2String(commandParameters)
                }, e);
            }
            return val;

        }

        /*
        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="connectionString">连接字符串</param>
        /// <param name="spName">存储过程名</param>
        /// <param name="parameterValues">存储过程参数</param>
        /// <returns>执行结果对象</returns>
        public static object ExecuteScalar(string connectionString, string spName, params object[] parameterValues)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                connectionString = SqlHelper.ConnectionString;

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                return ExecuteScalar(conn, spName, parameterValues);
            }
        }

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="connectionString">连接字符串</param>
        /// <param name="spName">存储过程名</param>
        /// <param name="parameterValues">存储过程参数</param>
        /// <returns>执行结果对象</returns>
        public static object ExecuteScalar(SqlConnection conn, string spName, params object[] parameterValues)
        {
            SqlCommand cmd = new SqlCommand();

            if (conn.State != ConnectionState.Open)
                conn.Open();
            PrepareCommand(cmd, conn, spName, parameterValues);
            object val = cmd.ExecuteScalar();

            return val;
        }*/


        #endregion

        #region BulkCopy

        /// <summary>
        /// 把DataTable中数据快速插入数据库指定表中
        /// </summary>
        /// <param name="connectionString">目标连接字符</param>
        /// <param name="tableName">目标表</param>
        /// <param name="dataSource">源数据</param>
        public static int SqlBulkCopyByDatatable(string tableName, DataTable dataSource, Dictionary<string, string> mappingDic = null)
        {
            int ret;

            using (SqlContext context = SqlHelper.BeginContextWithTransaction())
            {
                if (context == null) return ConstantManagement.DB_ERROR;

                SqlBulkCopyByDatatable(tableName, dataSource, context, mappingDic);

                ret = context.FinishTransaction();
            }

            return ret;
        }

        /// <summary>
        /// 把DataTable中数据快速插入数据库指定表中
        /// </summary>
        /// <param name="conn">sqlconnection</param>
        /// <param name="tableName">目标表</param>
        /// <param name="dataSource">源数据</param>
        public static int SqlBulkCopyByDatatable(string tableName, DataTable dataSource, SqlContext context, Dictionary<string, string> mappingDic = null)
        {
            context.stacktrack += "<br>Sql BulkCopy to table:" + tableName;

            using (SqlBulkCopy sqlbulkcopy = new SqlBulkCopy(context.conn, SqlBulkCopyOptions.FireTriggers, context.transaction))
            {
                try
                {
                    sqlbulkcopy.DestinationTableName = tableName;
                    //把超时时间设置为0，指示无限制
                    sqlbulkcopy.BulkCopyTimeout = 0;

                    if (mappingDic != null)
                    {
                        foreach (var key in mappingDic.Keys)
                        {
                            sqlbulkcopy.ColumnMappings.Add(key, string.IsNullOrEmpty(mappingDic[key]) ? key : mappingDic[key]);
                        }
                    }

                    //把dataSource数据插入到指定位置
                    sqlbulkcopy.WriteToServer(dataSource);

                    //tran.Commit();
                    LogHelper.logInfo(new { method = "SqlBulkCopyByDatatable", tableName = tableName, colMaps = LogHelper.Dictionary2String(mappingDic), tableInfo = LogHelper.BriefInfoFromTable(dataSource) });
                }
                catch (Exception ee)
                {
                    //LogHelper.logError(new { tableName = tableName, cols = LogHelper.Dictionary2String(mappingDic), dataTable = LogHelper.DataTable2Json(dataSource) }, ee);
                    LogHelper.logError(new { method = "SqlBulkCopyByDatatable", tableName = tableName, colMaps = LogHelper.Dictionary2String(mappingDic), tableInfo = LogHelper.BriefInfoFromTable(dataSource) }, ee);
                    context.errorFlag = true;
                    context.errMsg = ee.Message;
                    return ConstantManagement.DB_ERROR;
                }
            }

            return ConstantManagement.OK;
        }

        #endregion

        #region Private Method
        /// <summary>
        /// 设置一个等待执行的SqlCommand对象
        /// </summary>
        /// <param name="cmd">SqlCommand 对象，不允许空对象</param>
        /// <param name="conn">SqlConnection 对象，不允许空对象</param>
        /// <param name="commandText">Sql 语句</param>
        /// <param name="cmdParms">SqlParameters  对象,允许为空对象</param>
        private static void PrepareCommand(SqlCommand cmd, CommandType commandType, SqlContext context, string commandText, SqlParameter[] cmdParms)
        {
            //打开连接
            if (context.conn.State != ConnectionState.Open)
                context.conn.Open();

            //设置SqlCommand对象
            cmd.Connection = context.conn;
            cmd.CommandText = commandText;
            cmd.CommandType = commandType;
            if(context.transaction != null)
            {
                cmd.Transaction = context.transaction;
            }

            if (cmdParms != null)
            {
                foreach (SqlParameter parm in cmdParms)
                    cmd.Parameters.Add(parm);
            }
        }

        /// <summary>
        /// 设置一个等待执行存储过程的SqlCommand对象
        /// </summary>
        /// <param name="cmd">SqlCommand 对象，不允许空对象</param>
        /// <param name="conn">SqlConnection 对象，不允许空对象</param>
        /// <param name="spName">Sql 语句</param>
        /// <param name="parameterValues">不定个数的存储过程参数，允许为空</param>
        private static void PrepareCommand(SqlCommand cmd, SqlContext context, string spName, object[] parameterValues)
        {
            //打开连接
            if (context.conn.State != ConnectionState.Open)
                context.conn.Open();

            //设置SqlCommand对象
            cmd.Connection = context.conn;
            cmd.CommandText = spName;
            cmd.CommandType = CommandType.StoredProcedure;
            if (context.transaction != null)
            {
                cmd.Transaction = context.transaction;
            }

            //获取存储过程的参数
            SqlCommandBuilder.DeriveParameters(cmd);

            //移除Return_Value 参数
            cmd.Parameters.RemoveAt(0);

            //设置参数值
            if (parameterValues != null)
            {
                for (int i = 0; i < cmd.Parameters.Count; i++)
                {
                    cmd.Parameters[i].Value = parameterValues[i];

                }
            }
        }
        #endregion

    }
}
