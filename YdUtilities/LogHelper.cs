using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Reflection;
using System.Data.SqlClient;
using System.Data;

namespace YdUtilities.Logger
{
    public enum InfoType
    {
        Default = 0, Sql, IO, 
    }


    public static class LogHelper
    {
        private static readonly log4net.ILog loginfo = log4net.LogManager.GetLogger("loginfo");

        private static readonly log4net.ILog logerror = log4net.LogManager.GetLogger("logerror");

        public static void logInfo(object infoObj)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var propertyInfo in infoObj.GetType().GetProperties())
            {
                sb.Append(string.Format("{0} : {1}<br>", propertyInfo.Name, propertyInfo.GetValue(infoObj)));
            }
            logInfo(sb.ToString());
        }

        public static void logInfo(string msg)
        {
            if (loginfo.IsInfoEnabled)
            {
                loginfo.Info(msg);
            }
        }

        public static void logError(Dictionary<string, string> infoDic, Exception e)
        {
            StringBuilder sb = new StringBuilder();
            foreach(var kvp in infoDic.AsEnumerable())
            {
                sb.Append(string.Format("{0} : {1}<br>", kvp.Key, kvp.Value));
            }
            logError(sb.ToString(), e);
        }

        public static void logError(object infoObj, Exception e)
        {
            StringBuilder sb = new StringBuilder();
            foreach(var propertyInfo in infoObj.GetType().GetProperties())
            {
                sb.Append(string.Format("{0} : {1}<br>", propertyInfo.Name, propertyInfo.GetValue(infoObj)));
            }
            logError(sb.ToString(), e);
        }

        public static void logError(string msg, Exception e = null)
        {
            if(logerror.IsErrorEnabled)
            {
                msg += @"<HR Size=1>";
                StackFrame[] sfs = new StackTrace(true).GetFrames();
                for(int i = 1; i < 6 && i < sfs.Length; i++)
                {
                    msg += sfs[i].ToString() + @"<br>";
                }

                logerror.Error(msg, e);
            }
        }
        
        public static string Enumerable2String(IEnumerable enumerables)
        {
            StringBuilder sb = new StringBuilder();
            
            if(enumerables != null)
            {
                foreach(object obj in enumerables)
                {
                    if(obj is SqlParameter) {
                        SqlParameter para = (SqlParameter)obj;
                        sb.Append(string.Format("<br>  {0} : {1}", para.ParameterName, para.Value));
                    }
                    else
                    {
                        sb.Append("<br>  " + obj.ToString());
                    }
                }
            }
            else
            {
                sb.Append("null");
            }

            return sb.ToString();
        }

        public static string Dictionary2String<TKey, TValue>(Dictionary<TKey, TValue> dic)
        {
            if (dic == null)
                return "null";

            StringBuilder sb = new StringBuilder();
            foreach(var kvp in dic.AsEnumerable())
            {
                sb.Append(string.Format("<br>  {0} : {1}", kvp.Key, kvp.Value));
            }
            return sb.ToString();
        }

        public static string DataTable2Json(DataTable dt)
        {
            StringBuilder JsonString = new StringBuilder();
            //Exception Handling        
            if (dt != null && dt.Rows.Count > 0)
            {
                JsonString.Append("{ ");
                JsonString.Append("\"T_blog\":[ ");
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    JsonString.Append("{ ");
                    for (int j = 0; j < dt.Columns.Count; j++)
                    {
                        if (j < dt.Columns.Count - 1)
                        {
                            JsonString.Append("\"" + dt.Columns[j].ColumnName.ToString() + "\":" + "\"" + dt.Rows[i][j].ToString() + "\",");
                        }
                        else if (j == dt.Columns.Count - 1)
                        {
                            JsonString.Append("\"" + dt.Columns[j].ColumnName.ToString() + "\":" + "\"" + dt.Rows[i][j].ToString() + "\"");
                        }
                    }

                    if (i == dt.Rows.Count - 1)
                    {
                        JsonString.Append("} ");
                    }
                    else
                    {
                        JsonString.Append("}, ");
                    }
                }
                JsonString.Append("]}");
                return JsonString.ToString();
            }
            else
            {
                return string.Empty;
            }
        }

        public static string BriefInfoFromTable(DataTable dt)
        {
            if (dt == null)
                return "null";
            StringBuilder sb = new StringBuilder();
            sb.Append(string.Format("Displaying 20 rows from {0} rows total<br><table><tr>", dt.Rows.Count));
            foreach (DataColumn col in dt.Columns)
            {
                sb.Append("<th>" + col.ColumnName + "</th>");
            }
            sb.Append("</tr>");

            for(int i = 0; i < dt.Rows.Count && i < 25; i++)
            {
                sb.Append("<tr>");
                for(int j = 0; j < dt.Columns.Count; j++)
                {
                    sb.Append(string.Format("<td>{0}</td>", dt.Rows[i][j]));
                }
                sb.Append("</tr>");
            }

            sb.Append("</table>");

            return sb.ToString();
        }

        public static string ColTitlesFromTable(DataTable dt)
        {
            if (dt == null)
                return "null";
            StringBuilder sb = new StringBuilder();
            foreach (DataColumn col in dt.Columns)
            {
                sb.Append(" " + col.ColumnName + ", ");
            }
            return sb.ToString();
        }

        public static string ConvertXmlEscapeCharacter(string content)
        {
            return content.Replace("<", "&lt;");
        }

    }
}
