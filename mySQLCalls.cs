using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Data;

namespace dataShopQuandleClass
{
    public static class mySQLCalls
    {


        //= "Data Source=Chris-Base\\SQLEXPRESS;Initial Catalog=dataShop;Integrated Security=False;User Id=sa;Password=shallNotpass12;";

        //= "Data Source=Chris-Base\\SQLEXPRESS;Initial Catalog=dataShop;Integrated Security=False;User Id=sa;Password=shallNotpass12;";
        private static String buildConStr()
        {
            String connStr;
            String server = "localhost";
            String database = "datastore";
            String uid = "root";
            String password = "shallNotpass12";

            connStr = "SERVER=" + server + ";" + "DATABASE=" + database + ";" + "UID=" + uid + ";" + "PASSWORD=" + password + ";";
            return connStr;
            //connStr = new MySqlConnection(connectionString);
        }

        public static DataTable SelectSQL(String query, Boolean isTable)
        {
            MySqlConnection sqlConnection1 = new MySqlConnection(buildConStr());
            MySqlCommand cmd = new MySqlCommand();

            cmd.CommandText = query;
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            var dataReader = cmd.ExecuteReader();
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            sqlConnection1.Close();
            if (dataTable.Rows.Count > 0)
            {
                if (isTable == true)
                {
                    return dataTable;
                }
                else
                {
                    //  return dataTable.Rows[0].ItemArray[0];
                    return dataTable;
                }
            }
            return dataTable;
        }

        public static void insertError(String thsclass = "", String proc = "", String exToString = "", String description = "",int qURL_ID = 1)
            {
            mySQLCalls.InsertSQL("INSERT INTO Errors (class, proc, extostring, description, qurl_ID) VALUES('" + thsclass + "','" + proc + "','" + exToString + "','" + description + "'," + qURL_ID + ")");
            }

        public static String SQLsafeText(String Val)
        {
            Val = Val.Replace("'", "''");
            return Val;
        }
        public static void InsertSQL(String query)
        {
            MySqlConnection sqlConnection1 = new MySqlConnection(buildConStr());
            MySqlCommand cmd = new MySqlCommand();

            cmd.CommandText = query;
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            var dataReader = cmd.ExecuteReader();
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            sqlConnection1.Close();
        }

        public static String SelectSQLRetString(String query)
        {
            MySqlConnection sqlConnection1 = new MySqlConnection(buildConStr());
            MySqlCommand cmd = new MySqlCommand();

            cmd.CommandText = query;
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            var dataReader = cmd.ExecuteReader();
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            sqlConnection1.Close();
            if (dataTable.Rows.Count > 0)
            {
                return dataTable.Rows[0][0].ToString();
            }
            else
            {
                return "";
            }

        }

        public static int SelectSQLRetNum(String query)
        {
            MySqlConnection sqlConnection1 = new MySqlConnection(buildConStr());
            MySqlCommand cmd = new MySqlCommand();

            cmd.CommandText = query;
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();
            var dataReader = cmd.ExecuteReader();
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            sqlConnection1.Close();
            if (dataTable.Rows.Count > 0)
            {
                return Int32.Parse(dataTable.Rows[0][0].ToString());
            }
            else
            {
                return 0;
            }

        }

      public static List<string> SelectSQLRetListString(String query, Boolean isTable)
        {
            MySqlConnection sqlConnection1 = new MySqlConnection(buildConStr());
            MySqlCommand cmd = new MySqlCommand();

            cmd.CommandText = query;
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();

            List<String> resultLst = new List<String>();

            using (MySqlConnection connection = new MySqlConnection(buildConStr())) { 

            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        resultLst.Add(reader.GetString(0));
                    }
                }
            }
        }
            sqlConnection1.Close();
            return resultLst;
    }

        public static void InsertRunStat(String runTime, String message = "", String runType = "", int timeSpan_Secs = 0, int qurl_ID = 0, String runLocation = "")
        {
            mySQLCalls.InsertSQL("INSERT INTO RunStats (StatMessage, qURL_ID, runTime, timeSpan_Seconds, Start_End_Full, runLocation) VALUES('" + message + "'," + qurl_ID + ",'" + runTime + "'," + timeSpan_Secs + ",'" + runType + "','" + runLocation + "')");
        }
        public static List<int> SelectSQLRetListNum(String query, Boolean isTable)
        {
            MySqlConnection sqlConnection1 = new MySqlConnection(buildConStr());
            MySqlCommand cmd = new MySqlCommand();

            cmd.CommandText = query;
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();

            List<int> resultLst = new List<int>();

            using (MySqlConnection connection = new MySqlConnection(buildConStr()))
            {

                using (MySqlCommand command = new MySqlCommand(query, sqlConnection1))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            resultLst.Add(reader.GetInt32(0));
                        }
                    }
                }
            }
            sqlConnection1.Close();
            return resultLst;
        }
        public static List<int> InsertLoop(String DBName, DataTable dtURLs)
        {
            List<String> colList = new List<String>();
            List<int> lstURLs = new List<int>();
            String startSQL = "INSERT INTO " + DBName + " (`";

            foreach (DataColumn thsCol in dtURLs.Columns)
            {
                startSQL = startSQL + thsCol.ColumnName + "`,`";
            }
            startSQL = startSQL.Substring(0, startSQL.Length - 3);
            startSQL = startSQL + "`) VALUES(";
            int P = 0;
            String SQL;
            SQL = startSQL;
            DateTime str = DateTime.Now;
            DateTime strStart = DateTime.Now;
            foreach (DataRow thsRow in dtURLs.Rows)
            {
                do
                {
                    SQL = SQL + "'" + SQLsafeText(thsRow[P].ToString()) + "',";
                    P++;
                } while (P < dtURLs.Columns.Count);
                SQL = SQL.Substring(0, SQL.Length - 1);
                SQL = SQL + "),(";
                P = 0;
               
                //lstURLs.Add(mySQLCalls.SelectSQLRetNum("SELECT LAST_INSERT_ID()"));
            //        SQL = startSQL;
            }
            SQL = SQL.Substring(0, SQL.Length - 2);
            mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "Completion of insert DB Values for one qURL Rows in qurl Table:" + dtURLs.Rows.Count.ToString(), "full", (int)((TimeSpan)(DateTime.Now - strStart)).TotalMilliseconds, 0, "Q2:DBValInsertQueryCreation");
            strStart = DateTime.Now;
            mySQLCalls.InsertSQL(SQL);
            mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "Completion of insert DB Values for one qURL Rows in qurl Table:" + dtURLs.Rows.Count.ToString(), "full", (int)((TimeSpan)(DateTime.Now - strStart)).TotalMilliseconds, 0, "Q2:DBTableFinish");
            return lstURLs;
        }
    }

    //public static List<int> InsertLoopOLD(String DBName, DataTable dtURLs)
    //{
    //    List<String> colList = new List<String>();
    //    List<int> lstURLs = new List<int>();
    //    String startSQL = "INSERT INTO " + DBName + " (`";

    //    foreach (DataColumn thsCol in dtURLs.Columns)
    //    {
    //        startSQL = startSQL + thsCol.ColumnName + "`,`";
    //    }
    //    startSQL = startSQL.Substring(0, startSQL.Length - 3);
    //    startSQL = startSQL + "`) VALUES(";
    //    int P = 0;
    //    String SQL;
    //    SQL = startSQL;
    //    DateTime str = DateTime.Now;
    //    DateTime strStart = DateTime.Now;
    //    foreach (DataRow thsRow in dtURLs.Rows)
    //    {
    //        do
    //        {
    //            SQL = SQL + "'" + SQLsafeText(thsRow[P].ToString()) + "',";
    //            P++;
    //        } while (P < dtURLs.Columns.Count);
    //        SQL = SQL.Substring(0, SQL.Length - 1);
    //        SQL = SQL + ")";
    //        P = 0;
    //        str = DateTime.Now;
    //        mySQLCalls.InsertSQL(SQL);
    //        mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "Completion of one row Insert. Run the time is takes to run the SQL", "full", (int)((TimeSpan)(DateTime.Now - str)).TotalMilliseconds, 0, "Q2:DBInsertOneRow");
    //        lstURLs.Add(mySQLCalls.SelectSQLRetNum("SELECT LAST_INSERT_ID()"));
    //        SQL = startSQL;
    //    }
    //    mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "Completion of full table from Insert Loop to insert DB Values for one qURL Rows in qurl Table:" + dtURLs.Rows.Count.ToString(), "full", (int)((TimeSpan)(DateTime.Now - strStart)).TotalMilliseconds, 0, "Q2:DBTableFinish");
    //    return lstURLs;
    //}
}
