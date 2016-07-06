using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
namespace dataShopQuandleClass
{
  public static  class Q4_TempTableConstruct
    {
        public static DataTable buildTbl(DataTable dtFieldInfo, int URLID)
        {
            try
            {
                           
           // mySQLCalls mySQL = new mySQLCalls("Data Source=Chris-Base\\SQLEXPRESS;Initial Catalog=dataShop;Integrated Security=False;User Id=sa;Password=shallNotpass12;");
            DataTable tmpTable = new DataTable();
            mySQLCalls.SelectSQL("DROP TABLE IF EXISTS `TEMP1`", false);
            String qryStr = "CREATE TABLE TEMP1 (";
            foreach (DataRow thsRow in dtFieldInfo.Rows)
            {
                qryStr = qryStr + "`" + thsRow["column-name"] + "` " + thsRow["qcolType"] + ",";
            }
            qryStr = qryStr.Substring(0, qryStr.Length - 1);
            qryStr = qryStr + ")";
            mySQLCalls.SelectSQL(qryStr, false);

            //pull in qValues one row at a time. First get highest row then loop unitl you hit the highest row
            //     int highRow = Int32.Parse(mySQLCalls.SelectSQL("SELECT MAX(rowINT) FROM datastore.qvalues inner join qvaluemetadata on qvalues.qValueCol_ID = qvaluemetadata.qvalueCol_ID WHERE qvaluemetadata.qCodeVal_ID = " + URLID, true).Rows[0][0].ToString());
            int highRow = mySQLCalls.SelectSQLRetNum("SELECT MAX(rowNum) FROM datastore.qvalues inner join qurls on qvalues.qURL_ID = qurls.qURL_ID WHERE qurls.qURL_ID = " + URLID);

            //int currRow = 1;
            for (int curRow = 1; curRow <= highRow; curRow++)
            {
                //DataTable valTbl = mySQLCalls.SelectSQL("SELECT `column-name`, colValue, ColType FROM datastore.qvalues inner join qurls on qvalues.qURL_ID = qurls.qURL_ID  = " + URLID + " AND qvalues.rowNum = " + curRow + " order by colNum", true);
                DataTable valTbl = mySQLCalls.SelectSQL("SELECT `column-name`, qcolValue, qColType   FROM datastore.qvalues inner join qurls on qvalues.qURL_ID = qurls.qURL_ID inner join qcolstructure ON qcolstructure.colList_ID = qurls.colStructure_ID and qcolstructure.colNum = qvalues.colNum WHERE qurls.qURL_ID = " + URLID + " AND qvalues.rowNum = " + curRow + " order by qcolstructure.colNum", true);
                string insertQry = "INSERT INTO TEMP1 (`";
                foreach (DataRow thsRow in valTbl.Rows)
                {
                    insertQry = insertQry + thsRow["column-name"] + "`,`";
                }
                insertQry = insertQry.Substring(0, insertQry.Length - 2);
                insertQry = insertQry + ") VALUES ('";
                foreach (DataRow thsRow in valTbl.Rows)
                {
                    if (thsRow["qcolType"].ToString() == "nil")
                    {
                        insertQry = insertQry + 0 + "','";
                    }
                    else
                    {
                        String blah = thsRow["qcolValue"].ToString();
                        insertQry = insertQry + thsRow["qcolValue"] + "','";
                    }
                }
                insertQry = insertQry.Substring(0, insertQry.Length - 2);
                insertQry = insertQry + ")";
                if (valTbl.Rows.Count > 0)
                {
                    mySQLCalls.SelectSQL(insertQry, false);
                }
            }

            return tmpTable;

            }
            catch (Exception ex)
            {
                mySQLCalls.insertError("Q4_TempmTableConstruct", "buildTbl", ex.ToString(), "SDF", 0);
                return dtFieldInfo;
            }
        }
//        SELECT `column-name`, qcolValue, qColType, colNum
//FROM datastore.qvalues
//inner join qurls
//on qvalues.qURL_ID = qurls.qURL_ID

//inner join qcolstructure ON
//qcolstructure.colList_ID = qurls.colStructure_ID
//and qcolstructure.colNum = qvalues.colNum
//WHERE qurls.qURL_ID = 81745
//order by colNum
    }
}
