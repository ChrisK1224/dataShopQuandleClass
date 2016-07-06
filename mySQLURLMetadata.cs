using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
namespace dataShopQuandleClass
{
    public static class writeURLMetadata
    {
        public static List<int> addURLCode(DataTable dtURLs)
        {
            return mySQLCalls.InsertLoop("qurls", dtURLs);
        }

        public static List<int> addColStructure(DataTable dtColStructure, List<int> urlsIds, int DBID)
        {
            //Might want to tkae in database name and then select with databaseID and URLCode to get URLID this should be unique
            //Then add that value to each row of dtcolstructure table
            if (urlsIds.Count != dtColStructure.Rows.Count)
            {
                String Blah = "Should not get here it means the URL_IDs assinged are off in the colstructure table";
            }
            int colNum = 1;
            foreach(DataRow thsRow in dtColStructure.Rows)
            {
                // thsRow["qURL_ID"] = mySQLCalls.SelectSQLRetNum("SELECT qURL_ID FROM qurls WHERE database_ID = " + DBID + " AND dataset-code='" + thsRow["dataset-code"] + "'");
                thsRow["qURL_ID"] = 1;
                thsRow["colNum"] = colNum;
                colNum++;
            }
            return mySQLCalls.InsertLoop("qcolstructure", dtColStructure);
        }
        //getColStrctureID -- Takes in DBID and then checks to see if col strcutre with all these col names already exists
        //if not it will create it. Will return the ID either way to be added to the QURL Table
        private static int InsertNewColStructure(DataTable dtColStructure, int DBID)
        {
            //Need to select Max ColList_ID then add one to it to get next ID
            //Inserts all columns then return the new ColListID
          int ColList_ID =  mySQLCalls.SelectSQLRetNum("SELECT colList_ID FROM qColStructure WHERE database_ID = " + DBID);
            ColList_ID++;
            int colNum = 1;
            foreach (DataRow thsCol in dtColStructure.Rows)
            {
                mySQLCalls.InsertSQL("INSERT INTO qcolStructure (database_ID, `column-name`, colList_ID, colNum) VALUES(" + DBID + ",'" + thsCol["column-name"] + "'," + ColList_ID + "," + colNum + ")");
                colNum++;
            }
            return ColList_ID;
        }
        public static int getColStructureID(DataTable dtColStructure, int DBID)
        {
            //table: Date
            //       EOD Value
            // AND DBID
            //Going to store all colLstID's in this list do a where clause on each col value and keep taking colLstiD"s off the list if not on each results of select
            List<int> ColListInts = mySQLCalls.SelectSQLRetListNum("SELECT colList_ID FROM qColStructure WHERE database_ID = " + DBID + " AND `column-name`='" + dtColStructure.Rows[0]["column-name"].ToString() + "'", true);
            //DtcolStructure has rows of all column names
            if (ColListInts.Count == 0)
            {
                return InsertNewColStructure(dtColStructure , DBID);
            }
            else
            {
                foreach (DataRow thsCol in dtColStructure.Rows)
                {
                    List<int> TempList = new List<int>();
                    TempList = mySQLCalls.SelectSQLRetListNum("SELECT colList_ID FROM qColStructure WHERE database_ID = " + DBID + " AND `column-name`='" + thsCol["column-name"] + "'", true);
                    //Need to add sql call that returns list of values so just one column
                    foreach (int ID in TempList)
                    {
                        if (!ColListInts.Contains(ID))
                        {
                            ColListInts.Remove(ID);
                        }
                    }
                }
                if (ColListInts.Count == 1)
                {
                    return ColListInts[0];
                }
                else
                {
                    //***Write Error Log HERE should only be length of one or else problem
                    return 0;
                }
            }
        }
    }

}
