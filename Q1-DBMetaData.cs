using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Data;

namespace dataShopQuandleClass
{
    //This Class takes in a Database Name Code (WIKI) and builds a table of each page within the metadata from Quandle. Each page 
    //contains list of Datasets (Same as a unique instance of a URL for data retrieval). Each dataset contains the URL for that dataset, description of that URL/Dataset, and the column Names.
    //This class increments the page number until it gets to the last page. The page number is at the top of the page with "dataSets" element name.
    //It runs a loop for each page. And within each page it runs a loop on each dataset. Turns full page into a Datatable with the URL and Metadata Values
    //When Datatable for page is created is sends to a mySQL class to write/Update the values 
    //ex:https://www.quandl.com/api/v3/datasets.xml?database_code=FRED&per_page=100&sort_by=id&page=1&api_key=x8LRTeBsGSxjdXjzcfJM 
    public static class Q1_DBMetaData
    {
         public static DataTable loadMetaData(String DBName, int DBID)
        {
            // DataSet dsVals = new DataSet();
            DataTable dtURLVals = initializeMetaTbl();
            DataTable dtcolStructure = initializeColTbl();
            DataSet tbls = new DataSet();
            tbls.Tables.Add(dtURLVals);
            tbls.Tables.Add(dtcolStructure);
            String URLString = "";
            int pageNum = 1;
            int MaxPageNum = 100;
            //** Run Stat
            DateTime runStart = DateTime.Now;
           // mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This is the start of Q1 where it takes in a database and runs for all pages within the metadata page for that database. It Saves value in qURLs and qcolStructure. This is the beginning for the Whole DB. DBName:" + DBName, "start", 0,0, "Q1:Start");
            do{
                DateTime pageStart = DateTime.Now;
                //this do is for each page of quandle Metadata. page get incremented for each do
                URLString = "https://www.quandl.com/api/v3/datasets.xml?database_code=" + DBName + "&per_page=100&sort_by=id&page=" + pageNum + "&api_key=x8LRTeBsGSxjdXjzcfJM";
              
                XmlReader readXML = XmlReader.Create(URLString);
                mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This is just after the URL is loaded from quandle. So time to load xml variable with page from quandle. Datbase:" + DBName, "full", (int)((TimeSpan)(DateTime.Now - pageStart)).TotalMilliseconds, 0, "Q1:APIxml");
                pageStart = DateTime.Now;
                while (readXML.Read())
                {
                    if (readXML.NodeType == XmlNodeType.Element)
                    {
                        if (readXML.Name == "datasets")
                        {                           
                            MaxPageNum = SetPage(readXML);
                        }
                        else if (readXML.Name == "dataset")
                        {
                            DateTime pageTime = DateTime.Now;
                            DataSet thsDS   = readDataSet(readXML, dtURLVals, DBID, dtcolStructure, tbls);
                            mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This is just after the xml is read and stored in a table. Before actually written to DB:" + DBName, "end", (int)((TimeSpan)(DateTime.Now - pageTime)).TotalMilliseconds, 0, "Q1:XMLParse");
                            dtURLVals = thsDS.Tables[0];
                            pageTime = DateTime.Now;
                            //Check This***
                            dtURLVals.Rows[dtURLVals.Rows.Count - 1]["colStructure_ID"] = writeURLMetadata.getColStructureID(thsDS.Tables[1], DBID);
                            //dtcolStructure = thsDS.Tables[1];
                            mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This is just after script to write table of qurls to Database:" + DBName, "end", (int)((TimeSpan)(DateTime.Now - pageTime)).TotalMilliseconds, 0, "Q1:ColStructureToDB");
                        }
                    }
                }
                mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This is the end of one page of metadata values stored in qurls to get qurls from metadata page NOT including time to read URL just to write to DB and parse XML that is already in variable. Datbase:" + DBName, "full", (int)((TimeSpan)(DateTime.Now - pageStart)).TotalMilliseconds, 0, "Q1:URL");
                pageNum++;
                pageStart = DateTime.Now;
                List<int> lstURLIDs =   writeURLMetadata.addURLCode(dtURLVals);
                mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This time it takes for qURLS C# datatable is written to qurls Datbase:" + DBName, "full", (int)((TimeSpan)(DateTime.Now - pageStart)).TotalMilliseconds, 0, "Q1:qurlsToDB");
                //**might need probably not: writeURLMetadata.addColStructure(dtcolStructure, lstURLIDs, DBID);
                //  writeURLMetadata.getColStructureID(dtURLVals, DBID);
                dtcolStructure.Rows.Clear();
                dtURLVals.Rows.Clear();

            } while (pageNum <= MaxPageNum);
            //** Run Stat
            mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This is the End of Q1 where it takes in a database and runs for all pages within the metadata page for that database. It Saves value in qURLs and qcolStructure. This is the beginning for the Whole DB. DBName:" + DBName, "full", (int)((TimeSpan)(DateTime.Now - runStart)).TotalMilliseconds, 0, "Q1:Start");
            return dtURLVals;
        }

        private static DataSet readDataSet(XmlReader xml, DataTable URLVals, int DBID, DataTable dtColStructure, DataSet tbls)
        {
            DataRow newRow = URLVals.NewRow();
            newRow["database_ID"] = DBID;

            while (xml.Read())
            {
                if (xml.NodeType == XmlNodeType.Element) {
                    if (URLVals.Columns.Contains(xml.Name))
                    {
                        String thsName = xml.Name;
                        xml.Read();
                        newRow[thsName] = xml.Value;
                    }
                    else if (dtColStructure.Columns.Contains(xml.Name)){
                        String thsName = xml.Name;
                        xml.Read();
                        DataRow newColrow = dtColStructure.NewRow();
                        newColrow[thsName] = xml.Value;
                        dtColStructure.Rows.Add(newColrow);
                    }
                }
                else if (xml.NodeType == XmlNodeType.EndElement && xml.Name == "dataset")
                {                    
                    URLVals.Rows.Add(newRow);
                    
                    return tbls;
                }
            }
            
            return tbls;
        }

        private static DataTable initializeMetaTbl()
        {
            DataTable dtURLMetadata = new DataTable();
            dtURLMetadata.Columns.Add("dataBase_ID");
            dtURLMetadata.Columns.Add("name");
            dtURLMetadata.Columns.Add("dataset-code");
            dtURLMetadata.Columns.Add("description");
            dtURLMetadata.Columns.Add("refreshed-at");
            dtURLMetadata.Columns.Add("newest-available-date");
            dtURLMetadata.Columns.Add("oldest-available-date");
            dtURLMetadata.Columns.Add("premium");
            dtURLMetadata.Columns.Add("frequency");
            dtURLMetadata.Columns.Add("type");
            dtURLMetadata.Columns.Add("colStructure_ID");

            return dtURLMetadata;

        }
        private static DataTable initializeColTbl()
        {
            DataTable dtColStructure = new DataTable();
            dtColStructure.Columns.Add("qURL_ID");
            dtColStructure.Columns.Add("column-name");
            
            return dtColStructure;
        }
        private static int SetPage(XmlReader xml)
        {
           return getAttributes(xml);
        }

        private static int getAttributes(XmlReader xml)
        {
            while (xml.MoveToNextAttribute())
            {
                switch (xml.Name)
                {
                    case "total_pages":
                        //if type = Total Pages return Number
                        return Int32.Parse(xml.Value.ToString());              
                }
            }
            return 0;
        }
    }
}
