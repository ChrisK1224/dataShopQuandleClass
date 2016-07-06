using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Xml;

namespace dataShopQuandleClass
{
    //This Class takes in list of URL's then hits each URL and gets the table of values for each page.  Then **NEEDS** to send that table
    //to be written in database before moving on to next code. Needs to save the URLID to the table so probably need to take in URL_ID as well.
   public static class Q2_DBValues
    {
       public static void insertValues(DataTable tblURLs, String dbName, int DBID)
        {
            DateTime strTimeInsert = DateTime.Now;
            //  mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This is the start of Insert DB Values Goes through table of qURLs and gets values for each Long Process... datbase:" + DBID.ToString(), "start", 0, 0, "Q2:Start");
            foreach (DataRow thsURL in tblURLs.Rows)
            {
                DateTime strTime = DateTime.Now;
                //Get Values from API from each URL in this above table. Return table with all the XML Values in it
                // readURLXml xmlValues = new readURLXml(thsURL.ItemArray[0].ToString());
                DataTable valTbl =  readURLXmlVals("https://www.quandl.com/api/v3/datasets/" + dbName + "/" + thsURL["dataset-code"].ToString() + "/data.xml?api_key=x8LRTeBsGSxjdXjzcfJM", Int32.Parse(thsURL["qURL_ID"].ToString()));
                mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This is the end of the api reading to quandle URL to get dbValues from. Datbase:" + DBID.ToString(), "full", (int)((TimeSpan)(DateTime.Now - strTime)).TotalMilliseconds, Int32.Parse(thsURL["qURL_ID"].ToString()), "Q2:APIxmlRead");
                //DataTable dtVals = xmlValues.dtResults;
                strTime = DateTime.Now;//*This procedure is way to Slow****
                 mySQLCalls.InsertLoop("qvalues", valTbl);
                //foreach(DataRow thsVal in valTbl.Rows){
                //    //**Should change this datatable to be able to use InsertLoop for scaling purposes**
                //    //mySQLCalls.InsertLoop("qvalues",valTbl);
                //    mySQLCalls.InsertSQL("INSERT INTO qvalues (qcolValue,qurl_ID) VALUES('" + thsVal["qcolValue"] + "'," + thsURL["qURL_ID"] + ")");
                //}
                mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This is the start of one qURL to get dbValues from Not including time to read URL just to write to DB and parse XML that is already in variable. Datbase:" + DBID.ToString(), "full",  (int)((TimeSpan)(DateTime.Now - strTime)).TotalMilliseconds, Int32.Parse(thsURL["qURL_ID"].ToString()), "Q2:URL");
            }
            mySQLCalls.InsertRunStat(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "This is the End of Insert DB Values Goes through table of qURLs and gets values for each Long Process... datbase:" + DBID.ToString(), "full", (int)((TimeSpan)(DateTime.Now - strTimeInsert)).TotalMilliseconds, 0, "Q2:Start");
        }
        
            public static DataTable  readURLXmlVals(string URLString, int qurl_ID)
            {
                int rowInt;
                int colNum;
                DataTable dtResults = new DataTable();
                dtResults.Columns.Add("qcolName");
                dtResults.Columns.Add("qcolValue");
                dtResults.Columns.Add("qColType");
                dtResults.Columns.Add("rowNum");
              dtResults.Columns.Add("colNum");
            dtResults.Columns.Add("qURL_ID");
            
                XmlReader readXML = XmlReader.Create(URLString);

                rowInt = 1;
            colNum = 1;
                while (readXML.Read())
                {
                    switch (readXML.NodeType)
                    {

                        case XmlNodeType.Element:
                            readElementName(readXML,ref rowInt,ref colNum,  dtResults, qurl_ID);
                            break;

                        case XmlNodeType.EndElement:
                            removeFields(readXML, ref rowInt);
                            break;
                    }
                }
            return dtResults;
            }

            private static void readElementName(XmlReader xml, ref int rowInt, ref int colNum,  DataTable dtResults, int qURL_ID)
            {
            
                if (xml.Name == "datum")
                {

                    DataRow newRow = dtResults.NewRow();
                    newRow["qcolName"] = xml.Name;
                    newRow["qColType"] = getAttributes(xml);
                    xml.MoveToElement();
                    xml.Read();
                    newRow["qcolValue"] = xml.Value;
                    if (newRow["qColType"].ToString() == "array")
                    {
                        rowInt++;
                        colNum = 1;
                        newRow["colNum"] = colNum;
                    }
                    else
                    {
                        newRow["rowNum"] = rowInt;
                        newRow["colNum"] = colNum;
                        newRow["qURL_ID"] = qURL_ID;
                        dtResults.Rows.Add(newRow);
                        colNum = colNum + 1;
                    }
                }
            }

            //private void readElementValue(XmlReader xml, String Name)
            //{
            //    xml.Read();

            //}

            private static String getAttributes(XmlReader xml)
            {
                while (xml.MoveToNextAttribute())
                {
                    switch (xml.Name)
                    {
                        case "type":
                            //if type = array start a new rowNum at 0
                            return xml.Value;
                            break;
                        case "nil":
                            return "nil";
                    }
                }
                return "";
            }

            private static void removeFields(XmlReader xml, ref int rowInt)
            {
                switch (xml.Name)
                {
                    case "element":
                        {
                            //  rowInt = 1;
                        }

                        break;
                }
            }

        }
    }


