using Microsoft.VisualBasic.FileIO;
using SalesForce_DataArchival.Logging;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Web.Mvc;

namespace SalesForce_DataArchival
{
    public class DBOperations : Controller
    {
        public static string connectionString = ConfigurationManager.ConnectionStrings["SF_Connection"].ConnectionString;
        
        public  void ConvertCSVToDataTable(string fileName, string tableName)
        {
            try
            {
                var table = new DataTable();
                using (TextFieldParser MyReader = new TextFieldParser(fileName))
                {
                    MyReader.TextFieldType = FieldType.Delimited;
                    MyReader.SetDelimiters(",");
                    MyReader.HasFieldsEnclosedInQuotes = true;
                    string[] currentRow;
                    string header = MyReader.ReadLine();
                    var fields = header.Split(',');
                    foreach (string column in fields)
                    {
                        // add columns to new datatable based on first row of csv
                        table.Columns.Add(column);
                    }
             
                    while (!MyReader.EndOfData)
                    {
                        DataRow row = table.NewRow();
                        currentRow = MyReader.ReadFields();
                        for (int i = 0; i < currentRow.Length; i++)
                        {
                            row[i] = currentRow[i];
                        }
                        table.Rows.Add(row);
                    }
                }
               
                using (var sqlConnection = new SqlConnection(connectionString))
                {
                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(sqlConnection))
                    {
                        sqlConnection.Open();
                        sqlBulkCopy.DestinationTableName = "dbo." + tableName;
                        sqlBulkCopy.WriteToServer(table);
                        int rowsCount = table.Rows.Count;
                        LogWriter.Write("The data was inserted successfully for Object " + tableName + "\n" + "Number of records inserted: " + rowsCount);
                        sqlConnection.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                LogWriter.Write(ex.Message);
                throw ex.InnerException;
            }
        }

        public string GetLatestModifiedDate(string objectName)
        {
            string query= "select max(LastModifiedDate) as LastModifiedDate from " + objectName;
            string LastModifiedDate = "";
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(query, sqlConnection))
                {
                    sqlConnection.Open();
                    SqlDataReader rdr = command.ExecuteReader();
                    while (rdr.Read())
                    {
                        LastModifiedDate = rdr["LastModifiedDate"].ToString();

                    }
                    //when the data is been inserted for the first time
                    if (LastModifiedDate == "" || LastModifiedDate == null)
                    {
                        LastModifiedDate = "NO_LATEST_DATE_FOUND";
                    }
                }
                
            }
            return LastModifiedDate;
        }

    }

}

