using Newtonsoft.Json.Linq;
using SalesForce_DataArchival.Logging;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Mvc;
using static SalesForce.Salesforcemodel;

namespace SalesForce_DataArchival
{
    public class BusinessLogic : Controller
    {
        public static List<AccessToken> ListContents;
        public static string connectionString = ConfigurationManager.ConnectionStrings["SF_Connection"].ConnectionString;
        public static string csvFile = ConfigurationManager.AppSettings["CSVFilePath"];
        JsonResult Jobject = new JsonResult();
        Dictionary<string, string> dict = new Dictionary<string, string>();
        List<string> OtherThancomplete = new List<string>();
        List<string> ObjectInfo = new List<string>();
        public async void DataArchival_CreateJobByUsingUpsert(List<AccessToken> ListContents)
        {
            string RequestQuery = "";

            try
            {
                // public string LatestModifiedDate = "";

                string[] RequestData = System.IO.File.ReadAllLines(ConfigurationManager.AppSettings["RequestDataFilePath"]);
                for (int i = 1; i < RequestData.Length; i = i + 2)
                {
                    string JobIdVal = string.Empty;
                    string jobinfoResponse = string.Empty;
                    HttpClient client = new HttpClient();
                    JObject createjob = new JObject();
                    createjob.Add("operation", "query");
                    //db call to get the latest modified date for a particular object

                    DBOperations dbobj = new DBOperations();
                    string LatestDate = dbobj.GetLatestModifiedDate(RequestData[i - 1]);
                    //if the latestDate is returned as null, that means there is no data in the table or it is the first time we are inserting the data
                    if (LatestDate.Equals("NO_LATEST_DATE_FOUND"))
                    {
                        RequestQuery = RequestData[i];
                    }
                    else
                    {
                        RequestQuery = RequestData[i] + " where LastModifiedDate > " + LatestDate;
                    }

                    //If the value is returned null ie first time, handle and insert all records
                    createjob.Add("query", RequestQuery);
                    HttpContent contentCreate = new StringContent(createjob.ToString(), Encoding.UTF8, "application/json"); //1.Createt Job
                    string uri = $"{ConfigurationManager.AppSettings["jobCreatePoint"]}{ConfigurationManager.AppSettings["queryJobApiEndpoint"]}";
                    HttpRequestMessage requestCreate = new HttpRequestMessage(HttpMethod.Post, uri);
                    requestCreate.Headers.Add("Authorization", "Bearer " + ListContents[0].Access_Token);
                    requestCreate.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
                    requestCreate.Content = contentCreate;

                    HttpResponseMessage response = client.SendAsync(requestCreate).Result;
                    //here we need to verify if the accesstoken is valid or not from the job response
                    string StatusCode = response.StatusCode.ToString();
                    var jobOpenResponse = response.Content.ReadAsStringAsync().Result;
                    if (jobOpenResponse.Contains("Session expired or invalid") == true)
                    {
                        LogWriter.Write("Access token expired..A new access token is been generated...");
                        string NewAccessToken = TokenGeneration.GetUpdatedAccessToken(ListContents[0].Refresh_Token, ListContents[0].Client_Id, ListContents[0].Client_Secret);
                        //update the new access token in db
                        string status = TokenGeneration.updateNewAccessToken(NewAccessToken, ListContents[0].Refresh_Token);
                        if (status.Equals("Success"))
                        {
                            LogWriter.Write("New Access Token was updated to the DB");
                            DataArchival_CreateJobByUsingUpsert(ListContents);
                        }
                    }
                    jobinfoResponse = StatusCode;
                    JObject jobinfoStatus = JObject.Parse(jobOpenResponse);
                    JobIdVal = (string)jobinfoStatus["id"];
                    //verify if the job is ready 
                    //checkJobStatus(JobIdVal, ListContents);
                    string jobStatus = checkJobStatus2(JobIdVal, ListContents);
                    if (jobStatus.Equals("Pending"))
                    {
                        ObjectInfo.Add(RequestData[i - 1]);
                        continue;
                    }
                    // else
                    //{
                    await PopulateData(JobIdVal, ListContents, RequestData[i - 1]);
                    /*  if (jobinfoResponse == "OK")
                      {

                          BusinessLogic blObj = new BusinessLogic();
                          JsonResult csvData = blObj.DataArchival_JobinfoInsert(JobIdVal, ListContents[0].Access_Token);
                          using (StreamWriter writer = new StreamWriter(csvFile))
                          {
                              writer.Write(csvData.Data.ToString());

                          }
                          DBOperations dbObj = new DBOperations();
                          dbObj.ConvertCSVToDataTable(csvFile, RequestData[i - 1]);

                      }
                    */
                    //}
                    /*    if (jobinfoResponse == "OK")
                        {

                            BusinessLogic blObj = new BusinessLogic();
                            JsonResult csvData = blObj.DataArchival_JobinfoInsert(JobIdVal, ListContents[0].Access_Token);
                            using (StreamWriter writer = new StreamWriter(csvFile))
                            {
                                writer.Write(csvData.Data.ToString());

                            }
                            DBOperations dbObj = new DBOperations();
                            dbObj.ConvertCSVToDataTable(csvFile, RequestData[i - 1]);

                        }*/
                }
                ReadListData(OtherThancomplete, ListContents);


            }
            catch (Exception ex)
            {
                LogWriter.Write(ex.Message);
                throw ex.InnerException;
            }


        }

        private async Task PopulateData(string JobIdVal, List<AccessToken> ListContents, string RequestData)
        {

            BusinessLogic blObj = new BusinessLogic();
            JsonResult csvData = blObj.DataArchival_JobinfoInsert(JobIdVal, ListContents[0].Access_Token);
            using (StreamWriter writer = new StreamWriter(csvFile))
            {
                writer.Write(csvData.Data.ToString());

            }
            DBOperations dbObj = new DBOperations();
            dbObj.ConvertCSVToDataTable(csvFile, RequestData);


        }

        private void checkJobStatus(string jobId, List<AccessToken> ListContents)
        {
            HttpClient client = new HttpClient();
            string uri = $"{ConfigurationManager.AppSettings["jobCreatePoint"]}{ConfigurationManager.AppSettings["queryJobApiEndpoint"]}{"/"}{jobId}";
            HttpRequestMessage requestCreate = new HttpRequestMessage(HttpMethod.Get, uri);
            requestCreate.Headers.Add("Authorization", "Bearer " + ListContents[0].Access_Token);
            requestCreate.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
            HttpResponseMessage response = client.SendAsync(requestCreate).Result;
            string StatusCode = response.StatusCode.ToString();
            var jobOpenResponse = response.Content.ReadAsStringAsync().Result;
            JObject jobinfoStatus = JObject.Parse(jobOpenResponse);
            string JobStatus = (string)jobinfoStatus["state"];
            if (!JobStatus.Equals("JobComplete"))
            {
                checkJobStatus(jobId, ListContents);
            }

        }

        private string checkJobStatus2(string jobId, List<AccessToken> ListContents)
        {

            HttpClient client = new HttpClient();
            string uri = $"{ConfigurationManager.AppSettings["jobCreatePoint"]}{ConfigurationManager.AppSettings["queryJobApiEndpoint"]}{"/"}{jobId}";
            HttpRequestMessage requestCreate = new HttpRequestMessage(HttpMethod.Get, uri);
            requestCreate.Headers.Add("Authorization", "Bearer " + ListContents[0].Access_Token);
            requestCreate.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
            HttpResponseMessage response = client.SendAsync(requestCreate).Result;
            string StatusCode = response.StatusCode.ToString();
            var jobOpenResponse = response.Content.ReadAsStringAsync().Result;
            JObject jobinfoStatus = JObject.Parse(jobOpenResponse);
            string JobStatus = (string)jobinfoStatus["state"];
            if ((!JobStatus.Equals("JobComplete")))
            {
                if(!(OtherThancomplete.Contains(jobId)))
                {
                    OtherThancomplete.Add(jobId);
                }
                //checkJobStatus(jobId, ListContents);
                
                // OtherThancomplete.Add(RequestData);
                return "Pending";
            }
            else
            {
                return "Complete";
            }

        }

        private async void ReadListData(List<string> otherThancomplete, List<AccessToken> ListContents)
        {
            for (int i = 0; i < otherThancomplete.Count;)
            {
                string status = checkJobStatus2(otherThancomplete[0], ListContents);
                if (status.Equals("Complete"))
                {
                    await PopulateData(otherThancomplete[0], ListContents, ObjectInfo[0]);
                    ObjectInfo.RemoveAt(0);
                    otherThancomplete.RemoveAt(0);
                }
                else
                {
                    ReadListData(otherThancomplete,ListContents);
                }
                
              
            }
        }



        public JsonResult DataArchival_JobinfoInsert(string jobId, string authToken)
        {

            try
            {
                string returnResponse = string.Empty;
                string endPoint = System.Configuration.ConfigurationManager.AppSettings["jobCreatePoint"];
                HttpClient client = new HttpClient();
                string contentUrl = endPoint + "/services/data/v53.0/jobs/query/" + jobId + "/results";
                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, contentUrl);
                request.Headers.Add("Authorization", "Bearer " + authToken);
                //Thread.Sleep(5000);
                HttpResponseMessage response = client.SendAsync(request).Result;
                //if 204 error is returned, this set of lines need to be executed
                // if (response.StatusCode == System.Net.HttpStatusCode.NoContent)
                //{
                //  LogWriter.Write("StatusCode: " + response.StatusCode + "Was returned..Trying to fetch the CSV response Again...");
                //return Jobject = DataArchival_JobinfoInsertAgain(jobId, authToken);

                //}
                //else
                //{
                return Jobject = jData(response);
                //}

            }
            catch (Exception ex)
            {

                throw ex.InnerException;
            }

        }

        public JsonResult jData(HttpResponseMessage response)
        {
            string returnResponse = string.Empty;
            returnResponse = response.Content.ReadAsStringAsync().Result;

            return Json(returnResponse, JsonRequestBehavior.AllowGet);
        }
    }
}
