using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;


namespace OpenTable_SyncData.Data
{
    public class TableManager
    {
        private CloudTable table;
        private static IConfiguration _configuration;

        public TableManager(string TableName, IConfiguration configuration)
        {
            _configuration = configuration;
            // Check if Table Name is blank
            if (string.IsNullOrEmpty(TableName))
            {
                throw new ArgumentNullException("Table", "Table Name can't be empty");
            }
            try
            {
                // Get azure table storage connection string.
                string ConnectionString = _configuration["AzureStorageConnection"];

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConnectionString);

                // Create the table if not exist and put the refarence of the table into Cloud Table object
                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                table = tableClient.GetTableReference(TableName);
                table.CreateIfNotExistsAsync();
            }
            catch (Exception ExceptionObj)
            {
                throw ExceptionObj;
            }


        }
        public TableManager(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        /// <summary>
        /// This method is used to get record from azure table storage.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Query">This is query to fetch specific record from table.</param>
        /// <returns></returns>
        public async Task<List<T>> RetrieveEntityAsync<T>(string Query = null) where T : TableEntity, new()
        {
            try
            {
                TableQuery<T> DataTableQuery = new TableQuery<T>();
                if (!string.IsNullOrEmpty(Query))
                {
                    DataTableQuery = new TableQuery<T>().Where(Query);
                }
                TableContinuationToken continuationToken = null;
                List<T> DataList = new List<T>();
                //IEnumerable<T> IDataList = await table.ExecuteQuerySegmentedAsync(DataTableQuery, continuationToken);
                //foreach (var singleData in IDataList)
                //    DataList.Add(singleData);
                do
                {
                    var IDataList = await table.ExecuteQuerySegmentedAsync(DataTableQuery, continuationToken);
                    DataList.AddRange(IDataList.Results);
                    continuationToken = IDataList.ContinuationToken;
                } while (continuationToken != null);
                return DataList;
            }
            catch (Exception ExceptionObj)
            {
                throw ExceptionObj;
            }
        }
        /// <summary>
        /// This method is used to get last update record from azure table.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Query">This is query to fetch specific record from table.</param>
        /// <returns></returns>
        public T RetrieveLastRecord<T>(string Query = null) where T : TableEntity, new()
        {
            try
            {
                TableQuery<T> DataTableQuery = new TableQuery<T>();
                if (!string.IsNullOrEmpty(Query))
                {
                    DataTableQuery = new TableQuery<T>().Where(Query);
                }
                TableContinuationToken continuationToken = null;
                var Data = table.ExecuteQuerySegmentedAsync(DataTableQuery, continuationToken).Result.OrderBy(x => x.RowKey).LastOrDefault(); ;
                return Data;
            }
            catch (Exception ExceptionObj)
            {
                throw ExceptionObj;
            }
        }
        /// <summary>
        /// This method is used to get notifications record from azure table storage.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Query">This is query to fetch specific record from table.</param>
        /// <param name="continuationToken">This is token provided by azure storage to fetch next record.</param>
        /// <returns></returns>
        public async Task<TableQuerySegment<T>> RetrieveNotification<T>(string Query = null, TableContinuationToken continuationToken = null) where T : TableEntity, new()
        {
            try
            {
                int maxEntitiesToFetch = 500;
                TableQuery<T> DataTableQuery = new TableQuery<T>();
                if (!string.IsNullOrEmpty(Query))
                {
                    DataTableQuery = new TableQuery<T>().Where(Query).Take(maxEntitiesToFetch);
                }

                var Data = await table.ExecuteQuerySegmentedAsync(DataTableQuery, continuationToken);
                return Data;
            }
            catch (Exception ExceptionObj)
            {
                throw ExceptionObj;
            }
        }
        /// <summary>
        /// This method is used to get last updated vital record from azure table storage.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Query">This is query to fetch specific record from table.</param>
        /// <returns></returns>
        public T RetrieveVital<T>(string Query = null) where T : TableEntity, new()
        {
            try
            {
                TableQuery<T> DataTableQuery = new TableQuery<T>();
                if (!string.IsNullOrEmpty(Query))
                {
                    DataTableQuery = new TableQuery<T>().Where(Query);
                }
                TableContinuationToken continuationToken = null;
                var data = table.ExecuteQuerySegmentedAsync(DataTableQuery, continuationToken).Result.OrderBy(x => x.RowKey).LastOrDefault();
                return data;
            }
            catch (Exception ExceptionObj)
            {
                throw ExceptionObj;
            }
        }

        public static CloudTable AuthTable(string tableName)
        {
            string accountName = _configuration["AzureTableName"];
            string accountKey = _configuration["AzureTableKey"];
            try
            {
                StorageCredentials creds = new StorageCredentials(accountName, accountKey);
                CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);

                CloudTableClient client = account.CreateCloudTableClient();

                CloudTable table = client.GetTableReference(tableName);

                return table;
            }
            catch
            {
                return null;
            }
        }
        public async Task InsertRecord<T>(List<T> lst, string tableName)
        {
            try
            {
                TableBatchOperation batchOperationObj = new TableBatchOperation();
                CloudTable tbl = TableManager.AuthTable(tableName);

                if (lst != null && lst.Count() > 0)
                {
                    await tbl.CreateIfNotExistsAsync();
                    foreach (var item in lst)
                    {
                        batchOperationObj.InsertOrReplace((ITableEntity)item);
                    }
                    var response = await tbl.ExecuteBatchAsync(batchOperationObj);
                }
            }
            catch (Exception ExceptionObj)
            {
                throw ExceptionObj;

            }

        }

        public async Task InsertRecord<T>(T item, string tableName)
        {
            try
            {
                TableBatchOperation batchOperationObj = new TableBatchOperation();
                CloudTable tbl = TableManager.AuthTable(tableName);

                if (item != null)
                {
                    await tbl.CreateIfNotExistsAsync();
                    batchOperationObj.InsertOrReplace((ITableEntity)item);
                    var response = await tbl.ExecuteBatchAsync(batchOperationObj);
                }
            }
            catch (Exception ExceptionObj)
            {
                throw ExceptionObj;
            }
        }

        internal async Task InsertGroupingRecord<T>(List<IGrouping<string, T>> list, string tableName)
        {
            try
            {
                CloudTable tbl = TableManager.AuthTable(tableName);
                if (list != null && list.Count() > 0)
                {
                    await tbl.CreateIfNotExistsAsync();
                    foreach (var group in list)
                    {
                        TableBatchOperation batchOperationObj = new TableBatchOperation();
                        foreach (var item in group)
                        {
                            batchOperationObj.InsertOrReplace((ITableEntity)item);
                        }
                        var response = await tbl.ExecuteBatchAsync(batchOperationObj);
                    }

                }
            }
            catch (Exception ExceptionObj)
            {

                throw ExceptionObj;
            }
        }
        public async Task RemoveEntityByPartitionKey<T>(List<string> lstPartitionKey, string tableName) where T : TableEntity, new()
        {
            try
            {
                CloudTable table = TableManager.AuthTable(tableName);
                foreach (var PartitionKey in lstPartitionKey)
                {
                    TableQuery<T> query = new TableQuery<T>()
                   .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, PartitionKey));
                    try
                    {
                        foreach (var item in await table.ExecuteQuerySegmentedAsync(query, null))
                        {
                            var oper = TableOperation.Delete(item);
                            await table.ExecuteAsync(oper);
                        }
                    }
                    catch (Exception)
                    {


                    }

                }
            }
            catch (Exception ExceptionObj)
            {
                throw ExceptionObj;
            }
        }
    }
}