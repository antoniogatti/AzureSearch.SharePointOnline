//THIS CODE IS PROVIDED AS IS WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABILITY, OR NON-INFRINGEMENT.
// Azure Search API documentation: https://docs.microsoft.com/en-us/rest/api/searchservice/index-2019-05-06-preview

using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using RestSharp;
using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AzureSearch.SharePointOnline.Connector.Helpers
{
    public class SearchServiceHelper
    {
        private string _clientKey;
        private string _clientUri;
        private readonly SearchServiceClient client;

        public SearchServiceHelper(string searchServiceName, string searchServiceAdminKey)
        {
            client = new SearchServiceClient(searchServiceName, new SearchCredentials(searchServiceAdminKey));
            client.HttpClient.DefaultRequestHeaders.Add("api-key", searchServiceAdminKey);
            _clientKey = searchServiceAdminKey;
            _clientUri = $"https://{client.SearchServiceName}.{client.SearchDnsSuffix}";
        }

        public async Task CreateOrUpdateBlobDataSourceAsync(
            string dataSourceName,
            string storageAccountName,
            string storageAccountKey,
            string storageContainerName)
        {
            Console.WriteLine($"Creating '{dataSourceName}' blob data source...");
            DataSource dt = new DataSource()
            {
                Name = dataSourceName,
                Type = "azureblob",
                Credentials = new DataSourceCredentials($"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey};"),
                Container = new DataContainer(storageContainerName) // In query param you can specify an optional virtual directory name
            };

            await client.DataSources.CreateOrUpdateAsync(dt);
        }

        [Obsolete]
        public async Task CreateOrUpdateCosmosDBDataSourceAsync(
            string dataSourceName,
            string cosmosDBConnectionString,
            string cosmosDbDatabaseName,
            string cosmosDBContainer)
        {
            Console.WriteLine($"Creating '{dataSourceName}' CosmosDB data source...");
            await client.DataSources.CreateOrUpdateAsync(DataSource.DocumentDb(
                name: dataSourceName,
                documentDbConnectionString: $"{cosmosDBConnectionString};Database={cosmosDbDatabaseName}",
                collectionName: cosmosDBContainer,
                useChangeDetection: true
            ));
        }

        public async Task DeleteDataSourceAsync(string dataSourceName)
        {
            Console.WriteLine($"Deleting '{dataSourceName}' data source...");
            await client.DataSources.DeleteAsync(dataSourceName);
        }

        public async Task CreateSynonymsMapFromJsonDefinitionAsync(string synonymMapName, string synonymMapDefinitionPath)
        {
            Console.WriteLine($"Creating '{synonymMapName}' synonym map with '{synonymMapDefinitionPath}'...");
            using (StreamReader reader = new StreamReader(synonymMapDefinitionPath))
            {
                var uri = $"{_clientUri}/synonymmaps/?api-version={client.ApiVersion}";

                var json = reader.ReadToEnd();
                json = json.Replace("[SynonymMapName]", synonymMapName);

                var request = new RestRequest(Method.POST);
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("api-key", _clientKey);
                request.AddParameter("asd", json, ParameterType.RequestBody);

                var response = await new RestClient(uri).ExecuteAsync(request);

                ProcessStatusCode(response);
            }
        }

        public async Task DeleteSynonymMapAsync(string synonymMapName)
        {
            Console.WriteLine($"Deleting '{synonymMapName}' synonym map...");

            var uri = $"{_clientUri}/synonymmaps/{synonymMapName}?api-version={client.ApiVersion}";

            var request = new RestRequest(Method.DELETE);
            request.AddHeader("api-key", _clientKey);

            var response = await new RestClient(uri).ExecuteAsync(request);
            Console.WriteLine($"Deleting '{synonymMapName}' results: {response.StatusCode},{response.Content}");
        }

        public async Task CreateIndexFromJsonDefinitionAsync(string indexName, string indexDefinitionPath, string synonymMapName)
        {
            Console.WriteLine($"Creating '{indexName}' index with '{indexDefinitionPath}'...");
            using (StreamReader reader = new StreamReader(indexDefinitionPath))
            {
                var uri = $"{_clientUri}/indexes/{indexName}?api-version={client.ApiVersion}";
                var json = reader.ReadToEnd();
                //json = json.Replace("[IndexName]", indexName);
                json = json.Replace("[SynonymMapName]", synonymMapName);

                var request = new RestRequest(Method.PUT);
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("api-key", _clientKey);
                request.AddParameter("asd", json, ParameterType.RequestBody);

                var response = await new RestClient(uri).ExecuteAsync(request);
                ProcessStatusCode(response);
            }
        }

        public async Task DeleteIndexAsync(string indexName)
        {
            Console.WriteLine($"Deleting '{indexName}' index...");

            var uri = $"{_clientUri}/indexes/{indexName}?api-version={client.ApiVersion}";

            var request = new RestRequest(Method.DELETE);
            request.AddHeader("api-key", _clientKey);

            var response = await new RestClient(uri).ExecuteAsync(request);
            Console.WriteLine($"Deleting '{indexName}' results: {response.StatusCode},{response.Content}");
        }

        public async Task CreateSkillsetFromJsonDefinitionAsync(string skillsetName, string skillsetDefinitionPath, string cognitiveKey, string cognitiveAccount, string customSpoMetadataSkillUri, string spoMetadataMapperApiKey)
        {
            Console.WriteLine($"Creating '{skillsetName}' skillset with '{skillsetDefinitionPath}'...");
            using (StreamReader reader = new StreamReader(skillsetDefinitionPath))
            {
                var uri = $"{_clientUri}/skillsets/{skillsetName}?api-version={client.ApiVersion}";
                var json = reader.ReadToEnd();
                json = json.Replace("[CognitiveServicesAccount]", cognitiveAccount);
                json = json.Replace("[CognitiveServicesKey]", cognitiveKey);
                json = json.Replace("[CustomSpoMetadataSkillUri]", customSpoMetadataSkillUri);
                json = json.Replace("[SPOMetadataMapper-Api-Key]", spoMetadataMapperApiKey);

                var request = new RestRequest(Method.PUT);
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("api-key", _clientKey);
                request.AddParameter("asd", json, ParameterType.RequestBody);

                var response = await new RestClient(uri).ExecuteAsync(request);
                ProcessStatusCode(response);
            }
        }

        public async Task DeleteSkillsetAsync(string skillsetName)
        {
            Console.WriteLine($"Deleting '{skillsetName}' skillset...");
            var uri = $"{_clientUri}/skillsets/{skillsetName}?api-version={client.ApiVersion}";

            var request = new RestRequest(Method.DELETE);
            request.AddHeader("api-key", _clientKey);

            var response = await new RestClient(uri).ExecuteAsync(request);
            Console.WriteLine($"Deleting '{skillsetName}' results: {response.StatusCode},{response.Content}");
        }

        public async Task CreateIndexerFromJsonDefinitionAsync(string indexerName, string indexerDefinitionPath, string dataSourceName, string indexName, string skillsetName)
        {
            Console.WriteLine($"Creating '{indexerName}' indexer with '{indexerDefinitionPath}'...");
            using (StreamReader reader = new StreamReader(indexerDefinitionPath))
            {
                var uri = $"{_clientUri}/indexers/{indexerName}?api-version={client.ApiVersion}";
                var json = reader.ReadToEnd();

                json = json.Replace("[IndexerName]", indexerName);
                json = json.Replace("[DataSourceName]", dataSourceName);
                json = json.Replace("[IndexName]", indexName);
                json = json.Replace("[SkillSetName]", skillsetName);

                var request = new RestRequest(Method.PUT);
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("api-key", _clientKey);
                request.AddParameter("asd", json, ParameterType.RequestBody);

                var response = await new RestClient(uri).ExecuteAsync(request);
                ProcessStatusCode(response);
            }
        }

        public async Task CreateIndexerAsync(string indexerName, string dataSourceName, string indexName)
        {
            Console.WriteLine($"Creating '{indexerName}' indexer...");
            await client.Indexers.CreateAsync(new Indexer(
                name: indexerName,
                dataSourceName: dataSourceName,
                targetIndexName: indexName
            ));
        }

        public async Task DeleteIndexerAsync(string indexerName)
        {
            Console.WriteLine($"Deleting '{indexerName}' indexer...");

            var uri = $"{_clientUri}/indexers/{indexerName}?api-version={client.ApiVersion}";

            var request = new RestRequest(Method.DELETE);
            request.AddHeader("api-key", _clientKey);

            var response = await new RestClient(uri).ExecuteAsync(request);
            Console.WriteLine($"Deleting '{indexerName}' results: {response.StatusCode},{response.Content}");
        }

        public async Task WaitForIndexerToFinishAsync(string indexerName, int delaySecs = 60)
        {
            IndexerExecutionInfo info;

            do
            {
                Console.WriteLine($"   Waiting {delaySecs} seconds...");
                await Task.Delay(delaySecs * 1000);
                Console.WriteLine($"   Getting indexer status...");
                info = await client.Indexers.GetStatusAsync(indexerName);
                Console.WriteLine($"   ...Indexer status: {info.Status}, Indexer Execution Status: {info.LastResult?.Status}.");
            } while (
                info.Status == IndexerStatus.Running
                && (info.LastResult == null || info.LastResult.Status == IndexerExecutionStatus.InProgress));

            if (info.Status == IndexerStatus.Running && info.LastResult?.Status == IndexerExecutionStatus.Success)
            {
                Console.WriteLine($"...Indexer '{indexerName}' created successfully.");
            }
            else
            {
                Console.WriteLine($"...Failed to create '{indexerName}' indexer.");
                Console.WriteLine($"   Error: '{info.LastResult.ErrorMessage}'");
            }

            foreach (var warning in info.LastResult?.Warnings)
            {
                Console.WriteLine("===========================================================================");
                Console.WriteLine($"   Warning for '{warning.Key}': '{warning.Message}'");
            }

            foreach (var error in info.LastResult?.Errors)
            {
                Console.WriteLine("===========================================================================");
                Console.WriteLine($"   Error for '{error.Key}': '{error.ErrorMessage}'");
            }
            Console.WriteLine("===========================================================================");
        }

        private void ProcessStatusCode(IRestResponse response)
        {
            switch (response.StatusCode)
            {
                case HttpStatusCode.Accepted:
                case HttpStatusCode.Continue:
                case HttpStatusCode.Created:
                case HttpStatusCode.OK:
                case HttpStatusCode.MultiStatus:
                    break;

                default:
                    throw new Exception(response.ErrorMessage);
            }
        }
    }
}
