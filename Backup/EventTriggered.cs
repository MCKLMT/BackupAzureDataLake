// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
// https://45eec682f01b.ngrok.io/runtime/webhooks/EventGrid?functionName=FileRenamed


using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Files.DataLake;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Backup
{
    public static class EventTriggered
    {
        private static string BLOB_OUTPUT_CONNECTION_STRING = Environment.GetEnvironmentVariable("AzureWebJobsStorageOutput");

        [FunctionName("FileCreated")]
        public static async Task OnFileCreated(
            [EventGridTrigger] EventGridEvent eventGridEvent,
            [Blob("{data.url}", FileAccess.ReadWrite, Connection = "AzureWebJobsStorageInput")] Stream input,
            ILogger log)
        {
            try
            {
                if (input != null)
                {
                    var createdEvent = ((JObject)eventGridEvent.Data).ToObject<StorageBlobCreatedEventData>();

                    var blobName = GetBlobPathFromUrl(createdEvent.Url);

                    DataLakeFileClient dataLakeFileClient = new DataLakeFileClient(BLOB_OUTPUT_CONNECTION_STRING, "backup", blobName);
                    await dataLakeFileClient.UploadAsync(input, overwrite: true);
                }
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message);
                throw;
            }
        }

        [FunctionName("FileRenamed")]
        public static async Task OnFileRenamed([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            try
            {
                var createdEvent = ((JObject)eventGridEvent.Data).ToObject<StorageBlobRenamedEventData>();

                Uri sourceFileUri = new Uri(createdEvent.SourceBlobUrl);

                DataLakeFileClient dataLakeFileClient = new DataLakeFileClient(BLOB_OUTPUT_CONNECTION_STRING, "backup", sourceFileUri.AbsolutePath);

                Uri destinationFileUri = new Uri(createdEvent.DestinationBlobUrl);

                await dataLakeFileClient.RenameAsync(destinationFileUri.AbsolutePath, "backup");
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message);
                throw;
            }
        }

        [FunctionName("FileDeleted")]
        public static async Task OnFileDeleted([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            try
            {
                var createdEvent = ((JObject)eventGridEvent.Data).ToObject<StorageBlobDeletedEventData>();

                DataLakeServiceClient serviceClient = new DataLakeServiceClient(BLOB_OUTPUT_CONNECTION_STRING);

                var fileSystemClient = serviceClient.GetFileSystemClient("backup");

                var fileName = GetBlobPathFromUrl(createdEvent.Url);


                await fileSystemClient.DeleteIfExistsAsync();
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message);
                throw;
            }
        }

        [FunctionName("DirectoryCreated")]
        public static async Task OnDirectoryCreated([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            try
            {
                var createdEvent = ((JObject)eventGridEvent.Data).ToObject<StorageBlobCreatedEventData>();

                DataLakeServiceClient serviceClient = new DataLakeServiceClient(BLOB_OUTPUT_CONNECTION_STRING);

                var fileSystemClient = serviceClient.GetFileSystemClient("backup");

                var directoryName = GetBlobPathFromUrl(createdEvent.Url);

                await fileSystemClient.CreateDirectoryAsync(directoryName);
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message);
                throw;
            }
        }

        [FunctionName("DirectoryRenamed")]
        public static async Task OnDirectoryRenamed([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            try
            {
                var createdEvent = ((JObject)eventGridEvent.Data).ToObject<StorageBlobRenamedEventData>();

                Uri sourceFileUri = new Uri(createdEvent.SourceBlobUrl);

                DataLakeDirectoryClient dataLakeFileClient = new DataLakeDirectoryClient(BLOB_OUTPUT_CONNECTION_STRING, "backup", sourceFileUri.AbsolutePath);

                Uri destinationFileUri = new Uri(createdEvent.DestinationBlobUrl);

                await dataLakeFileClient.RenameAsync(destinationFileUri.AbsolutePath, "backup");
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message);
                throw;
            }
        }

        [FunctionName("DirectoryDeleted")]
        public static async Task OnDirectoryDeleted([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            try
            {
                var createdEvent = ((JObject)eventGridEvent.Data).ToObject<StorageBlobDeletedEventData>();

                DataLakeServiceClient serviceClient = new DataLakeServiceClient(BLOB_OUTPUT_CONNECTION_STRING);

                var fileSystemClient = serviceClient.GetFileSystemClient("backup");

                var directoryName = GetBlobPathFromUrl(createdEvent.Url);

                await fileSystemClient.DeleteDirectoryAsync(directoryName);
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message);
                throw;
            }
        }

        private static string GetBlobPathFromUrl(string bloblUrl)
        {
            var uri = new Uri(bloblUrl);
            return uri.AbsolutePath;
        }
    }

    public class StorageBlobRenamedEventData
    {
        [JsonProperty(PropertyName = "api")]
        public string Api
        {
            get;
            set;
        }

        [JsonProperty(PropertyName = "requestId")]
        public string RequestId
        {
            get;
            set;
        }

        [JsonProperty(PropertyName = "destinationBlobUrl")]
        public string DestinationBlobUrl
        {
            get;
            set;
        }

        [JsonProperty(PropertyName = "sourceBlobUrl")]
        public string SourceBlobUrl
        {
            get;
            set;
        }

        [JsonProperty(PropertyName = "destinationUrl")]
        public string DestinationUrl
        {
            get;
            set;
        }

        [JsonProperty(PropertyName = "sourceUrl")]
        public string SourceUrl
        {
            get;
            set;
        }

        [JsonProperty(PropertyName = "sequencer")]
        public string Sequencer
        {
            get;
            set;
        }

        [JsonProperty(PropertyName = "storageDiagnostics")]
        public object StorageDiagnostics
        {
            get;
            set;
        }

        public StorageBlobRenamedEventData() { }
    }
}
