using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using System.Threading.Tasks;

namespace DataLakeStorePlayground
{
    class Program
    {
        const int iterations = 100;

        static async Task Main(string[] args)
        {
            try
            {
                long totalReadMs = 0;
                long totalWriteMs = 0;
                ServiceClientCredentials credentials = await GetCredentials("5818bd20-bf25-47b1-b996-d419d7e6e8ba", "392fe526-dbc0-46a4-b7fb-dd1f1ec3e7b5", "CUfsbfPQaZXm4AfTFVlUwZzFkaE/Jx5lz7EN4XrP5HU=");
                AdlsClient client = AdlsClient.CreateClient(@"erini.azuredatalakestore.net", credentials);
                byte[] content = new byte[1024];
                new Random().NextBytes(content);
                var stopwatch = new Stopwatch();

                for (int i = 0; i < iterations; i++)
                {
                    var fileName = $"sample-{i}.json";
                    Console.WriteLine(fileName);

                    stopwatch.Start();
                    using (var stream = await client.CreateFileAsync(fileName, IfExists.Overwrite))
                    {
                        await stream.WriteAsync(content, 0, content.Length);
                    }
                    stopwatch.Stop();
                    Console.WriteLine($"Wrote file '{fileName}' in {stopwatch.ElapsedMilliseconds}ms.");
                    totalWriteMs += stopwatch.ElapsedMilliseconds;

                    stopwatch.Reset();

                    stopwatch.Start();
                    using (var readStream = new StreamReader(await client.GetReadStreamAsync(fileName)))
                    {
                        while (await readStream.ReadLineAsync() != null) { }
                    }
                    stopwatch.Stop();
                    Console.WriteLine($"Read file '{fileName}' in {stopwatch.ElapsedMilliseconds}ms.");
                    totalReadMs += stopwatch.ElapsedMilliseconds;
                }

                Console.WriteLine($"Average write is {totalWriteMs / iterations}ms.");
                Console.WriteLine($"Average read is {totalReadMs / iterations}ms.");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            Console.ReadKey();
        }

        private static Task<ServiceClientCredentials> GetCredentials(string tenant, string clientId, string secretKey)
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

            var serviceSettings = ActiveDirectoryServiceSettings.Azure;
            serviceSettings.TokenAudience = new System.Uri(@"https://management.core.windows.net/");

            return ApplicationTokenProvider.LoginSilentAsync(tenant, clientId, secretKey, serviceSettings);
        }
    }
}
