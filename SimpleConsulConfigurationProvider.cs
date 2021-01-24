using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ConsulApi
{
    public class ConsulConfigurationProvider : ConfigurationProvider
    {
        private const string ConsulIndexHeader = "X-Consul-Index"; //consul 的变更通知 最后一个索引配置的值

        private readonly string _path;
        private readonly HttpClient _httpClient;
        private readonly IReadOnlyList<Uri> _consulUrls;
        private readonly Task _configurationListeningTask;
        private int _consulUrlIndex;
        private int _failureCount;
        private int _consulConfigurationIndex;

        public ConsulConfigurationProvider(IEnumerable<Uri> consulUrls, string path)
        {
            _path = path;
            _consulUrls = consulUrls.Select(u => new Uri(u, $"v1/kv/{path}")).ToList();

            if (_consulUrls.Count <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(consulUrls));
            }

            _httpClient = new HttpClient(new HttpClientHandler { AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip }, true);
            _configurationListeningTask = new Task(ListenToConfigurationChanges);
        }

        public override void Load() => LoadAsync().ConfigureAwait(false).GetAwaiter().GetResult();

        private async Task LoadAsync()
        {
            Data = await ExecuteQueryAsync();

            if (_configurationListeningTask.Status == TaskStatus.Created)
                _configurationListeningTask.Start();
        }
        // consul 的变更通知
        private async void ListenToConfigurationChanges()
        {
            while (true)
            {
                try
                {
                    if (_failureCount > _consulUrls.Count)
                    {
                        _failureCount = 0;
                        await Task.Delay(TimeSpan.FromMinutes(1));
                    }

                    Data = await ExecuteQueryAsync(true);
                    OnReload();
                    _failureCount = 0;
                }
                catch (TaskCanceledException)
                {
                    _failureCount = 0;
                }
                catch
                {
                    _consulUrlIndex = (_consulUrlIndex + 1) % _consulUrls.Count;
                    _failureCount++;
                }
            }
        }

        private async Task<IDictionary<string, string>> ExecuteQueryAsync(bool isBlocking = false)
        {
            //?recurse=true以递归方式查询任何节点
            var requestUri = isBlocking ? $"?recurse=true&index={_consulConfigurationIndex}" : "?recurse=true";
            using (var request = new HttpRequestMessage(HttpMethod.Get, new Uri(_consulUrls[_consulUrlIndex], requestUri)))
            using (var response = await _httpClient.SendAsync(request))
            {
                response.EnsureSuccessStatusCode();
                if (response.Headers.Contains(ConsulIndexHeader))
                {
                    var indexValue = response.Headers.GetValues(ConsulIndexHeader).FirstOrDefault();
                    int.TryParse(indexValue, out _consulConfigurationIndex);
                }

                var tokens = JToken.Parse(await response.Content.ReadAsStringAsync());
                List<KeyValuePair<string, JToken>> pairs=null;
                Dictionary<string, string> retDic = null;
                //我这里实际只有一个token
                int tokenCount = tokens.Count();
                if (tokenCount == 1)
                {
                    string valueStr = tokens[0].Value<string>("Value");
                    JToken value = string.IsNullOrEmpty(valueStr) ? null : JToken.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(valueStr)));
                    pairs = new List<KeyValuePair<string, JToken>>(1);
                    pairs.Add(KeyValuePair.Create(string.Empty, value));
                   
                }
                else if (tokenCount > 1) {
                    pairs = tokens.Select(k => KeyValuePair.Create
                              (
                                  k.Value<string>("Key").Substring(_path.Length + 1),
                                  k.Value<string>("Value") != null ? JToken.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(k.Value<string>("Value")))) : null
                              ))
                         .Where(v => !string.IsNullOrWhiteSpace(v.Key)).ToList();
                        
                }
                if (pairs!=null)
                {
                    retDic= pairs.SelectMany(Flatten)
                         .ToDictionary(v => ConfigurationPath.Combine(v.Key.Split('/')), v => v.Value, StringComparer.OrdinalIgnoreCase);
                }
                return retDic;             
            }
        }

        // 使键值变平的方法是对树进行简单的深度优先搜索
        private static IEnumerable<KeyValuePair<string, string>> Flatten(KeyValuePair<string, JToken> tuple)
        {
            if (!(tuple.Value is JObject value))
                yield break;

            foreach (var property in value)
            {
                var propertyKey = $"{tuple.Key}/{property.Key}";
                if (string.IsNullOrEmpty(tuple.Key)) {
                    propertyKey = property.Key;
                }

                switch (property.Value.Type)
                {
                    case JTokenType.Object:
                        foreach (var item in Flatten(KeyValuePair.Create(propertyKey, property.Value)))
                            yield return item;
                        break;
                    case JTokenType.Array:
                        break;
                    default:
                        yield return KeyValuePair.Create(propertyKey, property.Value.Value<string>());
                        break;
                }
            }
        }
    }

    // 有了一个 ConfigurationProvider, 再写一个 ConfigurationSource 来创建 我们的 provide
    public class ConsulConfigurationSource : IConfigurationSource
    {
        public IEnumerable<Uri> ConsulUrls { get; }
        public string Path { get; }

        public ConsulConfigurationSource(IEnumerable<Uri> consulUrls, string path)
        {
            ConsulUrls = consulUrls;
            Path = path;
        }

        public IConfigurationProvider Build(IConfigurationBuilder builder)
        {
            return new ConsulConfigurationProvider(ConsulUrls, Path);
        }
    }
    // 扩展方法
    public static class ConsulConfigurationExtensions
    {
        public static IConfigurationBuilder AddConsul(this IConfigurationBuilder configurationBuilder, IEnumerable<Uri> consulUrls, string consulPath)
        {
            return configurationBuilder.Add(new ConsulConfigurationSource(consulUrls, consulPath));
        }

        public static IConfigurationBuilder AddConsul(this IConfigurationBuilder configurationBuilder, IEnumerable<string> consulUrls, string consulPath)
        {
            return configurationBuilder.AddConsul(consulUrls.Select(u => new Uri(u)), consulPath);
        }
    }

    public class MongodbHostOptions
    {
        public string Connection { get; set; }
        public string DataBase { get; set; }

        public string Table { get; set; }
    }
}
