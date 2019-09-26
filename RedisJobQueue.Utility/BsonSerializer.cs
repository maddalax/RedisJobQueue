using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace RedisJobQueue.Utility
{
    public static class BsonSerializer
    {
        public static byte[] ToBson<T>(T value)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BsonDataWriter(ms))
            {
                var serializer = new JsonSerializer();
                serializer.Serialize(writer, value);
                return ms.ToArray();
            }
        }

        public static T FromBson<T>(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            using (var reader = new BsonDataReader(ms))
            {
                var serializer = new JsonSerializer();
                return serializer.Deserialize<T>(reader);
            }
        }
    }
}