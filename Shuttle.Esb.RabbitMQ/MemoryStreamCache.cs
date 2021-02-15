#if NETSTANDARD2_1
using Microsoft.IO;

namespace Shuttle.Esb.RabbitMQ
{
    internal static class MemoryStreamCache
    {
        public static readonly RecyclableMemoryStreamManager Manager = new RecyclableMemoryStreamManager();
    }
}
#endif