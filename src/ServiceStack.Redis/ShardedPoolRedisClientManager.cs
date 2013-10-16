using System.Collections.Generic;
using ServiceStack.Caching;

namespace ServiceStack.Redis
{
    /// <summary>
    /// Provides a pool of redis connections to each shard in a cluster given a shardKey.
    /// </summary>
    public class ShardedPoolRedisClientManager
    {
        private readonly Dictionary<string, PooledRedisClientManager> _shardedPool = new Dictionary<string, PooledRedisClientManager>();
        private readonly string[] _readWriteServers;

        /// <summary>
        /// Creates a new instance of the sharded client manager
        /// </summary>
        /// <param name="readWriteServers">An array of server:port that we want to connect to</param>
        public ShardedPoolRedisClientManager(string[] readWriteServers)
        {
            _readWriteServers = readWriteServers;
            foreach (var server in readWriteServers)
            {
                _shardedPool.Add(server, new PooledRedisClientManager(server));
            }
        }

        /// <summary>
        /// Gets a redis pooled connection client using the given shardKey to balance data across a cluster.
        /// </summary>
        /// <param name="shardKey">The shardKey to determine which redis shard the data lives in</param>
        /// <returns>An IRedisClient</returns>
        public IRedisClient GetShardedClient(string shardKey)
        {
            var serverIndex = GetServerIndex(shardKey);
            var pool = _shardedPool[_readWriteServers[serverIndex]];

            return pool.GetClient();
        }

        /// <summary>
        /// Gets a redis pooled connection client using the given shardKey to balance data across a cluster.
        /// </summary>
        /// <param name="shardKey">The shardKey to determine which redis shard the data lives in</param>
        /// <returns>An ICacheClient</returns>
        public ICacheClient GetShardedCacheClient(string shardKey)
        {
            var serverIndex = GetServerIndex(shardKey);
            var pool = _shardedPool[_readWriteServers[serverIndex]];

            return pool.GetCacheClient();
        }

        /// <summary>
        /// Generates an index for the server based on the shardKey. 
        /// The index determines which server out of the server pool to retrieve/write data to
        /// </summary>
        /// <param name="shardKey">The shardKey that will be used to determine the target redis server for data</param>
        /// <returns>Index from the redis servers array where data will be written</returns>
        private long GetServerIndex(string shardKey)
        {
            //Compute a hash from a string for which we can shard on.  This is the same algorithm used by pecl/memcache standard crc32 sharding algorithm
            var hash = Crc32.Compute(System.Text.Encoding.Unicode.GetBytes(shardKey));
            var shift = (hash >> 16) & 0x7fff;
            return shift % _readWriteServers.Length;
        }
        
    }
}
