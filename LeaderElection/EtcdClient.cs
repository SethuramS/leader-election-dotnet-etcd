using System.Text;
using Etcdserverpb;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Mvccpb;

namespace LeaderElection;

/// <summary>
/// Minimal Etcd client for Leader Election
/// </summary>
public class EtcdClient
{
    /// <summary>
    /// Lease time in seconds
    /// </summary>
    private const int LeaseTtlInSeconds = 3;

    /// <summary>
    /// Key-Value Client
    /// </summary>
    private readonly KV.KVClient _kvClient;

    /// <summary>
    /// Lease Client
    /// </summary>
    private readonly Lease.LeaseClient _leaseClient;

    /// <summary>
    /// Watch Client
    /// </summary>
    private readonly Watch.WatchClient _watchClient;

    /// <summary>
    /// Lease ID of the current connection
    /// </summary>
    private readonly long _leaseId;

    /// <summary>
    /// Initializes a new instance of the <see cref="EtcdClient"/> class
    /// </summary>
    /// <param name="url">URL of the Etcd cluster</param>
    /// <param name="leaseTtlInSeconds">Lease time in seconds</param>
    public EtcdClient(string url, int leaseTtlInSeconds = LeaseTtlInSeconds)
    {
        GrpcChannel grpcChannel = GrpcChannel.ForAddress(url);
        this._leaseClient = new Lease.LeaseClient(grpcChannel);
        this._kvClient = new KV.KVClient(grpcChannel);
        this._watchClient = new Watch.WatchClient(grpcChannel);
        this._leaseId = this.AcquireLease(leaseTtlInSeconds);
        this.KeepAlive(leaseTtlInSeconds);
    }

    /// <summary>
    /// Puts a key-value pair in theEtcd cluster
    /// </summary>
    /// <param name="key">Key</param>
    /// <param name="value">Value</param>
    /// <returns>A <see cref="PutResponse"/> containing the response details</returns>
    public PutResponse PutAsync(string key, string value)
    {
        var putRequest = new PutRequest
        {
            Key = ByteString.CopyFromUtf8(key),
            Value = ByteString.CopyFromUtf8(value),
            Lease = this._leaseId
        };

        return this._kvClient.Put(putRequest);
    }

    /// <summary>
    /// Retrieves all keys with the specified prefix
    /// </summary>
    /// <param name="prefix">Prefix to match</param>
    /// <returns>A <see cref="RangeResponse"/> containing all the keys with the given prefix</returns>
    public RangeResponse Range(string prefix)
    {
        string rangeEnd = this.GetRangeEnd(prefix);
        var rangeRequest = new RangeRequest
        {
            Key = ByteString.CopyFromUtf8(prefix),
            RangeEnd = ByteString.CopyFromUtf8(rangeEnd)
        };

        return this._kvClient.Range(rangeRequest);
    }

    /// <summary>
    /// Watches a key for changes (update/delete)
    /// </summary>
    /// <param name="key">Key to watch</param>
    /// <param name="startRevision">Revision to start watch from</param>
    /// <param name="callback">A callback method that accepts an <see cref="Event"/> parameter</param>
    public void WatchKey(string key, long startRevision, Action<Event> callback)
    {
        Task.Run(async () =>
        {
            using AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher = this._watchClient.Watch();
            try
            {
                var watchRequest = new WatchRequest
                {
                    CreateRequest = new WatchCreateRequest
                    {
                        Key = ByteString.CopyFromUtf8(key),
                        StartRevision = startRevision
                    }
                };
                await watcher.RequestStream.WriteAsync(watchRequest);

                // Ignore the 'watcher created' response
                if (await watcher.ResponseStream.MoveNext())
                {
                    WatchResponse watchResponse = watcher.ResponseStream.Current;
                    if (!watchResponse.Created)
                    {
                        throw new Exception("Unable to create watcher!");
                    }
                }
                
                // Check if the key was changed before creating the watcher
                if (await watcher.ResponseStream.MoveNext())
                {
                    WatchResponse watchResponse = watcher.ResponseStream.Current;
                    if (watchResponse.Events.Last().Kv.ModRevision != startRevision)
                    {
                        Console.WriteLine($"Key {key} was changed before watcher was created!");
                        await watcher.RequestStream.CompleteAsync();
                        callback(watchResponse.Events.Last());
                    }
                }

                // Send the watcher event using the callback
                if (await watcher.ResponseStream.MoveNext())
                {
                    WatchResponse watchResponse = watcher.ResponseStream.Current;
                    await watcher.RequestStream.CompleteAsync();
                    callback(watchResponse.Events.Last());
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Watcher for key {key} ran into issues. Exception details: {e}");
                await watcher.RequestStream.CompleteAsync();
                var watchEvent = new Event()
                {
                    Kv = new KeyValue
                    {
                        Key = ByteString.CopyFromUtf8(key)
                    }
                };
                callback(watchEvent);

                throw;
            }
        });
    }
    
    /// <summary>
    /// Acquires a Lease ID
    /// </summary>
    /// <param name="leaseTtlInSeconds">Lease time in seconds</param>
    /// <returns>Lease ID</returns>
    private long AcquireLease(int leaseTtlInSeconds)
    {
        var leaseGrantRequest = new LeaseGrantRequest
        {
            TTL = leaseTtlInSeconds
        };
        LeaseGrantResponse leaseGrantResponse = this._leaseClient.LeaseGrant(leaseGrantRequest);

        return leaseGrantResponse.ID;
    }

    /// <summary>
    /// Keeps the lease alive by sending keep-alive requests in the background
    /// </summary>
    /// <param name="leaseTtlInSeconds">Lease time in seconds</param>
    private void KeepAlive(int leaseTtlInSeconds)
    {
        Task.Run(async () =>
        {
            try
            {
                using AsyncDuplexStreamingCall<LeaseKeepAliveRequest, LeaseKeepAliveResponse> keepAlive =
                    this._leaseClient.LeaseKeepAlive();
                var leaseKeepAliveRequest = new LeaseKeepAliveRequest
                {
                    ID = this._leaseId
                };

                while (true)
                {
                    await keepAlive.RequestStream.WriteAsync(leaseKeepAliveRequest);
                    if (!await keepAlive.ResponseStream.MoveNext())
                    {
                        await keepAlive.RequestStream.CompleteAsync();
                        throw new EndOfStreamException("Didn't get any response for keep alive request");
                    }

                    LeaseKeepAliveResponse leaseKeepAliveResponse = keepAlive.ResponseStream.Current;
                    if (leaseKeepAliveResponse.TTL == 0)
                    {
                        await keepAlive.RequestStream.CompleteAsync();
                        throw new TimeoutException("Keep alive lease expired");
                    }

                    double delayTimeInSeconds = (double)leaseTtlInSeconds * 1000 / 3;
                    await Task.Delay(TimeSpan.FromMilliseconds(delayTimeInSeconds));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Keep alive for lease id {this._leaseId} ran into issues. Exception details: {e}");
                throw;
            }
        });
    }

    /// <summary>
    /// Gets the string representing the range end.
    /// The range end is calculated by incrementing the last byte of the base 64 encoding of the given string.
    /// </summary>
    /// <param name="str">String for which the range end needs to be calculated</param>
    /// <returns>String representing range end</returns>
    private string GetRangeEnd(string str)
    {
        if (string.IsNullOrEmpty(str)) return str;

        byte[] bytes = Encoding.ASCII.GetBytes(str);
        bytes[^1]++;

        return Encoding.ASCII.GetString(bytes);
    }
}