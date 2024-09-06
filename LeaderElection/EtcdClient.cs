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
public class EtcdClient: IDisposable
{
    /// <summary>
    /// Lease time in seconds
    /// </summary>
    private const int LeaseTtlInSeconds = 3;

    /// <summary>
    /// Grpc Channel
    /// </summary>
    private readonly GrpcChannel _grpcChannel;
    
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
    /// KeepAlive task
    /// </summary>
    private readonly Task _keepAliveTask;

    /// <summary>
    /// Cancellation token source
    /// </summary>
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="EtcdClient"/> class
    /// </summary>
    /// <param name="url">URL of the Etcd cluster</param>
    /// <param name="leaseTtlInSeconds">Lease time in seconds</param>
    public EtcdClient(string url, int leaseTtlInSeconds = LeaseTtlInSeconds)
    {
        this._grpcChannel = GrpcChannel.ForAddress(url);
        this._leaseClient = new Lease.LeaseClient(this._grpcChannel);
        this._kvClient = new KV.KVClient(this._grpcChannel);
        this._watchClient = new Watch.WatchClient(this._grpcChannel);
        this._leaseId = this.AcquireLease(leaseTtlInSeconds);

        this._keepAliveTask = Task.Run(() => 
            this.KeepAlive(LeaseTtlInSeconds, this._cancellationTokenSource.Token), 
            this._cancellationTokenSource.Token);
    }

    /// <summary>
    /// Puts a key-value pair in theEtcd cluster
    /// </summary>
    /// <param name="key">Key</param>
    /// <param name="value">Value</param>
    /// <returns>A <see cref="PutResponse"/> containing the response details</returns>
    public PutResponse Put(string key, string value)
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
    /// <param name="cancellationToken">Cancellation token</param>
    public async void WatchKey(string key, long startRevision, Action<Event> callback, CancellationToken cancellationToken)
    {
        using AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher = 
            this._watchClient.Watch(cancellationToken: cancellationToken);
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
            await watcher.RequestStream.WriteAsync(watchRequest, cancellationToken);

            // Ignore the 'watcher created' response
            if (await watcher.ResponseStream.MoveNext(cancellationToken))
            {
                WatchResponse watchResponse = watcher.ResponseStream.Current;
                if (!watchResponse.Created)
                {
                    throw new Exception("Unable to create watcher!");
                }
            }
            
            // Check if the key was changed before creating the watcher
            if (await watcher.ResponseStream.MoveNext(cancellationToken))
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
            if (await watcher.ResponseStream.MoveNext(cancellationToken))
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
    }

    /// <summary>
    /// Signal cancellation and perform cleanup
    /// </summary>
    public void Dispose()
    {
        // Signal the cancellation to all background tasks
        this._cancellationTokenSource.Cancel();
        
        // Wait for the keep-alive task to complete gracefully
        try
        {
            this._keepAliveTask.Wait();
        }
        catch (Exception e)
        {
            Console.WriteLine($"KeepAlive task threw an exception: {e}");
        }
        
        // Dispose of the gRPC clients and channel
        this._cancellationTokenSource.Dispose();
        this._grpcChannel.Dispose();
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
    /// <param name="cancellationToken">Cancellation token</param>
    private async Task KeepAlive(int leaseTtlInSeconds, CancellationToken cancellationToken)
    {
        try
        {
            using AsyncDuplexStreamingCall<LeaseKeepAliveRequest, LeaseKeepAliveResponse> keepAlive =
                this._leaseClient.LeaseKeepAlive(cancellationToken: cancellationToken);
            var leaseKeepAliveRequest = new LeaseKeepAliveRequest
            {
                ID = this._leaseId
            };

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await keepAlive.RequestStream.WriteAsync(leaseKeepAliveRequest, cancellationToken);
                if (!await keepAlive.ResponseStream.MoveNext(cancellationToken))
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
                await Task.Delay(TimeSpan.FromMilliseconds(delayTimeInSeconds), cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("KeepAlive task was cancelled");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Keep alive for lease id {this._leaseId} ran into issues. Exception details: {e}");
            throw;
        }
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