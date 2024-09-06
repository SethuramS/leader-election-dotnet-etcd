using Etcdserverpb;
using Mvccpb;

namespace LeaderElection;

/// <summary>
/// Class to run Leader Election using Etcd
/// </summary>
public class LeaderElection
{
    /// <summary>
    /// Election prefix
    /// </summary>
    private const string ElectionPrefix = "election/";

    /// <summary>
    /// Etcd client
    /// </summary>
    private readonly EtcdClient _etcdClient;

    /// <summary>
    /// Current node ID
    /// </summary>
    private readonly string _id;

    /// <summary>
    /// Cancellation token source
    /// </summary>
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    
    /// <summary>
    /// Object used for waiting and locking
    /// </summary>
    private readonly object _lockObject = new();
    
    /// <summary>
    /// Flag indicating whether the leader election process has completed
    /// </summary>
    private bool _completed;

    /// <summary>
    /// Initializes the Etcd client and registers current node
    /// </summary>
    private LeaderElection()
    {
        this._etcdClient = new EtcdClient("http://localhost:2379");
        this._id = this.RegisterNode();
    }

    /// <summary>
    /// Registers current node and runs the Leader Election algorithm
    /// </summary>
    public static void Main()
    {
        var leaderElection = new LeaderElection();

        try
        {
            leaderElection.RunLeaderElection();
            leaderElection.Run();
        }
        finally
        {
            leaderElection.ShutDown();
        }
    }

    /// <summary>
    /// Add an entry in the Etcd cluster for the current node
    /// </summary>
    /// <returns>The node ID</returns>
    private string RegisterNode()
    {
        string id = this.GenerateFullKey(Guid.NewGuid().ToString());
        PutResponse putResponse = this._etcdClient.Put(id, "foo");
        this._etcdClient.WatchKey(
            id, 
            putResponse.Header.Revision, 
            this.WatchCallback, 
            this._cancellationTokenSource.Token);
        Console.WriteLine($"Registered current node with Id: {id}");

        return id;
    }

    /// <summary>
    /// Runs the Leader Election algorithm.
    /// A leader is the node with the minimum revision number.
    /// All other nodes track the node with the previous revision number (to avoid the herd effect).
    /// </summary>
    private void RunLeaderElection()
    {
        RangeResponse rangeResponse = this._etcdClient.Range(ElectionPrefix);
        List<KeyValue> orderedNodes = rangeResponse.Kvs.OrderBy(kv => kv.ModRevision).ToList();

        if (orderedNodes.First().Key.ToStringUtf8() == this._id)
        {
            Console.WriteLine("I am the leader!");
        }
        else
        {
            int nodeIndex = orderedNodes.TakeWhile(kv => kv.Key.ToStringUtf8() != this._id).Count();
            KeyValue prevKeyValue = orderedNodes[nodeIndex - 1];
            this._etcdClient.WatchKey(
                prevKeyValue.Key.ToStringUtf8(), 
                prevKeyValue.ModRevision, 
                this.WatchCallback,
                this._cancellationTokenSource.Token);
            Console.WriteLine($"I am node {orderedNodes[nodeIndex].Key.ToStringUtf8()} " +
                              $"and I track {prevKeyValue.Key.ToStringUtf8()}");
        }
    }

    /// <summary>
    /// Callback function passed to the watch API.
    /// It is called whenever there is a change (put/delete) in the key being watched.
    /// </summary>
    /// <param name="watchEvent">Watch event representing the key's changes</param>
    private void WatchCallback(Event watchEvent)
    {
        Console.WriteLine($"WatchResponse: {watchEvent}");
        // Check this event is for current node
        if (watchEvent.Kv.Key.ToStringUtf8() == this._id)
        {
            Console.WriteLine("Current node key was updated or deleted. Exiting.");

            lock (this._lockObject)
            {
                this._completed = true;
                Monitor.PulseAll(this._lockObject);
            }
        }
        // The only other key that is watched is the key of the node being tracked
        else
        {
            Console.WriteLine("The tracking node key was updated or deleted. Re-running Leader Election.");
            this.RunLeaderElection();
        }
    }

    /// <summary>
    /// Generate the full key name, i.e. electionPrefix + id
    /// </summary>
    /// <param name="id">Key ID</param>
    /// <returns>Full key name</returns>
    private string GenerateFullKey(string id)
    {
        return $"{ElectionPrefix}{id}";
    }

    /// <summary>
    /// Runs the app and waits until completion
    /// </summary>
    private void Run()
    {
        lock (this._lockObject)
        {
            while (!this._completed)
            {
                Monitor.Wait(this._lockObject);
            }
        }

        Console.WriteLine("Execution Completed!");
    }

    /// <summary>
    /// Signal cancellation and perform cleanup
    /// </summary>
    private void ShutDown()
    {
        this._cancellationTokenSource.Cancel();
        this._etcdClient.Dispose();
        this._cancellationTokenSource.Dispose();
    }
}