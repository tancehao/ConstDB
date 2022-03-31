# Introduction
ConstDB is an in-memory cache store which aims at master-master replications. A group of ConstDB instances can be deployed across many regions, and each of them is available for
reading and writing. Every instance keeps in touch with all other ones. When a write command has been executed in one instance, it is asynchronously replicated to all the other instances. The data structures that are used for storing the client's data implement CRDT(Conflict-Free Replicated Data Types), thus we are sure data in different instances are eventually consistent.  
ConstDB is compatible with redis protocol, and I have implemented a portion of redis's data types and commands. I'll keep developing it and take as much features in redis as possible into ConstDB. Contribution is also welcomed.

# Features:
* CRDT: Each write command is associated with an locally increasing 64-bit uuid and an unique node id, therefore the synchronization is idempotent and loopback is avoided. And then the data structures used for storing also track some extra information beside user's data. For example, for some data types, we store the time they are lastly modified. This way, if two commands modifying the same key are executed at different nodes concurrently, we solve the conflicts through `Last-Write-Win` and `Add-Win` policies when they are merged at each node eventually. We also support other crdt types including `PNCounter`, `LWWRegister`, `MultiValueRegister`,`ORSet` and `Sequence` and so on.
* Online scaling: Nodes are free to join or leave a group. When we want to add a node(B) into an already running group, we simply let it to `MEET` a node(A) that is contained in that group. The new node(B) firstly connects to that node. And then they exchange their whole data set through their own snapshot file(this is safe because their data both implement CRDT)——in which all the necessary information of their own replicas are also listed. Once one node has merged the other's snapshot, it then continues a)to accept his write commands happened after the snapshot, and b)connects to the newly found replicas from snapshot and exchange their data. Finnaly the new node(B) keeps pace with all other nodes in this group.
* Multiple io threads: Input requests and output responses are read or written on io threads, and each command is executed sequentially in the main thread. This way the performance is much better.


# Test
You can experience the master-master replications by the following test.
1. Download the project and build.
> git clone https://github.com/tancehao/ConstDB && cd ConstDB  
> cargo build
 
2. Run 2 processes listening to different local ports. Make 2 copies of the file `constdb.toml`.
> mkdir server9001 server9002</p>
> cp default.toml server9001/constdb.toml && cp default.toml server9002/constdb.toml  

And then edit the `constdb.toml` config files in each directory(change the id, node_alias, port and other options).
Finally start 2 process with these different configs.
> cd server9001 && ../target/debug/constdb-server constdb.toml  
> cd ..  
> cd server9002 && ../target/debug/constdb-server constdb.toml

3. Now a happy moment has come. We modify different keys in each node and make them keep in touch.
> redis-cli -p 9001 incr k1             // 1  
> redis-cli -p 9002 incr k2             // 1  
> redis-cli -p 9002 meet 127.0.0.1:9001 //  1

wait for some seconds.
> redis-cli -p 9001 get k2          // 1  
> redis-cli -p 9002 get k1          // 1  
> redis-cli -p 9002 incr k2         // 2  
> redis-cli -p 9001 get k2          // 2  
> redis-cli -p 9001 hset h1 k1 v1   // +OK  
> redis-cli -p 9002 hget h1 k1      // "v1"

we can see that the each node stays abreast of the other.

# Commands available
- **last-write-win registers**  
    `get`, `set`, `del`  

- **multi-value register**  
    `mvget`: get the list of node_id -> value pairs.  
    `mvset`: set the value corresponds to our node. Use the `-HARD` option to delete all other versions. 

- **counters**:  
    `get`, `incr`, `decr`  

- **hash**  
    `hset`: last write win  
    `hdel`: add win  
    `hget`, `hgetall`  

- **set**  
    `sadd`, `srem`, `spop`, `smembers`  

- **list**  
    `llen`, `lindex`, `lrange`, `lpos`, `rpop`, `rpush`, `lpop`, `lpush`, `lrem`, `lset`, `linsert`, `ltrim`  

- **control**  
    `desc`: describe the detailed information of a crdt value.  
    `replicas`: list the replicas in the group currently.  
    `node`: tell the identification of the server.  
    `info`: show the stats collected so far.  
    `meet`: let the server to join another one's group.
    `forget`: left the server to kick another one out from the group.  
    `repllog`: print the replicate log with a specified uuid.  
    `del`, `expire` 

# Contact
Email: tancehao93@163.com  
Wechat id: xiaotanzi-bien  
