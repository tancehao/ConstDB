## 0.0.4  
 - Add more commands of hash and set.  
## 0.0.3  
 - Improved the README.  
 - Add command `wait` for consistency configuration.  
 - Add command `replcheck` for replication progress asking.  
 - Add support for multi-value registers.  
 - Turn on NO_DELAY option for all tcp sockets.  
 - Other optimizations.  
  
## 0.0.2  
 - Added support for list type, including the crdt implementation and some of its commands.  
 - Switched the tokio's runtime from multi-threaded one to a single one on each thread. More effecient according to benchmarks.  
 - Optimized the implementation of stats collector. Make use of thread-local caches.  
 - Optimized the repl_backlog module.  
 - Optimized the event module.  
 - Fixed some bugs in case where deletion and modification happen concurrently on the same key.  
 - More unit tests.  
  
## 0.0.1  
init  
