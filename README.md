## disruptor-stream-csharp
An example of the Disruptor C# implementation which processes a stream of updates to Todo Lists.

To test the project on your machine, run the NUnit tests. The tests will try all different configuration combinations and report a ranked list to the test-ouput.

On my laptop, the best strategy was 2 JSON threads, using the sleeping wait strategy and the single-threaded claim strategy. This strategy processed 3194 requests per second. 

```
rate of 3191 with jsonParallelism: 2, ringSize: 1024, waitStrategyName: sleep, claimStrategyName: multi-low-contention.
rate of 3046 with jsonParallelism: 2, ringSize: 1024, waitStrategyName: yield, claimStrategyName: multi-low-contention.
rate of 3015 with jsonParallelism: 1, ringSize: 1024, waitStrategyName: sleep, claimStrategyName: multi-low-contention.
rate of 3006 with jsonParallelism: 1, ringSize: 1024, waitStrategyName: yield, claimStrategyName: single.
rate of 3003 with jsonParallelism: 2, ringSize: 1024, waitStrategyName: yield, claimStrategyName: single.
rate of 2994 with jsonParallelism: 1, ringSize: 1024, waitStrategyName: yield, claimStrategyName: multi-low-contention.
rate of 2944 with jsonParallelism: 1, ringSize: 1024, waitStrategyName: sleep, claimStrategyName: single.
rate of 2893 with jsonParallelism: 2, ringSize: 1024, waitStrategyName: sleep, claimStrategyName: single.
```
