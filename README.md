# Elasticsearch Cluster Cleanup Script

*Warning* this attempts a potentially destructive action. Do not run on an Elasticsearch cluster where you cannot accept data loss.

This script will attempt to bring a Yellow Elasticsearch cluster back to green by cleaning up large indices/data streams by:

1. Finding all data streams larger than the given `max-size-bytes` flag.
2. Rolling over these data streams to new internal indices.
3. Deleting the old large index. (This is destructive. Do not run this on an Elasticsearch cluster where you can't lose the old data in these indices)
4. Finally calling `_cluster/reroute` to attempt to reroute any pending failed shards.

## Building the tool

`make build`

## Running the tool

```sh
# cleans up indices larger than 100MB
./bin/cleanup -p your-password -u your-username -m 104857600
```
