# SimpleDynamo

A simplified version of Dynamo. 
Three main features implemented : 1) Partitioning, 2) Replication, and 3) Failure handling. 

The main goal is to provide both availability and linearizability at the same time. 
In other words, the system always perform read and write operations successfully even under failures. 
At the same time, a read operation always return the most recent value.
