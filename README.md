# SimpleDynamoDB
Implemented a Dynamo-style key-value storage with Partitioning, Replication, and Failure handling, that provides Availability and Linearizability at the same time across multiple Android devices. 
Users will be able to read and write data successfully even under device failures, with read operations always returning the most recent value. 

Please refer [Project Specification.pdf](https://github.com/dani-amirtharaj/SimpleDynamoDB/blob/master/Project%20Specification.pdf) for more details.
