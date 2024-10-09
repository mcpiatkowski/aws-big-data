
# Stream processing #

Write a program using Apache Spark to process streaming data from the dataset in the references by performing basic stateless and stateful operations.

Implement projection, filtering, windowing, aggregation, and join operations on a streaming data source.

It is necessary to:

- Implement a stateless operation:
  - Projection: Select and display the *status* of the *flights* based on *today's* *departure date*. 
  - Filtering: Apply a filter to only process data where the *status* field is equal to *canceled*.

- Implement stateful operations:
  - Windowing: Define a sliding time window of 5 minutes and calculate the total number of the *flights* field within each window. 
  - Aggregation: Calculate the total number of the *landed* *flights* field for every 1-minute interval.

## Acceptance criteria ##

- Streaming data source is set up, such as Apache Kafka or a socket source, to simulate continuous streaming data.
- Streaming data is processed using the implemented operations and display the results in real-time.
- Projected fields, filtered data, windowed aggregations, and joined datasets are correctly processed and displayed.

Dataset:

[Flight Status & Track](https://developer.flightstats.com/api-docs/flightstatus/v2)