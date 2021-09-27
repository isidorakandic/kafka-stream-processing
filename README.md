# Kafka Stream Processing
The goal was to create a Java application that reads input from a Kafka topic, processes it in some way and outputs the result to another Kafka topic.
The way data is processed is counting the number of unique users in the system per minute.

## How to run it?

The application takes in two arguments: input topic name and output topic name.

## Reasoning and discussion

### Library

Kafka Streams API is the library used for stream processing.

### Solution

My initial idea was to create a scalable solution using only simple methods of Streams API classes such as groupBy and count.
However, that solution didn't work properly. It seemed to count all users, not unique users. 
In order to fix that I needed multi-level grouping of the KStream, which couldn't be used trivially here (at least, as far as I could see). One option was to create a combined key from timestamp and user ID, but I didn't implement that.
(The code of this solution is in the second commit, called: "Flawed counter using windowedBy function".)

Instead, I try something else. I introduced custom aggregation method using Java HashMaps, which is explained in the code.
Choosing a set for storing user IDs enabled me to have the simplest aggregation logic I could think of.
Additionally, performance wise, it's a lot faster than other options since each aggregation operation is done in constant time.
This solution is produces better results than the previous attempt (I believe it counts the number of unique users correctly).

However, it's not scalable. If there's a lot of unique users in the system per one minute, the HashSet will get too big to serialize, and the application will crash at some point with this error:

``Caused by: org.apache.kafka.common.errors.RecordTooLargeException: The message is 1048592 bytes when serialized which is larger than 1048576, which is the value of the max.request.size configuration.
``

Of course, max size can be increased in configuration, but that's still not the optimal route. 
I believe my first attempt was more scalable since all the data structures used there were classes from the Streams API, meaning the way they operate
and store data is distributed and optimized to work with large amounts of data (for example KStreams, KTable, KGroupedStream...).

### Result format

Processing result is streamed into the output topic as a simple <String, Integer> key pair. The key is the timestamp and the value is the number
of unique users. It seemed intuitive to have the minute timestamp be the key, having in mind possible further processing of that topic.

#### Improvements: 
- the output is not a proper JSON file as it was required
- timestamp is not in unix format. simple conversion can be applied.

### Not completely working...

All the solutions I have, have the same problem: the result is constantly written to output stream.
So, instead of creating a new event every 60 seconds to say how many users there were in the previous minute,
data is constantly written to output topic as the counting is happening (you can see the count increasing).
I couldn't figure out how to fix that. I guess that would be easier to fix if I used a lower level API and 
implemented my own Producer. Then I could make it send only one event to the output topic per minute (I assume that's how that would be done,
I didn't spend much time on Consumer and Producer APIs).

### Timestamp dilemma

I spent some time thinking which timestamp I should use. Challenge propositions state that 99.9% of frames arrive with a maximum latency of 5 seconds, from which I gathered that
it's acceptable to use both 'ts' value from the event data, and the metadata timestamp of the moment event
entered the input topic. Since I used built-in windowing function, I opted for the second.

I also had the idea to use the 'ts' timestamp from JSON, map it to minute accuracy (each timestamp that belongs to one minute is mapped to the same value).
I would then group the data based on timestamp, and then group those partitions by uid (so I get unique ones only)
and then count them. I didn't get the chance to implement that.
