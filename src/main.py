from kafka.partitioner.default import DefaultPartitioner

"""
Keys will follow this structure: event.app_name.context.entity.use_case
The problem we are experimenting with is that we will have some parts of this
structure that will be more frequent that others.

We want to understand how this will affect the partition selected by the default_partitioner
implemented in https://github.com/dpkp/kafka-python/blob/2.0.2/kafka/partitioner/default.py
which is base in the hash algo murmur2: https://github.com/apache/kafka/blob/0.8.2/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L244
This means this experiment should be valid also for Debezium source connector.

On the other hand, if we were to use the other library available: https://github.com/confluentinc/confluent-kafka-python
This one is based on the C/C++ client: https://docs.confluent.io/platform/7.5/clients/librdkafka/html/
This client, has different partitioners available. The default one is: https://docs.confluent.io/platform/7.5/clients/librdkafka/html/rdkafka_8h.html#aac74a45b5ce46035782198f683b22363
Which implements the algo CRC32-c: https://en.wikipedia.org/wiki/Cyclic_redundancy_check

An important note on this, if we want to use a Producer based on the Java implementation and another one based on librdkafka
we must have in sight that both algorithms will get us different results for the same key, so the client based on librdkafka
must be configured to use the murmur2 function: https://docs.confluent.io/platform/7.5/clients/librdkafka/html/rdkafka_8h.html#ab5dab15f3d0a293db43338f17544e26a
More info in this article: https://www.confluent.io/blog/standardized-hashing-across-java-and-non-java-producers/
"""
record_keys = (
    "event.one.core.guitar.order_created",
    "event.one.core.keyboard.user_registered",
    "event.one.core.drum.payment_processed",
    "event.one.core.amplifier.product_added",
    "event.one.core.strings.transaction_completed",
    "event.one.core.microphone.customer_updated",
    "event.two.b2b.piano.order_created",
    "event.two.b2b.saxophone.user_registered",
    "event.three.billing.cymbals.payment_approved",
    "event.four.social.violin.product_removed"
)


def assign_partition(key: str, partitions: int = 10) -> int:
    """
    https://github.com/dpkp/kafka-python/blob/2.0.2/kafka/partitioner/default.py#L17C9-L23C12
    Get the partition corresponding to key
    :param key: partitioning key
    :param all_partitions: list of all partitions sorted by partition ID
    :param available: list of available partitions in no particular order
    :return: one of the values from all_partitions or available
    """
    partitions_id_sorted = [i for i in range(partitions)]
    available = [i for i in range(partitions)]
    serialized_key_to_bytes = key.encode("utf-8")

    partitioner = DefaultPartitioner()
    return partitioner(serialized_key_to_bytes, partitions_id_sorted, available)


def run_experiment(number_of_partitions: int) -> None:
    assigments = {}
    for key in record_keys:
        partition = assign_partition(key, number_of_partitions)

        if partition not in assigments:
            assigments[partition] = []
        assigments[partition].append(key)

        """print(f"key: {key}")
        print(f"partition: {partition}")
        print(f"-----")"""
    for partition, v in assigments.items():
        print(f"For partition {partition}: {len(v)} keys were assigned.")
        print(v)

    print(f"End experiment for {number_of_partitions} partitions")


if __name__ == '__main__':
    run_experiment(2)
    run_experiment(5)
    run_experiment(10)
