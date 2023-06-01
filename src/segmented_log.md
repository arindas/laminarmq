# `segmented_log`

The segmented-log data structure for storing records was originally described in the [Apache
Kafka](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/09/Kafka.pdf) paper.

![segmented_log](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-segmented-log.svg)
<p align="center">
<b>Fig:</b> File organisation for persisting the <code>segmented_log</code> data structure on a
<code>*nix</code> file system.
</p>

A segmented log is a collection of read segments and a single write segment. Each "segment" is
backed by a storage file on disk called "store".

The log is:
- "immutable", since only "append", "read" and "truncate" operations are allowed. It is not possible
  to update or delete records from the middle of the log.
- "segmented", since it is composed of segments, where each segment services records from a
  particular range of offsets.

All writes go to the write segment. A new record is written at `offset = write_segment.next_offset`
in the write segment. When we max out the capacity of the write segment, we close the write segment
and reopen it as a read segment. The re-opened segment is added to the list of read segments. A new
write segment is then created with `base_offset` equal to the `next_offset` of the previous write
segment.

When reading from a particular offset, we linearly check which segment contains the given read
segment. If a segment capable of servicing a read from the given offset is found, we read from that
segment. If no such segment is found among the read segments, we default to the write segment. The
following scenarios may occur when reading from the write segment in this case:
- The write segment has synced the messages including the message at the given offset. In this case
  the record is read successfully and returned.
- The write segment hasn't synced the data at the given offset. In this case the read fails with a
  segment I/O error.
- If the offset is out of bounds of even the write segment, we return an "out of bounds" error.
