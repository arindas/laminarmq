# `indexed_segmented_log`

While the conventional `segmented_log` data structure is quite performant for a `commit_log`
implementation, it still requires the following properties to hold true for the record being
appended:
- We have the entire record in memory
- We know the record bytes' length and record bytes' checksum before the record is appended

It's not possible to know this information when the record bytes are read from an asynchronous
stream of bytes. Without the enhancements, we would have to concatenate intermediate byte buffers to
a vector. This would not only incur more allocations, but also slow down our system.

Hence, to accommodate this use case, we introduced an intermediate indexing layer to our design.

![segmented_log](https://raw.githubusercontent.com/arindas/laminarmq/assets/assets/diagrams/laminarmq-indexed-segmented-log-landscape.svg)

```
//! Index and position invariants across segmented_log

// segmented_log index invariants
segmented_log.lowest_index  = segmented_log.read_segments[0].lowest_index
segmented_log.highest_index = segmented_log.write_segment.highest_index

// record position invariants in store
records[i+1].position = records[i].position + records[i].record_header.length

// segment index invariants in segmented_log
segments[i+1].base_index = segments[i].highest_index = segments[i].index[index.len-1].index + 1
```
<p align="center">
<b>Fig:</b> Data organisation for persisting the <code>segmented_log</code> data structure on a
<code>*nix</code> file system.
</p>

In the new design, instead of referring to records with a raw offset, we refer to them with indices.
The index in each segment translates the record indices to raw file position in the segment store
file.

Now, the store append operation accepts an asynchronous stream of bytes instead of a contiguously
laid out slice of bytes. We use this operation to write the record bytes, and at the time of writing
the record bytes, we calculate the record bytes' length and checksum. Once we are done writing the
record bytes to the store, we write it's corresponding `record_header` (containing the checksum and
length), position and index as an `index_record` in the segment index.

This provides two quality of life enhancements:
- Allow asynchronous streaming writes, without having to concatenate intermediate byte buffers
- Records are accessed much more easily with easy to use indices

Now, to prevent a malicious user from overloading our storage capacity and memory with a maliciously
crafted request which infinitely loops over some data and sends it to our server, we have provided
an optional `append_threshold` parameter to all append operations. When provided, it prevents
streaming append writes to write more bytes than the provided `append_threshold`.

At the segment level, this requires us to keep a segment overflow capacity. All segment append
operations now use `segment_capacity - segment.size + segment_overflow_capacity` as the
`append_threshold` value. A good `segment_overflow_capacity` value for segments could be
`segment_capacity / 2`.

