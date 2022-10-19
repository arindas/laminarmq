//! Module providing abstractions for supporting a immutable, segmented and persistent
//! [`CommitLog`](super::CommitLog) implementation.
//!
//! This module is not meant to be used directly by application developers. Application
//! developers would prefer one of the specializations specific to their async runtime.
//! See [`glommio_impl`](super::glommio_impl).

use std::{
    error::Error,
    fmt::Display,
    fs,
    ops::Deref,
    path::{Path, PathBuf},
    time::Duration,
};

use async_trait::async_trait;

use super::{
    segment::{self, Segment},
    store, Record,
};

/// Error type for our segmented log implementation. Used as the `Error` associated type for
/// our [`SegmentedLog`]'s [`CommitLog`](super::CommitLog) trait implementation.
#[derive(Debug)]
pub enum SegmentedLogError<T, S>
where
    T: Deref<Target = [u8]>,
    S: store::Store<T>,
{
    /// An error caused by an operation on an underlying segment.
    SegmentError(segment::SegmentError<T, S>),

    /// IO error caused during the creating and deletion of storage directories for our
    /// [`SegmentedLog`]'s storage.
    IoError(std::io::Error),

    /// The [`Option`] containing our write segment evaluates to [`None`].
    WriteSegmentLost,

    /// The given offset is greater than or equal to the highest offset of the segmented log.
    OffsetOutOfBounds,

    /// The given offset has not been synced to disk and is not a valid offset to advance to.
    OffsetNotValidToAdvanceTo,
}

impl<T, S> Display for SegmentedLogError<T, S>
where
    T: Deref<Target = [u8]>,
    S: store::Store<T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SegmentError(err) => write!(f, "Segment error occurred: {}", err),
            Self::IoError(err) => write!(f, "IO error occurred: {}", err),
            Self::WriteSegmentLost => write!(f, "Write segment is None."),
            Self::OffsetOutOfBounds => write!(f, "Given offset is out of bounds."),
            Self::OffsetNotValidToAdvanceTo => {
                write!(f, "Given offset is not a valid offset to advance to.")
            }
        }
    }
}

impl<T, S> Error for SegmentedLogError<T, S>
where
    T: Deref<Target = [u8]> + std::fmt::Debug,
    S: store::Store<T> + std::fmt::Debug,
{
}

/// Strategy pattern trait for constructing [`Segment`] instances for [`SegmentedLog`].
#[async_trait(?Send)]
pub trait SegmentCreator<T, S>
where
    T: Deref<Target = [u8]>,
    S: store::Store<T>,
{
    /// Constructs a new segment with it's store at the given store path, base_offset and
    /// segment config.
    async fn new_segment_with_store_file_path_offset_and_config<P: AsRef<Path>>(
        &self,
        store_file_path: P,
        base_offset: u64,
        config: segment::config::SegmentConfig,
    ) -> Result<Segment<T, S>, segment::SegmentError<T, S>>;

    /// Constructs a new segment in the given storage directory with the given base offset and
    /// config.
    ///
    /// The default provided implementation simply invokes
    /// [`Self::new_segment_with_store_file_path_offset_and_config`] with `store_file_path`
    /// obtained by invoking [`common::store_file_path`] on `storage_dir` and `base_offset`.
    async fn new_segment_with_storage_dir_offset_and_config<P: AsRef<Path>>(
        &self,
        storage_dir: P,
        base_offset: u64,
        config: segment::config::SegmentConfig,
    ) -> Result<Segment<T, S>, segment::SegmentError<T, S>> {
        self.new_segment_with_store_file_path_offset_and_config(
            common::store_file_path(storage_dir, base_offset),
            base_offset,
            config,
        )
        .await
    }
}

/// A cross platform immutable, persisted and segmented log implementation using [`Segment`]
/// instances.
///
/// [`SegmentedLog`] is a [`CommitLog`](super::CommitLog) implementation.
///
/// A segmented log is a collection of read segments and a single write segment. It consists of
/// a [`Vec<Segment>`] for storing read segments and a single [`Option<Segment>`] for storing
/// the write segment. The log is immutable since, only append and read operations are
/// available. There are no record update and delete operations. The log is segmented, since it
/// is composed of segments, where each segment services records from a particular range of
/// offsets.
///
/// ```text
/// [segmented_log]
/// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
/// │                                                                                                                                                     │
/// │                               [log:"read"]           ├[offset#x)    ├[offset#x + size(xm_1)]  ├[offset(xm_n-1)]   ├[offset(xm_n-1) + size(xm_n-1)]  │
/// │                               ┌──────────┐           ┌──────────────┬──────────────┬─         ┬───────────────────┬──────────────┐                  │
/// │  initial_offset: offset#x     │ offset#x │->[segment]│ message#xm_1 │ message#xm_2 │ …      … │ message#xm_n-1    │ message#xm_n │                  │
/// │                               │          │           └──────────────┴──────────────┴─         ┴───────────────────┴──────────────┘                  │
/// │                               │          │                                                                                                          │
/// │                               ├──────────┤           ├[offset#y)    ├[offset#y + size(ym_1)]  ├[offset(ym_n-1)]   ├[offset(ym_n-1) + size(ym_n-1)]  │
/// │                               ├──────────┤           ┌──────────────┬──────────────┬─         ┬───────────────────┬──────────────┐                  │
/// │  {offset#y = (offset(xm_n) +  │ offset#y │->[segment]│ message#ym_1 │ message#ym_2 │ …      … │ message#ym_n-1    │ message#ym_n │                  │
/// │               size(xm_n))}    │          │           └──────────────┴──────────────┴─         ┴───────────────────┴──────────────┘                  │
/// │                               │          │                                                                                                          │
/// │                               ├──────────┤           ├[offset#x)    ├[offset#x + size(xm_1)]  ├[offset(xm_n-1)]   ├[offset(xm_n-1) + size(xm_n-1)]  │
/// │                               ├──────────┤           ┌──────────────┬──────────────┬─         ┬───────────────────┬──────────────┐                  │
/// │  {offset#z = (offset(ym_n) +  │ offset#z │->[segment]│ message#zm_1 │ message#zm_2 │ …      … │ message#zm_n-1    │ message#zm_n │                  │
/// │               size(ym_n))}    │          │           └──────────────┴──────────────┴─         ┴───────────────────┴──────────────┘                  │
/// │                               │          │                                                                                                          │
/// │                               ├──────────┤                                                                                                          │
/// │                                   ....                                                                                                              │
/// │                                                                                                                                                     │
/// │                                                                                                                                                     │
/// │                               [log:"write"] -> [segment](with base_offset = next_offset of last read segment)                                       │
/// │                                                                                                                                                     │
/// │  where:                                                                                                                                             │
/// │  =====                                                                                                                                              │
/// │  offset(m: Message) := offset of message m in log.                                                                                                  │
/// │  size(m: Message) := size of message m                                                                                                              │
/// │                                                                                                                                                     │
/// │  offset#x := some offset x                                                                                                                          │
/// │  message#m := some message m                                                                                                                        │
/// │                                                                                                                                                     │
/// └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
/// ```
///
/// All writes go to the write segment. When we max out the capacity of the write segment, we
/// close the write segment and reopen it as a read segment. The re-opened segment is added to
/// the list of read segments. A new write segment is then created with `base_offset` as the
/// `next_offset` of the previous write segment.
///
/// When reading from a particular offset, we linearly check which segment contains the given
/// read segment. If a segment capable of servicing a read from the given offset is found, we
/// read from that segment. If no such segment is found among the read segments, we default to
/// the write segment. The following scenarios may occur when reading from the write segment in
/// this case:
/// - The write segment has synced the messages including the message at the given offset. In
/// this case the record is read successfully and returned.
/// - The write segment hasn't synced the data at the given offset. In this case the read fails
/// with a segment I/O error.
/// - If the offset is out of bounds of even the write segment, we return an "out of bounds"
/// error.

pub struct SegmentedLog<T, S, SegC>
where
    T: Deref<Target = [u8]>,
    S: store::Store<T>,
    SegC: SegmentCreator<T, S>,
{
    write_segment: Option<Segment<T, S>>,
    read_segments: Vec<Segment<T, S>>,

    storage_directory: PathBuf,
    config: config::SegmentedLogConfig,

    segment_creator: SegC,
}

#[doc(hidden)]
macro_rules! write_segment_ref {
    ($segmented_log:ident, $ref_method:ident) => {
        $segmented_log
            .write_segment
            .$ref_method()
            .ok_or(SegmentedLogError::WriteSegmentLost)
    };
}

impl<T, S, SegC> SegmentedLog<T, S, SegC>
where
    T: Deref<Target = [u8]>,
    S: store::Store<T>,
    SegC: SegmentCreator<T, S>,
{
    /// "Moves" the current write segment to the vector of read segments and in its place, a new
    /// write segment is created.
    ///
    /// This moving process involves closing the current write segments, creating a new read
    /// segment with this write segments base offset and adding it to the end of the read
    /// segments' vector. Once this process is completed, a new write segments is created with
    /// the old write segments base offset.
    ///
    /// ## Errors
    /// - [`SegmentedLogError::WriteSegmentLost`]: if the write segment [`Option`] evaluates to
    /// [`None`] before this operation.
    /// - [`SegmentedLogError::SegmentError`]: If there is an error during operning a new
    /// segment.
    async fn rotate_new_write_segment(&mut self) -> Result<(), SegmentedLogError<T, S>> {
        let old_write_segment = self
            .write_segment
            .take()
            .ok_or(SegmentedLogError::WriteSegmentLost)?;

        let old_write_segment_base_offset = old_write_segment.base_offset();
        let new_write_segment_base_offset = old_write_segment.next_offset();

        old_write_segment
            .close()
            .await
            .map_err(SegmentedLogError::SegmentError)?;

        self.read_segments.push(
            self.segment_creator
                .new_segment_with_storage_dir_offset_and_config(
                    Path::new(&self.storage_directory),
                    old_write_segment_base_offset,
                    self.config.segment_config,
                )
                .await
                .map_err(SegmentedLogError::SegmentError)?,
        );

        self.write_segment = Some(
            self.segment_creator
                .new_segment_with_storage_dir_offset_and_config(
                    Path::new(&self.storage_directory),
                    new_write_segment_base_offset,
                    self.config.segment_config,
                )
                .await
                .map_err(SegmentedLogError::SegmentError)?,
        );

        Ok(())
    }

    /// Returns whether the current write segment is maxed out or not. We invoke
    /// [`Segment::is_maxed`] on the current write segment and return it.
    ///
    /// ## Errors
    /// - [`SegmentedLogError::WriteSegmentLost`]: if the current write segment
    /// [`Option`] evaluates to [`None`].
    #[inline]
    pub fn is_write_segment_maxed(&self) -> Result<bool, SegmentedLogError<T, S>> {
        self.write_segment
            .as_ref()
            .map(|x| x.is_maxed())
            .ok_or(SegmentedLogError::WriteSegmentLost)
    }

    /// Creates a new [`SegmentedLog`] instance with files stored in the given storage
    /// directory.
    ///
    /// If there are no segments present in the given directory, a new write segment is created
    /// with [`config::SegmentedLogConfig::initial_offset`] as the base offset. The given
    /// segment creator is used for creating segments during the various operation of the
    /// segmented log, such as rotating in new write segments, reopening the write segment etc.
    ///
    /// ## Returns
    /// - A newly created [`SegmentedLog`] instance.
    ///
    /// ## Errors
    /// - As discussed in [`common::read_and_write_segments`] and
    /// [`common::segments_in_directory`]
    pub async fn new<P: AsRef<Path>>(
        storage_directory_path: P,
        config: config::SegmentedLogConfig,
        segment_creator: SegC,
    ) -> Result<Self, SegmentedLogError<T, S>> {
        let (read_segments, write_segment) = common::read_and_write_segments(
            &storage_directory_path,
            &segment_creator,
            config,
            common::segments_in_directory(
                &storage_directory_path,
                &segment_creator,
                config.segment_config,
            )
            .await?,
        )
        .await?;

        Ok(Self {
            write_segment: Some(write_segment),
            read_segments,
            storage_directory: storage_directory_path.as_ref().to_path_buf(),
            config,
            segment_creator,
        })
    }

    /// Reopens i.e. closes and opens the current write segment. This might be useful in
    /// syncing changes to persistent media.
    ///
    /// ## Errors
    /// - [`SegmentedLogError::WriteSegmentLost`]: if the write segment [`Option`] evaluates
    /// to [`None`].
    /// - [`SegmentedLogError::SegmentError`]: if there is and error during opening or closing
    /// the write segment.
    pub async fn reopen_write_segment(&mut self) -> Result<(), SegmentedLogError<T, S>> {
        let write_segment = self
            .write_segment
            .take()
            .ok_or(SegmentedLogError::WriteSegmentLost)?;

        let write_segment_base_offset = write_segment.base_offset();

        write_segment
            .close()
            .await
            .map_err(SegmentedLogError::SegmentError)?;

        self.write_segment = Some(
            self.segment_creator
                .new_segment_with_storage_dir_offset_and_config(
                    &self.storage_directory,
                    write_segment_base_offset,
                    self.config.segment_config,
                )
                .await
                .map_err(SegmentedLogError::SegmentError)?,
        );

        Ok(())
    }

    /// Removes segments older than the given `expiry_duration`.
    ///
    /// ## Mechanism for removal
    /// If the write segment is expired, we move it to the end of the vector of read segments,
    /// and in it's place create a new write segment with `base_offset` as the old write
    /// segment's `next_offset`. Note that this is done without reopening the write segment so
    /// as to avoid updating the creation time of the write segment. Also we don't care about
    /// sync-ing its contents since it's due for removal anyway.
    ///
    /// All the read segments are appended at the end of the vector of read segments. Hence
    /// by definition the read segment vector is sorted in descending order of age. First we
    /// look for the first non expired segment in the read segments vector. Once we find it, it
    /// means that, all the segments before it are expired. Hence we split off the vector at
    /// that point. This enables us to separate the expired read segments. Next we attempt to
    /// remove them on by one.
    ///
    /// If there is any error in removing a segment, we stop and move back the remaining
    /// expired read segments to vector of read segments, preserving chronology, so that we can
    /// try again later.
    ///
    /// ## Errors
    /// - [`SegmentedLogError::SegmentError`]: if there is an error during closing a segment.
    /// - [`SegmentedLogError::WriteSegmentLost`]: if the current write segment [`Option`]
    /// evaluates to [`None`].
    pub async fn remove_expired_segments(
        &mut self,
        expiry_duration: Duration,
    ) -> Result<(), SegmentedLogError<T, S>> {
        if write_segment_ref!(self, as_ref)?.creation_time().elapsed() >= expiry_duration {
            let new_segment_base_offset = write_segment_ref!(self, as_ref)?.next_offset();
            let old_write_segment = self.write_segment.replace(
                self.segment_creator
                    .new_segment_with_storage_dir_offset_and_config(
                        &self.storage_directory,
                        new_segment_base_offset,
                        self.config.segment_config,
                    )
                    .await
                    .map_err(SegmentedLogError::SegmentError)?,
            );

            self.read_segments
                .push(old_write_segment.ok_or(SegmentedLogError::WriteSegmentLost)?);
        }

        let first_non_expired_segment_position = self
            .read_segments
            .iter()
            .position(|x| x.creation_time().elapsed() < expiry_duration);

        let non_expired_read_segments =
            if let Some(first_non_expired_segment_position) = first_non_expired_segment_position {
                self.read_segments
                    .split_off(first_non_expired_segment_position)
            } else {
                Vec::new()
            };

        let mut expired_read_segments =
            std::mem::replace(&mut self.read_segments, non_expired_read_segments);

        let mut remove_result = Ok(());

        while !expired_read_segments.is_empty() {
            let expired_segment = expired_read_segments.remove(0);

            remove_result = expired_segment.remove().await;
            if remove_result.is_err() {
                break;
            }
        }

        if remove_result.is_err() && !expired_read_segments.is_empty() {
            expired_read_segments.append(&mut self.read_segments);
            self.read_segments = expired_read_segments;
        }

        remove_result.map_err(SegmentedLogError::SegmentError)
    }

    /// Advances this segmented log to the given new highest offset.
    ///
    /// This is useful in the scenario where multiple [`SegmentedLog`] instances are opened for
    /// the same backing files. This enables the [`SegmentedLog`] instances lagging behind to
    /// refresh their state to ensure that all persisted records upto the given offset can be
    /// read from them. This also helps [`SegmentedLog`] to ensure that no records are
    /// overwritten in situations like these.
    ///
    /// ## Mechanism for advancement
    /// Before doing anything we reopen the write segment of this [`SegmentedLog`] to refresh
    /// stale `highest_offset` if necessary.
    ///
    /// If the given `new_highest_offset <= self.highest_offset()` we do nothing and simply
    /// return Ok(()). Otherwise we have the following situation:
    /// - while The given `new_highest_offset` is beyond the capacity of the current write
    /// segment.
    ///     - if the size of the current write segment is zero
    ///         - this implies that the records upto the given offset have not been persisted on
    ///         the disk and the given new_highest_offset cannot be trusted. So we error out
    ///         with [`SegmentedLogError::OffsetNotValidToAdvanceTo`]
    ///     - we try to accommodate the offset by rotating the write segment
    /// - once the offset is accommodated, we attempt to advance the `next_offset` of the write
    /// segment to the given `new_highest_offset`. If there is an error, we error out with
    /// [`SegmentedLogError::SegmentError`].
    ///
    /// ## Returns
    /// - [`SegmentedLogError::OffsetNotValidToAdvanceTo`]: if the given offset is not a valid
    /// offset to advance to.
    /// - [`SegmentedLogError::SegmentError`]: if there is an error during advancing the offset
    /// ofthe write segment.
    pub async fn advance_to_offset(
        &mut self,
        new_highest_offset: u64,
    ) -> Result<(), SegmentedLogError<T, S>> {
        self.reopen_write_segment().await?;

        if new_highest_offset <= write_segment_ref!(self, as_ref)?.next_offset() {
            return Ok(());
        }

        while write_segment_ref!(self, as_ref)?
            .store_position(new_highest_offset)
            .is_none()
        {
            if write_segment_ref!(self, as_ref)?.size() == 0 {
                return Err(SegmentedLogError::OffsetNotValidToAdvanceTo);
            }

            self.rotate_new_write_segment().await?;
        }

        write_segment_ref!(self, as_mut)?
            .advance_to_offset(new_highest_offset)
            .map_err(SegmentedLogError::SegmentError)?;

        Ok(())
    }
}

#[doc(hidden)]
macro_rules! consume_segments_from_segmented_log_with_method {
    ($segmented_log:ident, $method:ident) => {
        let mut segments = $segmented_log.read_segments;

        segments.push(
            $segmented_log
                .write_segment
                .take()
                .ok_or(SegmentedLogError::WriteSegmentLost)?,
        );

        for segment in segments {
            segment
                .$method()
                .await
                .map_err(SegmentedLogError::SegmentError)?;
        }
    };
}

#[async_trait(?Send)]
impl<T, S, SegC> super::CommitLog for SegmentedLog<T, S, SegC>
where
    T: Deref<Target = [u8]>,
    S: store::Store<T>,
    SegC: SegmentCreator<T, S>,
{
    type Error = SegmentedLogError<T, S>;

    async fn append(&mut self, record_bytes: &[u8]) -> Result<u64, Self::Error> {
        while self.is_write_segment_maxed()? {
            self.rotate_new_write_segment().await?;
        }

        self.write_segment
            .as_mut()
            .ok_or(SegmentedLogError::WriteSegmentLost)?
            .append(record_bytes)
            .await
            .map_err(SegmentedLogError::SegmentError)
    }

    async fn read(&self, offset: u64) -> Result<(Record, u64), Self::Error> {
        if !self.offset_within_bounds(offset) {
            return Err(SegmentedLogError::OffsetOutOfBounds);
        }

        let read_segment = self
            .read_segments
            .iter()
            .find(|x| x.store_position(offset).is_some());

        if let Some(read_segment) = read_segment {
            read_segment
                .read(offset)
                .await
                .map_err(SegmentedLogError::SegmentError)
        } else {
            self.write_segment
                .as_ref()
                .ok_or(SegmentedLogError::WriteSegmentLost)?
                .read(offset)
                .await
                .map_err(SegmentedLogError::SegmentError)
        }
    }

    fn highest_offset(&self) -> u64 {
        self.write_segment
            .as_ref()
            .map(|x| x.next_offset())
            .unwrap_or(self.config.initial_offset)
    }

    fn lowest_offset(&self) -> u64 {
        self.read_segments
            .first()
            .or(self.write_segment.as_ref())
            .map(|x| x.base_offset())
            .unwrap_or(self.config.initial_offset)
    }

    async fn remove(mut self) -> Result<(), Self::Error> {
        consume_segments_from_segmented_log_with_method!(self, remove);
        fs::remove_dir_all(self.storage_directory).map_err(SegmentedLogError::IoError)?;
        Ok(())
    }

    async fn close(mut self) -> Result<(), Self::Error> {
        consume_segments_from_segmented_log_with_method!(self, close);
        Ok(())
    }
}

pub mod common {
    //! Module providing utilities used by [`SegmentedLog`](super::SegmentedLog).

    use super::{
        config::SegmentedLogConfig, segment::config::SegmentConfig,
        store::common::STORE_FILE_EXTENSION, Segment, SegmentCreator, SegmentedLogError,
    };
    use std::{
        fs, io,
        ops::Deref,
        path::{Path, PathBuf},
    };

    type Error<T, S> = SegmentedLogError<T, S>;

    /// Returns the backing [`Store`](crate::commit_log::store::Store) file path, with the
    /// given Log storage directory and the given segment base offset.
    #[inline]
    pub fn store_file_path<P: AsRef<Path>>(storage_dir_path: P, offset: u64) -> PathBuf {
        storage_dir_path.as_ref().join(format!(
            "{}.{}",
            offset,
            super::store::common::STORE_FILE_EXTENSION
        ))
    }

    /// Returns an iterator of the paths of all the [`crate::commit_log::store::Store`] backing
    /// files in the given directory.
    ///
    /// ## Errors
    /// - This functions returns an error if the given storage directory doesn't exist and
    /// couldn't be created.
    pub fn obtain_store_files_in_directory<P: AsRef<Path>>(
        storage_dir_path: P,
    ) -> io::Result<impl Iterator<Item = PathBuf>> {
        fs::create_dir_all(&storage_dir_path).and(fs::read_dir(&storage_dir_path).map(
            |dir_entries| {
                dir_entries
                    .filter_map(|dir_entry| dir_entry.ok().map(|x| x.path()))
                    .filter_map(|path| {
                        (path.extension()?.to_str()? == STORE_FILE_EXTENSION).then(|| path)
                    })
            },
        ))
    }

    /// Returns the given store paths sorted by the base offset of the respective segments that
    /// they belong to.
    ///
    /// ## Returns
    /// A vector of (path, offset) tuples, sorted by the offsets.
    pub fn store_paths_sorted_by_offset(
        store_paths: impl Iterator<Item = PathBuf>,
    ) -> Vec<(PathBuf, u64)> {
        let mut store_paths = store_paths
            .filter_map(|segment_file_path| {
                let base_offset: u64 = segment_file_path.file_stem()?.to_str()?.parse().ok()?;

                Some((segment_file_path, base_offset))
            })
            .collect::<Vec<(PathBuf, u64)>>();

        store_paths.sort_by(|a, b| a.1.cmp(&b.1));
        store_paths
    }

    /// Returns a [`Vec<Segment>`] of all the segments stored in this directory.
    ///
    /// ## Errors
    /// - [`SegmentedLogError::IoError`]: if there was an error in locating segment files in
    /// the given directory.
    /// - [`SegmentedLogError::SegmentError`]: if there was an error in opening a segment.
    pub async fn segments_in_directory<T, S, SegC, P: AsRef<Path>>(
        storage_dir_path: P,
        segment_creator: &SegC,
        segment_config: SegmentConfig,
    ) -> Result<Vec<Segment<T, S>>, Error<T, S>>
    where
        T: Deref<Target = [u8]>,
        S: super::store::Store<T>,
        SegC: SegmentCreator<T, S>,
    {
        let segment_args = store_paths_sorted_by_offset(
            obtain_store_files_in_directory(storage_dir_path)
                .map_err(SegmentedLogError::IoError)?,
        );

        let mut segments = Vec::with_capacity(segment_args.len());

        for segment_arg in segment_args {
            segments.push(
                segment_creator
                    .new_segment_with_store_file_path_offset_and_config(
                        &segment_arg.0,
                        segment_arg.1,
                        segment_config,
                    )
                    .await
                    .map_err(SegmentedLogError::SegmentError)?,
            );
        }

        Ok(segments)
    }

    /// Seperates the read segments and write segments in the given [`Vec<Segment>`].
    ///
    /// There are the following cases that can happen here:
    /// - If the given vector of segments is empty, we return an empty vector of read segments
    /// and a new write segment.
    /// - If the last segment is the given list of segment is maxed out, we return the given
    /// vector of segments as is as the vector of read segments, along with a new write
    /// segment.
    /// - Otherwise we pop out the last segment from the given vector of segments. The shorter
    /// vector of segments is returned as the vector of read segments along with the popped out
    /// segment as the write segment.
    ///
    /// ## Errors
    /// - [`SegmentedLogError::SegmentError`]: if there was an error in creating a new segment.
    /// (When creating the write segment)
    /// - [SegmentedLogError::WriteSegmentLost]: if the popped last segments from the given
    /// list of segments is `None` when `segments.last()` returned a [`Some(_)`].
    pub async fn read_and_write_segments<T, S, SegC, P: AsRef<Path>>(
        storage_dir_path: P,
        segment_creator: &SegC,
        log_config: SegmentedLogConfig,
        mut segments: Vec<Segment<T, S>>,
    ) -> Result<(Vec<Segment<T, S>>, Segment<T, S>), Error<T, S>>
    where
        T: Deref<Target = [u8]>,
        S: super::store::Store<T>,
        SegC: SegmentCreator<T, S>,
    {
        Ok(match segments.last() {
            Some(last_segment) if last_segment.is_maxed() => {
                let segment_base_offset = last_segment.next_offset();
                (
                    segments,
                    segment_creator
                        .new_segment_with_storage_dir_offset_and_config(
                            storage_dir_path,
                            segment_base_offset,
                            log_config.segment_config,
                        )
                        .await
                        .map_err(SegmentedLogError::SegmentError)?,
                )
            }
            Some(_) => {
                let last_segment = segments.pop();
                (
                    segments,
                    last_segment.ok_or(SegmentedLogError::WriteSegmentLost)?,
                )
            }
            None => (
                Vec::new(),
                segment_creator
                    .new_segment_with_storage_dir_offset_and_config(
                        storage_dir_path,
                        log_config.initial_offset,
                        log_config.segment_config,
                    )
                    .await
                    .map_err(SegmentedLogError::SegmentError)?,
            ),
        })
    }
}

pub mod config {
    //! Module providing types for configuring a [`SegmentedLog`](super::SegmentedLog).
    use serde::{Deserialize, Serialize};

    use super::segment::config::SegmentConfig;

    /// Log specific configuration.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
    pub struct SegmentedLogConfig {
        /// Offset from which the first segment of the log starts.
        pub initial_offset: u64,
        /// Config for every segment in this log.
        pub segment_config: SegmentConfig,
    }
}
