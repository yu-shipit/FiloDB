package filodb.core.store

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.concurrent.Future

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core._
import filodb.core.metadata.Schemas
import filodb.memory.format.UnsafeUtils

case class PartKeyRecord(partKey: Array[Byte], startTime: Long, endTime: Long, shard: Int)

object PartKeyRecord {
  def getBucket(partKey: Array[Byte], schemas: Schemas, numBuckets: Int): Int = {
    val hash = schemas.part.binSchema.partitionHash(partKey, UnsafeUtils.arayOffset)
    (hash & Int.MaxValue) % numBuckets
  }
}

/**
 * ChunkSink is the base trait for a sink, or writer to a persistent store, of chunks
 */
trait ChunkSink {
  def sinkStats: ChunkSinkStats

  /**
   * Writes the ChunkSets appearing in a stream/Observable to persistent storage, with backpressure
   * @param ref the DatasetRef for the chunks to write to
   * @param chunksets an Observable stream of chunksets to write
   * @param diskTimeToLive the time for chunksets to live on disk (Cassandra)
   * @return Success when the chunksets stream ends and is completely written.
   *         Future.failure(exception) if an exception occurs.
   */
  def write(ref: DatasetRef, chunksets: Observable[ChunkSet], diskTimeToLive: Long = 259200): Future[Response]

  /**
    * Used to bootstrap lucene index with partition keys for a shard
    */
  def scanPartKeys(ref: DatasetRef, shard: Int): Observable[PartKeyRecord]

  /**
    * Used by downsample shard to do periodic pulls of new partition keys
    * into lucene index
    *
    * @param updateHour hour since epoch, essentially millis / 1000 / 60 / 60
    */
  def getPartKeysByUpdateHour(ref: DatasetRef,
                              shard: Int,
                              updateHour: Long): Observable[PartKeyRecord]

  def writePartKeyUpdates(ref: DatasetRef,
                          epoch5mBucket: Long,
                          updatedTimeMs: Long,
                          offset: Long,
                          tagSet: TagSet,
                          partKeys: Observable[PartKeyRecord]): Future[Response]

  /**
   * Can be used by any downstream applications who is using this library. Also helpful for testing purposes.
   * */
  def getUpdatedPartKeysByTimeBucket(ref: DatasetRef,
                                     shard: Int,
                                     updateHour: Long): Observable[PartKeyRecord]

  def writePartKeys(ref: DatasetRef, shard: Int,
                    partKeys: Observable[PartKeyRecord], diskTTLSeconds: Long,
                    updateHour: Long, writeToPkUTTable: Boolean = true): Future[Response]
  /**
   * Initializes the ChunkSink for a given dataset.  Must be called once before writing.
   */
  def initialize(dataset: DatasetRef, numShards: Int, resources: Config): Future[Response]

  /**
   * Truncates/clears all data from the ChunkSink for that given dataset.
   * NOTE: please make sure there are no writes going on before calling this
   */
  def truncate(dataset: DatasetRef, numShards: Int): Future[Response]

  /**
   * Completely and permanently drops the dataset from the ChunkSink.
   * @param dataset the DatasetRef for the dataset to drop.
   */
  def dropDataset(dataset: DatasetRef, numShards: Int): Future[Response]

  /** Resets state, whatever that means for the sink */
  def reset(): Unit
}

/**
 * Stats for a ChunkSink
 */
class ChunkSinkStats {
  private val chunksPerCallHist  = Kamon.histogram("chunks-per-call").withoutTags
  private val chunkBytesHist     = Kamon.histogram("chunk-bytes-per-call").withoutTags
  private val chunkLenHist       = Kamon.histogram("chunk-length").withoutTags

  private val numIndexWriteCalls = Kamon.counter("index-write-calls-num").withoutTags
  private val indexBytesHist     = Kamon.histogram("index-bytes-per-call").withoutTags

  private val chunksetWrites     = Kamon.counter("chunkset-writes").withoutTags
  private val partKeysWrites     = Kamon.counter("partKey-writes").withoutTags

  // PartKeyUpdatesPublisher metrics
  private val partKeyUpdatesSuccess = Kamon.counter("partKey-updates-published")
  private val partKeyUpdatesError   = Kamon.counter("partKey-updates-failed")
  private val partKeyUpdatesLatency = Kamon.histogram("partKey-updates-latency", MeasurementUnit.time.milliseconds)

  val chunksetsWritten = new AtomicInteger(0)
  val partKeysWritten = new AtomicInteger(0)
  val partKeysUpdatesPublished = new AtomicInteger(0)

  def addChunkWriteStats(numChunks: Int, totalChunkBytes: Long, chunkLen: Int): Unit = {
    chunksPerCallHist.record(numChunks)
    chunkBytesHist.record(totalChunkBytes)
    chunkLenHist.record(chunkLen)
  }

  def addIndexWriteStats(indexBytes: Long): Unit = {
    numIndexWriteCalls.increment()
    indexBytesHist.record(indexBytes)
  }

  def chunksetWrite(): Unit = {
    chunksetWrites.increment()
    chunksetsWritten.incrementAndGet()
  }

  def partKeysWrite(numKeys: Int): Unit = {
    partKeysWrites.increment(numKeys)
    partKeysWritten.addAndGet(numKeys)
  }

  def partKeyUpdatesSuccess(num: Int, tagSet: TagSet): Unit = {
    partKeyUpdatesSuccess.withTags(tagSet).increment(num)
    partKeysUpdatesPublished.addAndGet(num)
  }

  def partKeyUpdatesFailed(num: Int, tagSet: TagSet): Unit = {
    partKeyUpdatesError.withTags(tagSet).increment(num)
  }

  def partKeyUpdatesLatency(latency: Long, tagSet: TagSet): Unit = {
    partKeyUpdatesLatency.withTags(tagSet).record(latency)
  }
}

/**
 * NullColumnStore keeps stats and partitions but other than that writes chunks nowhere.
 * It's convenient for testing though.
 */
class NullColumnStore(implicit sched: Scheduler) extends ColumnStore with StrictLogging {
  val sinkStats = new ChunkSinkStats
  val stats = new ChunkSourceStats

  // in-memory store of partition keys
  val partitionKeys = new ConcurrentHashMap[DatasetRef, scala.collection.mutable.Set[Types.PartitionKey]]().asScala

  def write(ref: DatasetRef, chunksets: Observable[ChunkSet], diskTimeToLive: Long): Future[Response] = {
    chunksets.foreach { chunkset =>
      val totalBytes = chunkset.chunks.map(_.limit()).sum
      sinkStats.addChunkWriteStats(chunkset.chunks.length, totalBytes, chunkset.info.numRows)
      sinkStats.chunksetWrite()
      logger.trace(s"NullColumnStore: [${chunkset.partition}] ${chunkset.info}  ${chunkset.chunks.length} " +
                   s"chunks with $totalBytes bytes")
      chunkset.listener(chunkset.info)
    }
    Future.successful(Success)
  }

  def initialize(dataset: DatasetRef, numShards: Int,
                 resources: Config): Future[Response] = Future.successful(Success)

  def truncate(dataset: DatasetRef, numShards: Int): Future[Response] = {
    partitionKeys -= dataset
    Future.successful(Success)
  }

  def dropDataset(dataset: DatasetRef, numShards: Int): Future[Response] = Future.successful(Success)

  def reset(): Unit = {
    partitionKeys.clear()
  }

  override def shutdown(): Unit = {}

  def readRawPartitions(ref: DatasetRef, maxChunkTime: Long,
                        partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod = AllChunkScan): Observable[RawPartData] = Observable.empty

  override def getScanSplits(dataset: DatasetRef, splitsPerNode: Int): Seq[ScanSplit] = Seq.empty

  override def scanPartKeys(ref: DatasetRef, shard: Int): Observable[PartKeyRecord] = Observable.empty

  override def writePartKeyUpdates(ref: DatasetRef, epoch5mBucket: Long, updatedTimeMs: Long, offset: Long,
                                   tagSet: TagSet,
                                   partKeys: Observable[PartKeyRecord]): Future[Response] = {
    partKeys.countL.map(c => sinkStats.partKeyUpdatesSuccess(c.toInt, TagSet.Empty)).runToFuture.map(_ => Success)
  }

  override def getUpdatedPartKeysByTimeBucket(ref: DatasetRef, shard: Int,
                                              updateHour: Long): Observable[PartKeyRecord] = Observable.empty

  override def writePartKeys(ref: DatasetRef, shard: Int,
                             partKeys: Observable[PartKeyRecord], diskTTLSeconds: Long,
                             updateHour: Long, writeToPkUTTable: Boolean = true): Future[Response] = {
    partKeys.countL.map(c => sinkStats.partKeysWrite(c.toInt)).runToFuture.map(_ => Success)
  }

  override def getPartKeysByUpdateHour(ref: DatasetRef, shard: Int,
                                       updateHour: Long): Observable[PartKeyRecord] = Observable.empty
}
