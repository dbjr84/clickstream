package com.dbjr.prac.processes

import com.dbjr.prac.utils._
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer


/**
 * Created by debjyoti on 05/15/17.
 * Email - debjyoti.roy@gmail.com
 */
trait TimeProcessor extends Serializable {
  private val LOGGER: Logger = LoggerFactory.getLogger("vid")

  /**
   * To process each sorted group of video events
   * @param rows
   * @param playId
   * @return
   */
  def processSortedRDD(rows: List[Row], playId: String): VideoRecord = {
    var newVidUsrRecord: VideoRecord = initVidUsrRec(playId)
    rows.map(row => {
      LOGGER.debug("PRINTING ROW" + row)
      val recAndTrackers: (VideoRecord, Long, Long, Double) =
        convertRowToVidUsrRec(row, newVidUsrRecord)
      newVidUsrRecord = recAndTrackers._1
    })
    newVidUsrRecord
  }


  /**
   * Initialize video per user record for operation
   * @param playId
   * @return
   */
  def initVidUsrRec(playId: String): VideoRecord = {
    val videoUserRecord: VideoRecord = new VideoRecord(
      playId,
      0L,
      0L,
      0L,
      ListBuffer[Segment](),
      0.0,
      0,
      0L,
      0L,
      0L,
      "",
      0,
      ""
    )
    videoUserRecord
  }


  /**
   * To update times calculations, trackers and flags
   * @param timeCalRes
   * @param row
   * @param completionFlag
   * @param completionTracker
   * @return
   */
  def updateTimesAndTrackers(timeCalRes: OffsetData, row: Row, completionFlag: Short,
                             completionTracker: Long): (VideoRecord, Long, Long, Double) = {

    val deviceTimestamp = row.getAs[Long]("deviceTimestamp")
    val profileId = row.getAs[String]("userId")
    val accountId = row.getAs[String]("accountId")
    val userId = accountId + "&" + profileId
    var resetFlag: Short = 0
    var tempCompletionTracker = completionTracker
    var tempCompletionFlag = completionFlag


    //updating completion tracker
    if (timeCalRes.totalDistinctWatchedTimePercentage >= 90) {
      resetFlag = 1
      tempCompletionTracker += 1
      tempCompletionFlag = 1
    }

    val updatedRec =
      new VideoRecord(row.getAs[String]("playId"),
        row.getAs[Long]("deviceTimestamp"), timeCalRes.cumulativeTimeSum, timeCalRes.distinctTimeSum,
        timeCalRes.updatedTimeFragList, timeCalRes.totalDistinctWatchedTimePercentage,
        tempCompletionFlag, tempCompletionTracker, deviceTimestamp,
        timeCalRes.prevEpp, timeCalRes.prevPlayType, timeCalRes.tuneInFlag, userId)

    (updatedRec, timeCalRes.distinctTime, timeCalRes.cumulativeTime, timeCalRes.distinctWatchedTimePercentage)
  }


  /**
   * Convert RDD row to video per user record 
   * @param row
   * @param prevVidUsrRec
   * @return
   */
  def convertRowToVidUsrRec(row: Row, prevVidUsrRec: VideoRecord): (VideoRecord, Long, Long, Double) = {
    val videoStartTime = row.getAs[Long]("videoStartTime")
    val videoEndTime = row.getAs[Long]("videoEndTime")
    val clickType = row.getAs[String]("clickType").trim
    val currentEpp = row.getAs[Long]("programOffset") - videoStartTime //epoch time
    val timeCalRes: OffsetData = VideoUtils.calculateSegments(prevVidUsrRec.lastevent_eventType, clickType,
        prevVidUsrRec.lastOffset, currentEpp, prevVidUsrRec.distinctTimeFragmentList, prevVidUsrRec.distinctViewedTime,
        prevVidUsrRec.cumulativeViewTime, videoStartTime, videoEndTime, prevVidUsrRec.tuneInFlag)

    val newVidUsrRec: (VideoRecord, Long, Long, Double) = updateTimesAndTrackers(timeCalRes, row,
      prevVidUsrRec.completionFlag, prevVidUsrRec.completetionTracker)

    newVidUsrRec
  }

}
object TimeProcessor extends Serializable{
  def apply() = new TimeProcessor(){}
}


