package com.dbjr.prac.utils

import java.io.Serializable

import scala.collection.mutable.ListBuffer

/**
 * Created by debjyoti on 05/15/17.
 * Email - debjyoti.roy@gmail.com
 */

case class Segment(start: Long, end: Long) extends Serializable{
}

case class OffsetData(prevPlayType: String, 
                      prevEpp: Long,
                      updatedTimeFragList : ListBuffer[Segment],
                      distinctTime: Long, 
                      distinctTimeSum: Long, 
                      cumulativeTime: Long, 
                      cumulativeTimeSum: Long,
                      tuneInFlag: Short, 
                      distinctWatchedTimePercentage: Double,
                      totalDistinctWatchedTimePercentage: Double) extends Serializable{
}

case class VideoRecord(playId: String,
                       latestEventTimestamp: Long,
                       cumulativeViewTime: Long,
                       distinctViewedTime: Long,
                       distinctTimeFragmentList: ListBuffer[Segment],
                       distinctViewedTimePercentage: Double,
                       completionFlag: Short,
                       completetionTracker: Long,
                       lastevent_deviceTimestamp: Long,
                       lastOffset : Long,
                       lastevent_eventType: String,
                       tuneInFlag: Short,
                       userId: String) extends Serializable



