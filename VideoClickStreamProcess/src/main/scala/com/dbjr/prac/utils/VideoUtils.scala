package com.dbjr.prac.utils


import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone}
/**
 * Created by debjyoti on 05/15/17.
 * Email - debjyoti.roy@gmail.com
 */
object VideoUtils extends Serializable{
  private val LOGGER: Logger = LoggerFactory.getLogger("vid");
  val startEvents: mutable.Set[String] = collection.mutable.Set[String]("tuneIn","resume","restart","stillTuned","prevShow","nextShow")
  val nonStartEvents = collection.mutable.Set[String]("tuneOut","rewind","forward","pause")


  /**
   * To calculate percentage by given numerator and denominator
   * @param numer
   * @param denom
   * @return
   */
  def calculatePercentage(numer: Long, denom: Long): Double = {

    val resPer: Double = if(denom > 0) {
      val resVal = (numer.toDouble / denom.toDouble) * 100
      BigDecimal(resVal).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    }else {
      0.0
    }
    resPer
  }

  /**
   * To decide the program starting point when the event type is restart
   * @param currEpp
   * @param progStartTime
   * @return
   */
  def getProgStartTime(currEpp : Long, progStartTime: Long): Long = {
    if(currEpp.toString.length > 6){
      progStartTime
    }else{
      0L
    }
  }

  /**
   * calculating distinct time and cumulative time
   * @param prevClickType
   * @param clickType
   * @param prevOffset
   * @param currentOffset
   * @param inpList
   * @param distinctTimeSum
   * @param cumulativeTimeSum
   * @param videoStartTime
   */
  def calculateSegments(prevClickType: String, clickType: String, prevOffset: Long, currentOffset: Long,
                             inpList : ListBuffer[Segment], distinctTimeSum: Long, cumulativeTimeSum: Long,
                             videoStartTime: Long, videoEndTime: Long, startingFlag: Short):  OffsetData={
    val assetLength = videoEndTime - videoStartTime
    var  distinctFragments = inpList
    var tempTuneInFlag = startingFlag
    var distinctTime = 0L
    var cumulativeTime = 0L
    var totalDistinctTime = distinctTimeSum
    var totalCumulativeTime = cumulativeTimeSum
    var tempPrevOffset = prevOffset
    var tempPrevClickType = prevClickType

    if (!tempPrevClickType.isEmpty && tempTuneInFlag == 1) {
      if(startEvents.contains(tempPrevClickType)){
        if(currentOffset > tempPrevOffset){
          val timefragment = new Segment(tempPrevOffset,currentOffset)
          val res = updateSegments(timefragment, distinctFragments)
          distinctFragments = res._1
          distinctTime = res._2
          totalDistinctTime += distinctTime
        }
        cumulativeTime = Math.abs(currentOffset - tempPrevOffset)
        totalCumulativeTime += cumulativeTime
      }
      if(clickType.equals("restart")){
        val progStartPoint = getProgStartTime(currentOffset, videoStartTime)
        tempPrevOffset = progStartPoint
      }else{
        tempPrevOffset = currentOffset
      }
      tempPrevOffset = currentOffset
      tempPrevClickType = clickType
    }else{
      LOGGER.info("1st event")
      if(clickType.equals("tuneIn")){
        tempPrevOffset = currentOffset //epoch
        tempPrevClickType = clickType
        tempTuneInFlag = 1
      }else{
        LOGGER.info("BAD RECORD")
      }
    }

    //calculate view percentage
    val distinctViewedTimePercentage = calculatePercentage(distinctTime, assetLength)
    val totalDistinctViewedTimePercentage: Double = calculatePercentage(totalDistinctTime, assetLength)
    val res: OffsetData =  OffsetData(tempPrevClickType, tempPrevOffset, distinctFragments,
      distinctTime, totalDistinctTime, cumulativeTime, totalCumulativeTime, tempTuneInFlag,
      distinctViewedTimePercentage, totalDistinctViewedTimePercentage)
    res
  }

  /**
   * Update list of distinct time fragments and merge overlapping fragments. It's modified insertion sort logic.
   * @param timeFragment
   * @param inpList
   * @return
   */
  def updateSegments(timeFragment : Segment, inpList : ListBuffer[Segment]): (ListBuffer[Segment], Long) ={
    var fragmentList = inpList
    var distinctTime = 0L
    if(fragmentList.size > 0){
      var movingIndex: Int = 0
      var newFrame : Segment = timeFragment
      breakable{
        for(i <- 0 to (fragmentList.size - 1)){
          val searchedFrag = fragmentList(i)

          if(searchedFrag.start <= newFrame.start && newFrame.start <= searchedFrag.end){
            if(searchedFrag.end < newFrame.end){
              newFrame = new Segment(searchedFrag.end, newFrame.end)
              movingIndex += 1
            }else{
              break
            }
          }else if(newFrame.start <= searchedFrag.start && searchedFrag.start <= newFrame.end){
            val updated = new Segment(newFrame.start, searchedFrag.end)
            fragmentList.update(i,updated)
            distinctTime += searchedFrag.start - newFrame.start
            if(searchedFrag.end < newFrame.end){
              newFrame = Segment(searchedFrag.end, newFrame.end)
            }else{
              break
            }
            movingIndex += 1
          }else if(newFrame.end < searchedFrag.start){
            fragmentList.insert(i, newFrame)
            distinctTime += newFrame.end - newFrame.start
            break
          }else if(searchedFrag.end < newFrame.start){
            //insertion position not found yet, loop continues forward,
            movingIndex += 1

          }
          if(movingIndex == fragmentList.size){
            fragmentList += newFrame
            distinctTime += newFrame.end - newFrame.start
            break
          }
        }
      }
    }else{
      fragmentList += timeFragment
      distinctTime = timeFragment.end - timeFragment.start
    }
    (fragmentList,distinctTime)
  }

  def getTimeWithZone(epochTime: Long, timeZone: String): String = {
    if (null == timeZone || !DateTimeZone.getAvailableIDs.contains(timeZone.trim)) {
      val zoneTime: DateTime = new DateTime(epochTime).withZone(DateTimeZone.forID("GMT"))
      zoneTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ"))
    }
    else {
      val zoneTime = new DateTime(epochTime).withZone(DateTimeZone.forID(timeZone))
      zoneTime.toString(ISODateTimeFormat.dateTime())
    }
  }
}
