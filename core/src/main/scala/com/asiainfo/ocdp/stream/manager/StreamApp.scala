package com.asiainfo.ocdp.stream.manager

import com.asiainfo.ocdp.stream.common.Logging
import com.asiainfo.ocdp.stream.config.MainFrameConf
import com.asiainfo.ocdp.stream.service.TaskServer
import org.apache.spark.streaming.{StreamingContextState, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by leo on 9/16/15.
 */
object StreamApp extends Logging {

  val delaySeconds = MainFrameConf.systemProps.getInt("delaySeconds", 10)
  val periodSeconds = MainFrameConf.systemProps.getInt("periodSeconds", 60)
  val taskServer = new TaskServer

  def main(args: Array[String]) {

    if (args.length < 1) {
      //      System.err.println("usage:  ./bin/spark-class com.asiainfo.ocdc.stream.mainframe [taskId]")
      System.exit(1)
    }

    val taskId = args(0)

    val taskConf = taskServer.getTaskInfoById(taskId)

    //1 初始化 streamingContext
    val sparkConf = new SparkConf().setAppName(taskConf.getName)
    val sc = new SparkContext(sparkConf)
    val listener = new AppStatusUpdateListener(taskConf.getId)
    sc.addSparkListener(listener)

    //2 启动 streamingContext
    val ssc = new StreamingContext(sc, Seconds(taskConf.getReceive_interval))
    //    ssc.addStreamingListener(new ReceiveRecordNumListener())
    new TaskStopManager(ssc, taskConf.getId)

    StreamTaskFactory.getStreamTask(taskConf).process(ssc)

    ssc.start()

    //4 update task status in db
    if (StreamingContextState.ACTIVE == ssc.getState()) {
      taskServer.startTask(taskConf.getId)
      logInfo("Start task " + taskConf.getId + " sucess !")
    }

    ssc.awaitTermination()

    taskServer.stopTask(taskConf.getId)
    logInfo("Stop task " + taskConf.getId + " sucess !")

    sys.exit()
  }

}