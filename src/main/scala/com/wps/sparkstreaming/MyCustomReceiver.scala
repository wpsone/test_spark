package com.wps.sparkstreaming

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

class MyCustomReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK) with Logging{
  override def onStart(): Unit = {
    // 启动一个线程，开始接收数据
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()

  }

  override def onStop(): Unit = ???

  private def receive(): Unit = {
    var socket:Socket = null
    var userInput:String = null
    try {
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host,port)
      logInfo("Connected to " + host + ":" + port)

      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while (!isStopped() && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      logInfo("Stopped receiving")
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException => restart("Error connecting to " + host + ":" + port,e)
      case t:Throwable => restart("Error receiving data",t)
    }
  }
}
