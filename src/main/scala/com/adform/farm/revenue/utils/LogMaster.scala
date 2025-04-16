package com.adform.farm.revenue.utils
import org.slf4j.{Logger, LoggerFactory}
trait LogMaster {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def logInfo(message: String): Unit = {
    logger.info(message)
  }

  def logWarn(message:String): Unit = {
    logger.warn(message)
  }

  def logError(message:String, internal:Option[Throwable]): Unit =  {
    internal match {
      case Some(e) => logger.error(message, e)
      case None => logger.error(message)
    }
  }

}
