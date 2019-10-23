import org.slf4j.{Logger, LoggerFactory}

object LogUtils {



  def getLogger(className: String): Logger = {

    val logger = LoggerFactory.getLogger(className)
    logger
  }
}