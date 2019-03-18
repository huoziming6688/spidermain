import logging
import getpass
import sys

class MyLog(object):
    def __init__(self):
        """这个类用于创建一个自用的log"""
        user=getpass.getuser()
        self.logger=logging.getLogger(user)
        self.logger.setLevel(logging.DEBUG)
        logFile=sys.argv[0][0:-3]+'.log' #日志文件名
        formatter=logging.Formatter('%(asctime)-12s %(levelname)-8s %(message)-12s')
        """日志显示到屏幕上并输出到日志文件夹内"""
        logHand=logging.FileHandler(logFile)
        logHand.setFormatter(formatter)
        logHand.setLevel(logging.INFO) #只有错误才会被记录到logfile中

        logHandSt=logging.StreamHandler()
        logHandSt.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(logHand)
            self.logger.addHandler(logHandSt)

    """日志的5个级别对应以下的5个函数"""
    def debug(self,msg):
        self.logger.debug(msg)
     

    def info(self,msg):
        self.logger.info(msg)
       


    def warning(self,msg):
        self.logger.warning(msg)
       

    def error(self,msg):
        self.logger.error(msg)
       

    def critical(self,msg):
        self.logger.critical(msg)


if __name__ == '__main__':
    mylog=MyLog()
    mylog.debug("我是一个debug")
    mylog.info("我是一个info")
    mylog.warning("我是一个warn")
    mylog.error("我是一个error")
    mylog.critical("我是一个critical")