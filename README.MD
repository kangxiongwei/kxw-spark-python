#使用Python学习Spark

##1.MacOS中学习环境搭建
```markdown
1. 下载Python3.6和Spark2.3.0并安装
2. 本地安装Pycharm开发工具
3. 启动hadoop，spark，分别运行各自sbin下的start-all.sh文件
4. 环境变量增加以下内容：
    
    4.1 export SPARK_HOME="/Users/kangxiongwei/JavaSoft/spark-2.3.0"
    
    4.2 修改spark的conf中的spark-env.sh文件
        export HADOOP_CONF_DIR=/Users/kangxiongwei/JavaSoft/hadoop-2.6.5/etc/hadoop
        export YARN_CONF_DIR=/Users/kangxiongwei/JavaSoft/hadoop-2.6.5/etc/hadoop
        export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home
        export SPARK_MASTER_IP=127.0.0.1
        export SPARK_MASTER_PORT=7077
    
    4.3 拷贝hadoop中lib/native/libhadoop.dylib文件，到${JAVA_HOME}/jre/lib
    
    4.4 export PYSPARK_PYTHON="/usr/local/bin/python3"
        export PYSPARK_DRIVER_PYTHON="/usr/local/bin/python3"
    
    4.5 export OPENSSL_ROOT_DIR="/usr/local/Cellar/openssl/1.0.2o_1"
        export OPENSSL_INCLUDE_DIR="/usr/local/Cellar/openssl/1.0.2o_1/include"
    
    4.6 追加以上变量到path中
    
    4.7 cp -r ${SPARK_HOME}/python/pyspark /Library/Frameworks/Python.framework/Versions/3.6/lib/python3.6/site-packages
    
    4.8 修改pycharm中Edit Configurations中的环境变量：
        SPARK_HOME = /Users/kangxiongwei/JavaSoft/spark-2.3.0
        PYTHONPATH = /Users/kangxiongwei/JavaSoft/spark-2.3.0/python
        
    4.9 编写RDD程序，测试是否OK
```
