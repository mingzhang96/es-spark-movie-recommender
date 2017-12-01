* [English](#a-recommender-based-on-elasticsearch-&-spark)
* [中文](#基于elasticsearch和spark的推荐系统)

## A Recommender based on Elasticsearch & Spark

> According to [@IBM 's work](https://github.com/IBM/elasticsearch-spark-recommender), do some processing and localizing.

### Change

Divide original code into two part:
* pre-recommend.py ==> getting data, training model and save into es
* recommend.py ==> how to search using plugin and show result

Different from that @IBM 's work is based on jupyter book, now you can directly open this project on pycharm and run it.
However it is a shame that pycharm can not show poster on console.

### Start

1. Follow [@IBM 's work](https://github.com/IBM/elasticsearch-spark-recommender) and build environment.
    * ES 5.3.0
    * Spark 2.2.0

2. In order to run in pycharm, you need to do something:
    * RUN -> Edit Configurations -> Environment Variables
    ```
    PYSPARK_PYTHON /usr/local/bin/python3
    PYSPARK_DRIVER_PYTHON /usr/local/bin/ipython
    SPARK_HOME /usr/local/spark
    ```

3. Move `elasticsearch-spark-20_2.11-5.3.0.jar` into $SPARK_HOME/jars

4. Change `API_KEY` in `pre-recommend.py` & `recommend.py`

5. Run!


## 基于Elasticsearch和Spark的推荐系统

> 根据[IBM/elasticsearch-spark-recommender](https://github.com/IBM/elasticsearch-spark-recommender)做了代码提取和本地实现。

#### 改变

将原本的代码拆分成了两部分：
* pre-recommend.py 实现了数据提取、插入es、训练模型并插入es
* recommend.py 实现了具体查询过程

因为原本的代码是在`jupyter book`上的，现在提取之后可以直接在pycharm运行，只是不能显示最后的海报图片内容。

#### 使用

1. 先按照[IBM/elasticsearch-spark-recommender](https://github.com/IBM/elasticsearch-spark-recommender)搭建
    * ES 5.3.0
    * Spark 2.2.0

2. 为了在pycharm中运行，进行如下配置：
    * RUN -> Edit Configurations 设置环境变量
    ```
    PYSPARK_PYTHON /usr/local/bin/python3
    PYSPARK_DRIVER_PYTHON /usr/local/bin/ipython
    SPARK_HOME /usr/local/spark
    ```

3. 将之前下好的`elasticsearch-spark-20_2.11-5.3.0.jar`放到`spark`的安装目录下的`jars`目录下。

4. 修改`pre-recommend.py`和`recommend.py`中的`API_KEY`为你自己的`key`。

5. 开始运行！

