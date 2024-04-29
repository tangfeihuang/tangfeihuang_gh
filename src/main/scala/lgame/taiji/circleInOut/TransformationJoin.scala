package lgame.taiji.circleInOut

import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, posexplode, posexplode_outer, split, udf}
import org.apache.spark.sql.functions.{col, column, explode, lit, split, udf}
import org.apache.spark.internal.config
import xdata.mining.util.tdw.Utils.StringToColumn


object TransformationJoin {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder
      .config("spark.driver.memory", "4g")
      .config("spark.executor.num", 20)
      .config("spark.executor.cores", 1)
      .config("spark.executor.memory", "3g")
      // .config("spark.sql.autoBroadcastJoinThreshold", -1)
      // 模拟设置GROUP_ID和GAIA_ID为Spark配置，而非环境变量
      .config("GROUP_ID", "g_ieg_wepop_xmomi")
      .config("GAIA_ID", "8628")
      .getOrCreate()

    /**
     * 读数据
     */
    // 定义列名列表
    val idCols = List("stat_yymmddhh", "roomid", "roleid", "heroid", "gametime")
    val filterCols = List("playerproficiencylv", "playerherotype", "grade", "camp", "battleresult")
    val nCoordCols = List("playerxaxisnew", "playeryaxisnew")
    val eCoordCols = List("playerdeadtimenew", "playerdeadxaxisnew", "playerdeadyaxisnew")

    // 合并所需列
    val requiredCols = idCols ++ filterCols ++ nCoordCols ++ eCoordCols

    val tdwXmomi = new TDWSQLProvider(spark, dbName = "ieg_lgame_skynet_application", user = config.USER, passwd = config.PASSWD)
    val tdwUtilXmomi = new TDWUtil(user = config.USER, passwd = config.PASSWD, dbName = "ieg_lgame_skynet_application")
    val tableInfo = tdwUtilXmomi.getTableInfo("lgame_actor_detection_combine_unique_hour")
    val origin = tdwXmomi.table("lgame_actor_detection_combine_unique_hour", List("p_2023112112")).selectExpr(requiredCols.toString()).cache()

    val OFFSET = 0
    val CENTRE_X_LS = List(8000, 12000)
    val CENTRE_Y_LS = List(1400, 18000)
    val RADIUS_LS = List(500, 300)

    // 处理正常坐标列
    var nCoordSplit = List.empty[DataFrame]
    for (col <- nCoordCols) {
      var t = origin.withColumn(col, split(functions.col(col), ","))
      t = (t select("roomid", "roleid", posexplode(functions.col(col)))).alias("idx", col)
      nCoordSplit :+= t
    }

    // 处理结束坐标列
    var nSplitDf = nCoordSplit.reduce(_.join(_, Seq("roomid", "roleid", "idx")))
    nSplitDf = nSplitDf.drop(eCoordCols:_*)
    nSplitDf = nSplitDf.withColumn("dead", lit(false))
    nSplitDf.cache().show()

    var eCoordSplit = List.empty[DataFrame]
    for (col <- eCoordCols) {
      var t = origin.withColumn(col, split(functions.col(col), ","))
      t = (t select("roomid", "roleid", posexplode(functions.col(col)))).alias("idx", col)
      eCoordSplit :+= t
    }
    var eSplitDf = eCoordSplit.reduce(_.join(_, Seq("roomid", "roleid", "idx")))
    eSplitDf = eSplitDf.drop(nCoordCols:_*)
    eSplitDf = eSplitDf.withColumn("dead", lit(true))
    eSplitDf.cache().show()

    nSplitDf = nSplitDf.withColumnRenamed("idx", "time")
    nSplitDf = nSplitDf.withColumn("time", col("time") * 10 + OFFSET)
    nSplitDf = nSplitDf.withColumnRenamed("playerxaxisnew", "start_x")
    nSplitDf = nSplitDf.withColumnRenamed("playeryaxisnew", "start_y")

    eSplitDf = eSplitDf.withColumnRenamed("playerdeadtimenew", "time")
    eSplitDf = eSplitDf.drop("idx")
    eSplitDf = eSplitDf.withColumnRenamed("playerdeadxaxisnew", "start_x")
    eSplitDf = eSplitDf.withColumnRenamed("playerdeadyaxisnew", "start_y")

    var splitDf = nSplitDf.unionAll(eSplitDf)
    splitDf.cache().show()

    splitDf = splitDf.sort("roomid", "roleid", "time", "dead")

    var t = splitDf
    t = t.withColumnRenamed("start_x", "end_x")
    t = t.withColumnRenamed("start_y", "end_y")

    val cond = List($"roomid" === t("roomid"),
      $"roleid" === t("roleid"),
      $"dead" === lit(false),
      t("time") - $"time" <= lit(10),
      t("time") - $"time" > lit(0))

    splitDf = splitDf.join(t,column(cond.toString()))
    splitDf.cache().show()
  }
}
