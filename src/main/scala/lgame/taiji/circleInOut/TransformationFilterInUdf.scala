package lgame.taiji.circleInOut

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.math.sqrt


object TransformationFilterInUdf {
  def pairwise[T](iterable: Iterable[T]): Iterator[(T, T)] = {
    // pairwise('ABCDEFG') --> AB BC CD DE EF FG
    iterable.iterator.sliding(2).map { case Seq(a, b) => (a, b) }
  }

  def main(args: Array[String]): Unit = {
    // 设置环境变量
    //  sys.env("GROUP_ID") = "g_ieg_wepop_xmomi"
    //  sys.env("GAIA_ID") = "8628" // 3751 8628 从左侧文件树根目录pools.html中选取

    // 创建SparkSession
    val spark = SparkSession.builder
      .config("spark.driver.memory", "2g")
      .config("spark.executor.num", 20)
      .config("spark.executor.cores", 1)
      .config("spark.executor.memory", "2g")
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

    // Spark的DataFrameReader从数据库直接读取数据，需要替换jdbcUrl, user, password, dbTable为实际值
    val jdbcUrl = "jdbc:tdw://ieg_lgame_skynet_application"
    val user = "config.USER"
    val password = "config.PASSWD"
    val dbTable = "lgame_actor_detection_combine_unique_hour"

    // 读取数据，p_2023112114是作为查询条件或过滤条件?
    val origin = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"(SELECT $requiredCols FROM $dbTable WHERE p_2023112114) AS subquery")
      .option("user", user)
      .option("password", password)
      .load()

    // 缓存DataFrame，Scala中使用persist代替cache
    origin.persist()

    /**
     * 转换数据
     */
    val OFFSET = 0L
    val INTERVAL = 10L
    val CENTRE_X_LS = List(8000, 12000)
    val CENTRE_Y_LS = List(1400, 18000)
    val RADIUS_LS = List(500, 300)

    // ++ 链接两个集合
    val allCoordCols = nCoordCols ++ eCoordCols
    // array string -> array of int
    var split_df = origin
    allCoordCols.foreach {
      colName =>
        // 首先使用split函数分割字符串，然后将结果转换为整型数组
        val splitCol = split_df.col(colName).cast("string").alias(s"${colName}_str")
        val splitAndCastCol = split(splitCol, ",\\s*").cast("array<int>")

        // 更新DataFrame，先分割再转换类型
        split_df = split_df.withColumn(colName, splitAndCastCol)
    }

    // pairwise data
    def isInCircle(x: Int, y: Int, xc: Int, yc: Int, r: Int): Boolean = {
      sqrt(math.pow(x - xc, 2.0) + math.pow(y - yc, 2.0)) <= r.toDouble
    }

    def isInCircles(x0: Int, y0: Int, x1: Int, y1: Int): Boolean = {
      (RADIUS_LS, CENTRE_X_LS, CENTRE_Y_LS).zipped.foreach { case (r, x, y) =>
        println(s"x0, y0, x1, y1 = $x0, $y0, $x1, $y1")
        println(s"r, x, y = $r, $x, $y")
        if (isInCircle(x0, y0, x, y, r)) return true
        if (isInCircle(x1, y1, x, y, r)) return true
      }
      false
    }

    val coordSeqToSegsArrayUdf = udf((nXSeq: List[Int], nYSeq: List[Int],
                                      eXSeq: List[Int], eYSeq: List[Int],
                                      eTimeSeq: List[Int]) => {
      val segsArray = new scala.collection.mutable.ListBuffer[Array[Int]]()

      //!! Pairwise normal coords
      nXSeq.zip(nYSeq).sliding(2).foreach { case Seq((start_x, end_x), (start_y, end_y)) =>
        segsArray += Array(OFFSET.toInt + INTERVAL.toInt * segsArray.size,
          OFFSET.toInt + INTERVAL.toInt * (segsArray.size + 1),
          start_x, end_x, start_y, end_y)
      }

      // Replace events coords
      eTimeSeq.zipWithIndex.foreach { case (eventTime, i) =>
        if (eventTime != null && eXSeq(i) != null && eYSeq(i) != null) {
          val index = math.floor((eventTime - OFFSET) / INTERVAL).toInt
          if (index >= segsArray.length) {
            segsArray += Array(OFFSET.toInt + INTERVAL.toInt * segsArray.length,
              eventTime.toInt,
              nXSeq.last, nYSeq.last, eXSeq(i), eYSeq(i))
          } else {
            val seg = segsArray(index)
            seg(1) = eventTime.toInt
            seg(3) = eXSeq(i)
            seg(5) = eYSeq(i)
          }
        }
      }

      // Filter if coords is in circles
      segsArray.filter { case Array(_, _, sx, ex, sy, ey) =>
        isInCircles(sx, sy, ex, ey)
      }.toArray
    }, ArrayType(ArrayType(IntegerType)))

    // Assuming `split_df` is already defined and transformed to have columns split
    val pairwisedDf = split_df.withColumn("pairwised",
      coordSeqToSegsArrayUdf(
        col("playerxaxisnew"), col("playeryaxisnew"),
        col("playerdeadxaxisnew"), col("playerdeadyaxisnew"),
        col("playerdeadtimenew")
      )
    )

    // Explode the "pairwised" array and create separate columns
    val explodedDf = pairwisedDf.selectExpr(
      "stat_yymmddhh", "roomid", "roleid", "heroid", "gametime",
      "posexplode_outer(pairwised) as (seg_index, seg)"
    ).selectExpr(
      "*",
      "seg(0) as s_time", "seg(1) as e_time","seg(2) as s_x",
      "seg(3) as e_x", "seg(4) as s_y", "seg(5) as e_y"
    ).drop("pairwised")

    // Save DataFrame to TSV
    explodedDf.write
      .option("sep", "\t")
      .option("index", "false")
      .option("quoteAll", "false")
      .csv("./filtered_df_3.tsv")
  }

}
