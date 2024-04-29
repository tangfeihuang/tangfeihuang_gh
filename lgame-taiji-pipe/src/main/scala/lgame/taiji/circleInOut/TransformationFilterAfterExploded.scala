package lgame.taiji.circleInOut
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, column, explode, lit, split, udf}
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import xdata.mining.util.tdw.Utils.StringToColumn

import scala.collection.{Iterator, mutable}
import scala.math.sqrt
import scala.util.Try

import scala.math._

object TransformationFilterAfterExploded {

  def pairwise[T](iterable: Iterable[T]): Iterator[(T, T)] = {
    val a = iterable.iterator
    val b = iterable.iterator.drop(1)
    Iterator.continually(Try(a.next -> b.next)).takeWhile(_.isSuccess).map(_.get)
  }

  def main(args: Array[String]): Unit = {
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
    val OFFSET = 0 // 时间起始点，即第一个坐标的时间
    val INTERVAL = 10 // 时间间隔

    val CIRCLES: List[(String, Int, Int, Int)] = List(
      ("center", 8000, 0, 0),
      ("dragon", 5500, 16250, -20000),
      ("blue", 6000, -40000, -40000),
      ("red", 6000, 40000, 40000),
      ("corner", 7000, -40000, 40000)
    )


    // array string -> array of int
    val fraction = 0.05
    val splitDf = origin sample(false, fraction)

    // 定义列处理函数
    def splitAndCastColumn(columnName: String)(df: DataFrame): DataFrame = {
      df.withColumn(columnName, split($"${columnName}", ","))
        .withColumn(columnName, column(columnName).cast(ArrayType(IntegerType)))
    }

    // 应用列处理到指定列
    val allCoordCols = nCoordCols ++ eCoordCols
    val finalDF = allCoordCols.foldLeft(splitDf)((df, col) => splitAndCastColumn(col)(df))


    // pairwise data
    val coordSeqToSegsArrayUdf = udf((nXSeq: Seq[Int], nYSeq: Seq[Int], eXSeq: Seq[Int], eYSeq: Seq[Int], eTime: Seq[Int]) => {
      val segsArray = new mutable.ArrayBuffer[String]()

      // 正常坐标序列处理  nXSeq.zip(nYSeq).sliding(2).zipWithIndex
      // zip 对应py的zip，enumerate是给序列的元素加上索引
      pairwise(nXSeq.zip(nYSeq)).zipWithIndex.foreach { case (seqPair, i) =>
        val (start_x, start_y), (end_x, end_y) = seqPair
        segsArray += s"${OFFSET + INTERVAL * i},${OFFSET + INTERVAL * (i + 1)},$start_x,$start_y,$end_x,$end_y"
      }

      // 事件坐标替换
      eTime.zipWithIndex.foreach { case (eventTime, i) =>
        if (eventTime != null) {
          val index = math.floor((eventTime - OFFSET) / INTERVAL).toInt
          if (index < segsArray.length) {
            val segStr = segsArray(index).split(",").map(_.toInt)
            segStr(1) = eventTime.toInt
            segStr(4) = eXSeq(i)
            segStr(5) = eYSeq(i)
            segsArray.update(index, segStr.mkString(","))
          } else {
            segsArray += s"${OFFSET + INTERVAL * segsArray.length},${eventTime.toInt},${nXSeq.last},${nYSeq.last},${eXSeq(i)},${eYSeq(i)}"
          }
        }
      }
      segsArray.mkString(", ") // 将所有字符串连接，形成单一字符串返回
    })

    // explode pairewised data
    var pairwisedDf = splitDf.withColumn("pairwised", coordSeqToSegsArrayUdf(
      col("playerxaxisnew"), col("playeryaxisnew"),
      col("playerdeadxaxisnew"), col("playerdeadyaxisnew"),
      col("playerdeadtimenew")
    ))
      // 丢弃 DataFrame 中的列 n_coord_cols 和 e_coord_cols
      .drop(allCoordCols: _*)

    // 添加索引项
    val indexColLs = Seq("s_t", "e_t", "s_x", "s_y", "e_x", "e_y")
    pairwisedDf = pairwisedDf.withColumn("pairwised", explode(col("pairwised")))

    val addIndexCols = indexColLs.zipWithIndex.map{
      case(col,i) => col -> pairwisedDf("pairwised").getItem(i)
    }.toMap

    for ((colName, colValue) <- addIndexCols) {
      pairwisedDf = pairwisedDf.withColumn(colName, colValue)
    }

    // 删除pairwised列
    val pairwisedDfFinal = pairwisedDf.drop("pairwised")

    /**
     * filter data in circle
     */
    val filterCircles = udf((s_x: Int, s_y: Int, e_x: Int, e_y: Int,
                             c_names: Seq[String], c_rs: Seq[Int],
                             c_xs: Seq[Int], c_ys: Seq[Int]) => {
      if (s_x == null || s_y == null || e_x == null || e_y == null) {
        ""
      } else {
        val lineLength = sqrt(pow(e_x - s_x, 2) + pow(e_y - s_y, 2))

        val circleInfo = c_xs.zip(c_ys).zip(c_rs).zip(c_names).map{
          case (((a, b), c), d) => (a, b, c, d)
        }

        for ((c_x, c_y, c_r, name) <- circleInfo) {
          val startDistance = sqrt(pow(s_x - c_x, 2) + pow(s_y - c_y, 2))
          val endDistance = sqrt(pow(e_x - c_x, 2) + pow(e_y - c_y, 2))
          if (startDistance <= c_r || endDistance <= c_r) {
            return name
          }

          if (lineLength == 0) {
            // Avoid division by zero
            ()
          } else {
            val t = ((c_x - s_x) * (e_x - s_x) + (c_y - s_y) * (e_y - s_y)) / pow(lineLength, 2)
            if (t >= 0 && t <= 1) {
              val proj_x = s_x + t * (e_x - s_x)
              val proj_y = s_y + t * (e_y - s_y)
              val distance = sqrt(pow(proj_x - c_x, 2) + pow(proj_y - c_y, 2))
              if (distance <= c_r) {
                return name
              }
            }
          }
        }
        ""
      }
    })

    val names = List("name1", "name2", "name3")
    val c_rs = List(1, 2, 3)
    val c_xs = List(4, 5, 6)
    val c_ys = List(7, 8, 9)

    val filteredDf = pairwisedDf.withColumn("circle", filterCircles(
      $"s_x", $"s_y", $"e_x", $"e_y",
      lit(names), lit(c_rs), lit(c_xs), lit(c_ys)
    ))

    val filteredDfFinal = filteredDf.filter($"circle" =!= "")

    filteredDfFinal.write
      .option("sep", "\t")
      .option("header", "false")
      .option("quoteMode", "NONE")
      .csv("./filtered_sampled.tsv")

  }

}
