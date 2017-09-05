import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.ml.regression.LinearRegression

Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("Hotel_Reviews_clean.csv")

val sentimentInfo = data.select(data("sentiment").as("label"), $"review_text")

val positiveReviews = sentimentInfo.filter("label = 1")
val negativeReviews = sentimentInfo.filter("label = 0")
val countNegative = negativeReviews.count().toInt

val balancedData = positiveReviews.limit(countNegative).unionAll(negativeReviews)

val tokenizer = new Tokenizer().setInputCol("review_text").setOutputCol("words")

val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(30000)

val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

val lr = new LinearRegression().setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8)

val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, lr))

val lrModel = pipeline.fit(balancedData)

val trainingSummary = lrModel.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"r2: ${trainingSummary.r2}")
