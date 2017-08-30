import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("Hotel_Reviews_clean.csv")

val sentimentInfo = data.select(data("sentiment").as("label"), $"review_text")

val positiveReviews = sentimentInfo.filter("label = 1")
val negativeReviews = sentimentInfo.filter("label = 0")
val countNegative = negativeReviews.count().toInt

val balancedData = positiveReviews.limit(countNegative).unionAll(negativeReviews)

val Array(training, test) = balancedData.randomSplit(Array(0.7, 0.3), seed = 12345)

val tokenizer = new Tokenizer().setInputCol("review_text").setOutputCol("words")

val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(80000)

val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

val layers = Array[Int](80000, 50000, 50000, 2)

val neuralNetwork = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(200)

  val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, naiveBayes))

  val model = pipeline.fit(training)

  val results = model.transform(test)

  val predictionAndLabels = results.select($"prediction",$"label").as[(Double, Double)].rdd

  // Instantiate metrics object
  val metrics = new MulticlassMetrics(predictionAndLabels)

  // Confusion matrix
  println("Confusion matrix:")
  println(metrics.confusionMatrix)

  println("Accuracy:")
  println(metrics.accuracy)
