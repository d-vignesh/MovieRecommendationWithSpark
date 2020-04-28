import java.io.File

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.ml.recommendation.{ALS, ALSModel}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, mean, col, desc}
import org.apache.spark.sql.types.{IntegerType, FloatType, LongType}
import org.apache.spark.sql.Dataset

object MovieLensALS {

	case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
	

	def computeRmse(model: ALSModel, data: Dataset[Rating], n: Long): Double = {
		val predictions = model.transform(data)
							.withColumn("error", col("rating") - col("prediction"))
							.withColumn("squared_error", col("error") * col("error"))
		// println("predictions are : ")
		// println(predictions.getClass)
		// println(predictions.show())
		var se: Double = predictions.select(sum("squared_error").cast("Double")).first.getDouble(0)		
		return math.sqrt(se / n)
	}

	def main(args: Array[String]): Unit = {

		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.OFF)

		// create spark session
		val spark = SparkSession
			.builder
			.appName("MovieLensALS")
			.getOrCreate()
		import spark.implicits._

		val movieLensHomeDir = "/home/vignesh/my_drive/Data Science/spark/CollaborativeFiltering/ml-25m/"

		val personalRatingsDir = "/home/vignesh/my_drive/Data Science/spark/MovieRecommendationALS/src/main/python"
		
		// load movie ratings as Dataset
		val ratings = spark.read
			.format("csv")
			.option("header", "true")
			.load(new File(movieLensHomeDir, "ratings.csv").toString())
			.withColumn("userId",col("userId").cast(IntegerType))
			.withColumn("movieId", col("movieId").cast(IntegerType))
			.withColumn("rating", col("rating").cast(FloatType))
			.withColumn("timestamp", col("timestamp").cast(LongType))
			.as[Rating]

		// println("ratings df : ")
		// println(ratings_all.first.getClass)
		// println(ratings_all.getClass)
		// println(ratings_all.show())

		// load movie names as a Map (movieId -> movieName)
		val movies = spark.read
			.format("csv")
			.option("header", "true")
			.load(new File(movieLensHomeDir, "movies.csv").toString())
			.withColumn("movieId", col("movieId").cast(IntegerType))
			//.rdd
			//.map(rec => (rec(0) , (rec(1), rec(2))))
			//.collectAsMap()

		// println("movies map : ")
		// println(movies.getClass)
		// println(movies.get(1))
		// movies.take(20).foreach(println)
		
		val numRatings = ratings.count()
		val numUsers = ratings.select("userId").distinct().count()
		val numMovies = ratings.select("movieId").distinct().count()

		println("Got " + numRatings + " ratings from " +
				numUsers + " users on " + numMovies + " movies.")

		// split data into train, validation and test sets
		val Array(training, validation, test) = ratings.randomSplit(Array(0.6, 0.2, 0.2))
		val numTraining = training.count()
		val numValidation = validation.count()
		val numTest = test.count()

		println("Training : " + numTraining + " , validation : " + numValidation + " , test : " + numTest)
		
		// val als = new ALS()
		// 	.setMaxIter(5)
		// 	.setRegParam(0.01)
		// 	.setRank(12)
		// 	.setUserCol("userId")
		// 	.setItemCol("movieId")
		// 	.setRatingCol("rating")
		// val model = als.fit(training)
		// model.setColdStartStrategy("drop")
		// // print("model type is : " + model.getClass)
		// val validationRmse = computeRmse(model, validation, numValidation)
		// println(validationRmse)

		// build model with different hyper param and evaluate the best model.
		val ranks = List(8, 10, 12)
		val regParams = List(0.09, 0.3, 1)
		val numIters = List(7, 10, 15)

		var bestModel: Option[ALSModel] = None
		var bestValidationRmse: Double = Double.MaxValue
		var bestRank : Int = 0
		var bestRegParam : Double = -1.0
		var bestNumIter : Int = 0

		for( rank <- ranks ; regParam <- regParams; numIter <- numIters) {
			val als = new ALS()
				.setMaxIter(numIter)
				.setRegParam(regParam)
				.setRank(rank)
				.setUserCol("userId")
				.setItemCol("movieId")
				.setRatingCol("rating")
			val model = als.fit(training)
			model.setColdStartStrategy("drop")
			println("model type : " + model.getClass)

			val validationRmse = computeRmse(model, validation, numValidation)
			println("Validation Rmse = " + validationRmse + " for the model trained with rank : " + 
					rank + " regularization : " + regParam + " iteration : " + numIter)

			if (validationRmse < bestValidationRmse) {
				bestModel = Some(model)
				bestValidationRmse = validationRmse
				bestRank = rank
				bestNumIter = numIter
			}
		}

		// evaluate the best model on the test set
		val testRmse = computeRmse(bestModel.get, test, numTest)
		print("the best model was trained with rank = " + bestRank + " and regParam : " + bestRegParam +
			  " and iterations : " + bestNumIter )
		// create a naive baseline and compare it with the best model
		val meanRating = training.union(validation).select(mean("rating").first().get(0))
		test = test.withColumn("baselineRmse", (meanRating - col(rating)) * (meanRating) - col(rating))
		val baselineRmse = math.sqrt(test.select(mean("baselineRmse").first().get(0)))
		val improvement = (baselineRmse - testRmse) / baselineRmse * 100
		println("the best model improves the base line by " + "%1.2f".format(imporvement) + "%.")


		println("Factorized user matrix with rank = " + model.rank)
		println(model.userFactors.show(5))

		println("Factorized movie matrix with rank = " + model.rank)
		println(model.itemFactors.show(5))

		println("Recommended top users for all items with the corresponding predicted rating : ")
		println(model.recommendForAllItems(1).show(5))

		println("Recommended top items for all users with the corresponding predicted rating")
		println(model.recommendForAllUsers(1).show(5))
		
		// make Recommendations 
		// lets try generating recommendation to a specific user(user 3)
		println("historical rating data ..")
		println(training.filter(col("userId") === 3).show())

		val user_suggest = test.filter(col("userId") === 3).select("userId", "movieId")
		println("movies we are thinking of suggesting .. ")
		println(user_suggest.show())

		var user_offer = model.transform(user_suggest)
		user_offer = user_offer.join(movies, "movieId")
		println("predicted movies in decending order of rating .. ")
		println(user_offer.orderBy(desc("prediction")).show(20, false))
	}
}