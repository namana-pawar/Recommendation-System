import java.io.PrintWriter

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object NamanaMadanMohanRao_Pawar_ModelBasedCF  {

  case class MyRating(userId: String, productID: String, rating: Double)


  def main(args: Array[String]): Unit = {

    val t1 = System.nanoTime

    val sparkSession = org.apache.spark.sql.SparkSession.builder.master("local[*]").appName("SparkScala").getOrCreate
    val sc = sparkSession.sparkContext


    val trainCSV = sparkSession.read.option("header", value = true).option("inferSchema", value = true).csv(args(0)).rdd
    val testCSV = sparkSession.read.option("header", value = true).option("inferSchema", value = true).csv(args(1)).rdd

    val trainRDD = trainCSV.map(row =>{
      val user = HashString(row.get(0).toString)
      val item = HashString(row.get(1).toString)
      val rating = row.getAs[Int](2).toDouble
      Rating(user,item,rating)
    })

    val testRDD = testCSV.map(row =>{
      val user = HashString(row.get(0).toString)
      val item = HashString(row.get(1).toString)
      val rating = row.getAs[Int](2).toDouble
      Rating(user,item,rating)
    })

    val userToUserID = testCSV.map(row => {
      val user = row.get(0).toString
      val userID = HashString(row.get(0).toString)
      ( userID, user)
    })

    val map_user = userToUserID.collect().toMap.mapValues(_.toString).map(identity)

    val prodToProdID = testCSV.map(row => {
      val prod = row.get(1).toString
      val prodID = HashString(row.get(1).toString)
      ( prodID, prod)
    })

    val map_prod = prodToProdID.collect().toMap.mapValues(_.toString).map(identity)

    //to calculate average
    val userToItemAndRating_map = trainRDD.map({case Rating(user,item,rating)=>
      (user,(item,rating))
    }).groupByKey().map(row =>{
      val key = row._1.toInt
      val value = row._2.toArray
      (key,value)
    }).collect().toMap.mapValues(_.toArray).map(identity)

    val itemToUserAndRating_map = trainRDD.map({case Rating(user,item,rating)=>
      (item,(user,rating))
    }).groupByKey().map(row =>{
      val key = row._1.toInt
      val value = row._2.toArray
      (key,value)
    }).collect().toMap.mapValues(_.toArray).map(identity)


    // Build the recommendation model using ALS 10 15 0.3 2 20 0.28 2 15 0.28 2 20 0.25
    val rank = 2
    val numIterations = 20
    val model = ALS.train(trainRDD, rank, numIterations, 0.25)


    // Evaluate the model on rating data
    val usersProducts = testRDD.map { case Rating(user, product, rate) =>
      (user, product)
    }

    //determine missing records and giving it average rating
    var predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    var predictions_newuser =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        (user, product)
      }

    val test_newuser =
      testRDD.map { case Rating(user, product, rate) =>
        (user, product)
      }



    val new_users = test_newuser.subtract(predictions_newuser).map({case(user,item)=>
      var avg = 0.0
      if(userToItemAndRating_map.contains(user)){
        var sum = userToItemAndRating_map(user).map(row => row._2).sum
        avg = sum/userToItemAndRating_map(user).length

      }
      else if(itemToUserAndRating_map.contains(item)){
        var sum = itemToUserAndRating_map(item).map(row => row._2).sum
        avg = sum/itemToUserAndRating_map(item).length
      }
      else{
        avg = 2.5
      }
      ((user, item), avg)
    })

    predictions = predictions.union(new_users)


    val predictions_sorted = predictions.map ({ case ((user, product), rate) =>
        ((map_user(user), map_prod(product)), rate)
      }).sortByKey(numPartitions = 1)

    val predictions_mapped =   predictions_sorted.map(x => x._1._1 + "," + x._1._2+"," + x._2).collect.mkString("\n")

    val output_path = "NamanaMadanMohanRao_Pawar_ModelBasedCF.txt"
    new PrintWriter(output_path) {
      write(predictions_mapped); close()
    }

    val ratesAndPreds = testRDD.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)



    val category = ratesAndPreds.map ({ case ((user, product), (r1, r2)) =>
      val err = Math.abs(r1 - r2)
      val cat = if (err>=0 && err<1) 1
      else if (err>=1 && err<2) 2
      else if (err>=2 && err<3) 3
      else if (err>=3 && err<4) 4
      else if (err>4) 5
      (cat.toString, 1)
    }).reduceByKey(_+_)

    val map_cat = category.collect().toMap.mapValues(_.toString).map(identity)



    val RMSE = Math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = Math.abs(r1 - r2)
      err * err
    }.mean())

    println(">=0 and <1: "+printCategory(map_cat,"1"))
    println(">=1 and <2: "+printCategory(map_cat,"2"))
    println(">=2 and <3: "+printCategory(map_cat,"3"))
    println(">=3 and <4: "+printCategory(map_cat,"4"))
    println(">4: "+printCategory(map_cat,"5"))
    println(s"Root Mean Squared Error = $RMSE")
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"Time = "+duration)

  }

  def HashString(s: String): Int = {
    var hash = 7
    var i = 0
    for( i <- 0 until s.length){
      hash = hash * 31 + s.charAt(i)
    }
    hash
  }
  def printCategory(category : Map[String,String], cat : String ): String = {
    if (category.contains(cat)){
      category(cat)
    }
    else{
      "0"
    }
  }



}
