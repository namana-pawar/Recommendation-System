import java.io.PrintWriter


object NamanaMadanMohanRao_Pawar_ItemBasedCF {
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime

    val sparkSession = org.apache.spark.sql.SparkSession.builder.master("local[*]").appName("SparkScala").getOrCreate
    val sc = sparkSession.sparkContext


    val trainCSV = sparkSession.read.option("header", value = true).option("inferSchema", value = true).csv(args(0)).rdd
    val testCSV = sparkSession.read.option("header", value = true).option("inferSchema", value = true).csv(args(1)).rdd

    var trainRDD = trainCSV.map(row => {
      val user = row.get(0).toString
      val item = row.get(1).toString
      val rating = row.getAs[Int](2).toDouble
      ((user, item), rating)
    })

    var testRDD = testCSV.map(row => {
      val user = row.get(0).toString
      val item = row.get(1).toString
      val rating = row.getAs[Int](2).toDouble
      ((user, item), rating)
    })


    val trainRdd_map = trainRDD.collect().toMap.mapValues(_.toDouble).map(identity)

    val userToItemAndRating_map = trainRDD.map(row => (row._1._1,(row._1._2,row._2))).groupByKey().map(row =>{
      val key = row._1.toString
      val value = row._2.toArray
      (key,value)
    }).collect().toMap.mapValues(_.toArray).map(identity)



    val itemToUserAndRating_map = trainRDD.map(row => (row._1._2,(row._1._1,row._2))).groupByKey().map(row =>{
      val key = row._1.toString
      val value = row._2.toArray
      (key,value)
    }).collect().toMap.mapValues(_.toArray).map(identity)



    val prediction = testRDD.map({case((user,item),rating) =>
      if(userToItemAndRating_map.contains(user) && itemToUserAndRating_map.contains(item)) {
        if(trainRdd_map.contains((user,item))){
          ((user, item), trainRdd_map((user,item)))
        }

        val users_corated_items = userToItemAndRating_map(user).map(row => row._1)
        val ratings_item1 = itemToUserAndRating_map(item).toMap

        //Calculating weights using Pearsons Corelation
        val pearsonsCorrelation = users_corated_items.map({ case (item2) =>

          //Average co ratings
          val ratings_item2 = itemToUserAndRating_map(item2).toMap
          val common_users = ratings_item1.keySet.intersect(ratings_item2.keySet)

          if(common_users.isEmpty) {
            var avg_rating1 = 0.0
            var avg_rating2 = 0.0
            for (u <- common_users) {
              avg_rating1 += ratings_item1(u)
              avg_rating2 += ratings_item2(u)
            }
            avg_rating1 = avg_rating1 / common_users.size
            avg_rating2 = avg_rating2 / common_users.size

            var numerator = 0.0
            for (u <- common_users) {
              numerator += (trainRdd_map((u, item)) - avg_rating1) * (trainRdd_map((u, item2)) - avg_rating2)

            }

            var denominator = 0.0
            var d1 = 0.0
            var d2 = 0.0
            for (u <- common_users) {
              d1 += Math.pow(trainRdd_map((u, item)) - avg_rating1, 2)
              d2 += Math.pow(trainRdd_map((u, item2)) - avg_rating2, 2)

            }

            denominator = Math.sqrt(d1) * Math.sqrt(d2)
            var s = numerator / denominator

            if (denominator == 0) {
              //NaN values from weights
              //Very important drastically affetcs accuracy
              var avg = 0.0
              if (userToItemAndRating_map.contains(user)) {
                var sum = userToItemAndRating_map(user).map(row => row._2).sum
                avg = sum / userToItemAndRating_map(user).length
              }
              else if (itemToUserAndRating_map.contains(item2)) {
                var sum = itemToUserAndRating_map(item2).map(row => row._2).sum
                avg = sum / itemToUserAndRating_map(item2).length
              }
              else {
                avg = 2.5
              }
              s = avg

            }


            (item2, Math.round(s))
          }
            else{
            //No common users
            (item2, Math.round(1.0))

          }

        }).map(row => (row._1, row._2))

        //Calculating Predictions
        var num = 0.0
        var den = 0.0
        var predict = 0.0
          pearsonsCorrelation.foreach({
            case (item2, rate) =>
              num += trainRdd_map(user, item2) * rate
              den += Math.abs(rate)
          })
          if(den==0){
            //NaN values from prediction
            var maximum = Double.NegativeInfinity
            var mostSimilarItem = ""
            pearsonsCorrelation.foreach({
              case (item2, rate) =>
                if(rate>maximum){
                  maximum = rate
                  mostSimilarItem = item2
                }
            })
            predict = trainRdd_map(user, mostSimilarItem)
          }
          else {
            predict = num / den
          }
        if(predict<0){
          predict = -1*predict
        }
        if (predict>5){
          predict=5

        }

        ((user, item), predict)
      }
        else{
        //cold start when item has no rating or user not found
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
      }

    }).sortByKey(numPartitions = 1)


    val predictions_mapped =
      prediction.map ({ case ((user, product), rate) =>
        (user, product, rate)
      }).map(x => x._1 + "," + x._2+"," + x._3).collect.mkString("\n")

    val output_path = "NamanaMadanMohanRao_Pawar_ItemBasedCF.txt"
    new PrintWriter(output_path) {
      write(predictions_mapped); close()
    }

    val ratesAndPreds = testRDD.map { case ((user, product), rate) =>
      ((user, product), rate)
    }.join(prediction)


    val category = ratesAndPreds.map ({ case ((user, product), (r1, r2)) =>

      val err = Math.abs(r1-r2)
      val cat = if (err>=0 && err<1) 1
      else if (err>=1 && err<2) 2
      else if (err>=2 && err<3) 3
      else if (err>=3 && err<4) 4
      else if (err>4) 5
      (cat.toString, 1.0)
    }).reduceByKey(_+_)

    val map_cat = category.collect().toMap.mapValues(_.toString).map(identity)


    val RMSE = Math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>

      val err = Math.abs(r1-r2)
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

  def printCategory(category : Map[String,String], cat : String ): String = {
    if (category.contains(cat)){
      category(cat)
    }
    else{
      "0"
    }
  }
}
