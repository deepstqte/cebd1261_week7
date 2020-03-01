package com.cellariot.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Find the superhero with the most co-appearances. */
object MostPopularSuperhero_assignment {
  
  // Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }
  
  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new  SparkConf().setMaster("local[*]").setAppName("MostPopularSuperhero").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named MostPopularSuperhero
    //alternative: val sc = new SparkContext("local[*]", "MostPopularSuperhero")
    val sc = new SparkContext(conf)   
    
    // Build up a hero ID -> name RDD
    val names = sc.textFile("../Marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)
    
    // Load up the superhero co-apperarance data
    val lines = sc.textFile("../Marvel-graph.txt")
    
    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurences)
    
    // Combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey( (x,y) => x + y )
    
    // Flip it to # of connections, hero ID
    val flipped = totalFriendsByCharacter.map( x => (x._2, x._1) )
    
    // Sort by # of connections, then take only the top 10
    val topTenPopularSuperheroes = flipped.sortByKey(false).collect().take(10)
    
    // Convert to (heroName) RDD
    val mostPopularNames = topTenPopularSuperheroes.map( x => namesRdd.lookup(x._2)(0))
    
    mostPopularNames.foreach(println)
  }
  
}
