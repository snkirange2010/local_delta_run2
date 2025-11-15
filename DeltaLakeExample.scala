import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import io.delta.tables._

/**
 * Comprehensive Delta Lake Operations Example
 * Demonstrates: Create, Read, Write, Update, Delete, Merge, Time Travel
 */
object DeltaLakeExample {
  
  // Delta table paths on local Windows filesystem
  val DELTA_TABLE_PATH = "file:///C:/delta-tables/cat_aperture_trades"
  val DELTA_SNAPSHOT_PATH = "file:///C:/delta-tables/cat_aperture_snapshots"
  
  def main(args: Array[String]): Unit = {
    
    // Create Spark Session with Delta Lake support
    val spark = SparkSessionBuilder.createSparkSession()
    
    try {
      // Execute Delta Lake operations
      println("\n" + "=" * 80)
      println("STARTING CAT-APERTURE DELTA LAKE OPERATIONS")
      println("=" * 80)
      
      // 1. Create and Write Delta Table
      createInitialDeltaTable(spark)
      
      // 2. Read Delta Table
      readDeltaTable(spark)
      
      // 3. Append Data to Delta Table
      appendDataToDeltaTable(spark)
      
      // 4. Update Delta Table
      updateDeltaTable(spark)
      
      // 5. Delete from Delta Table
      deleteFromDeltaTable(spark)
      
      // 6. Upsert (Merge) into Delta Table
      upsertIntoDeltaTable(spark)
      
      // 7. Time Travel - Read Historical Versions
      demonstrateTimeTravel(spark)
      
      // 8. Vacuum old files (cleanup)
      vacuumDeltaTable(spark)
      
      // 9. Describe History
      showDeltaHistory(spark)
      
      println("\n" + "=" * 80)
      println("✓ ALL DELTA LAKE OPERATIONS COMPLETED SUCCESSFULLY")
      println("=" * 80)
      
    } catch {
      case e: Exception =>
        println(s"\n✗ ERROR: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Stop Spark Session
      SparkSessionBuilder.stopSparkSession(spark)
    }
  }
  
  /**
   * 1. CREATE: Create initial Delta table with sample CAT trade data
   */
  def createInitialDeltaTable(spark: SparkSession): Unit = {
    import spark.implicits._
    
    println("\n[1] Creating Initial Delta Table...")
    
    val tradesDF = Seq(
      ("TRD001", "2024-11-01", "AAPL", "BUY", 150.25, 100, "NASDAQ", "ACTIVE"),
      ("TRD002", "2024-11-01", "GOOGL", "SELL", 2800.50, 50, "NASDAQ", "ACTIVE"),
      ("TRD003", "2024-11-02", "MSFT", "BUY", 380.75, 200, "NASDAQ", "ACTIVE"),
      ("TRD004", "2024-11-02", "TSLA", "BUY", 245.30, 75, "NASDAQ", "ACTIVE"),
      ("TRD005", "2024-11-03", "AMZN", "SELL", 178.90, 150, "NASDAQ", "ACTIVE")
    ).toDF("trade_id", "trade_date", "symbol", "action", "price", "quantity", "exchange", "status")
    
    // Write as Delta table with partitioning
    tradesDF.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .partitionBy("trade_date")
      .save(DELTA_TABLE_PATH)
    
    println(s"✓ Created Delta table at: $DELTA_TABLE_PATH")
    println(s"✓ Initial record count: ${tradesDF.count()}")
    tradesDF.show(truncate = false)
  }
  
  /**
   * 2. READ: Read data from Delta table
   */
  def readDeltaTable(spark: SparkSession): Unit = {
    println("\n[2] Reading Delta Table...")
    
    val deltaDF = spark.read
      .format("delta")
      .load(DELTA_TABLE_PATH)
    
    println(s"✓ Current record count: ${deltaDF.count()}")
    deltaDF.show(truncate = false)
    
    // Show schema
    println("✓ Delta Table Schema:")
    deltaDF.printSchema()
  }
  
  /**
   * 3. APPEND: Append new records to Delta table
   */
  def appendDataToDeltaTable(spark: SparkSession): Unit = {
    import spark.implicits._
    
    println("\n[3] Appending Data to Delta Table...")
    
    val newTradesDF = Seq(
      ("TRD006", "2024-11-04", "NVDA", "BUY", 495.20, 120, "NASDAQ", "ACTIVE"),
      ("TRD007", "2024-11-04", "META", "SELL", 520.15, 80, "NASDAQ", "ACTIVE")
    ).toDF("trade_id", "trade_date", "symbol", "action", "price", "quantity", "exchange", "status")
    
    newTradesDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(DELTA_TABLE_PATH)
    
    println(s"✓ Appended ${newTradesDF.count()} new records")
    
    // Verify
    val updatedCount = spark.read.format("delta").load(DELTA_TABLE_PATH).count()
    println(s"✓ Total records after append: $updatedCount")
  }
  
  /**
   * 4. UPDATE: Update existing records in Delta table
   */
  def updateDeltaTable(spark: SparkSession): Unit = {
    println("\n[4] Updating Delta Table...")
    
    val deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
    
    // Update status to COMPLETED for specific trade
    deltaTable.update(
      condition = expr("trade_id = 'TRD001'"),
      set = Map("status" -> lit("COMPLETED"))
    )
    
    println("✓ Updated status for TRD001 to COMPLETED")
    
    // Show updated record
    spark.read.format("delta").load(DELTA_TABLE_PATH)
      .filter("trade_id = 'TRD001'")
      .show(truncate = false)
  }
  
  /**
   * 5. DELETE: Delete records from Delta table
   */
  def deleteFromDeltaTable(spark: SparkSession): Unit = {
    println("\n[5] Deleting from Delta Table...")
    
    val deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
    
    // Delete trades with quantity less than 100
    deltaTable.delete(condition = expr("quantity < 100"))
    
    println("✓ Deleted trades with quantity < 100")
    
    val remainingCount = spark.read.format("delta").load(DELTA_TABLE_PATH).count()
    println(s"✓ Remaining records: $remainingCount")
  }
  
  /**
   * 6. UPSERT (MERGE): Merge data into Delta table
   */
  def upsertIntoDeltaTable(spark: SparkSession): Unit = {
    import spark.implicits._
    
    println("\n[6] Performing UPSERT (Merge) into Delta Table...")
    
    val updatesDF = Seq(
      ("TRD002", "2024-11-01", "GOOGL", "SELL", 2805.00, 50, "NASDAQ", "COMPLETED"), // Update existing
      ("TRD008", "2024-11-05", "NFLX", "BUY", 680.45, 90, "NASDAQ", "ACTIVE")        // Insert new
    ).toDF("trade_id", "trade_date", "symbol", "action", "price", "quantity", "exchange", "status")
    
    val deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
    
    deltaTable.alias("target")
      .merge(
        updatesDF.alias("source"),
        "target.trade_id = source.trade_id"
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
    
    println("✓ Merge operation completed (1 update, 1 insert)")
    
    // Show merged results
    spark.read.format("delta").load(DELTA_TABLE_PATH)
      .filter("trade_id IN ('TRD002', 'TRD008')")
      .show(truncate = false)
  }
  
  /**
   * 7. TIME TRAVEL: Read historical versions of Delta table
   */
  def demonstrateTimeTravel(spark: SparkSession): Unit = {
    println("\n[7] Demonstrating Time Travel...")
    
    // Read version 0 (initial version)
    val version0DF = spark.read
      .format("delta")
      .option("versionAsOf", 0)
      .load(DELTA_TABLE_PATH)
    
    println(s"✓ Version 0 record count: ${version0DF.count()}")
    
    // Read latest version
    val latestDF = spark.read
      .format("delta")
      .load(DELTA_TABLE_PATH)
    
    println(s"✓ Latest version record count: ${latestDF.count()}")
    
    // You can also read by timestamp
    // val timestampDF = spark.read
    //   .format("delta")
    //   .option("timestampAsOf", "2024-11-01")
    //   .load(DELTA_TABLE_PATH)
  }
  
  /**
   * 8. VACUUM: Clean up old files (older than retention period)
   */
  def vacuumDeltaTable(spark: SparkSession): Unit = {
    println("\n[8] Vacuuming Delta Table...")
    
    val deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
    
    // Vacuum files older than 0 hours (for demo - normally use 168 hours = 7 days)
    // WARNING: This will prevent time travel to older versions
    deltaTable.vacuum(0.0)
    
    println("✓ Vacuum completed - old files removed")
  }
  
  /**
   * 9. HISTORY: Show Delta table transaction log
   */
  def showDeltaHistory(spark: SparkSession): Unit = {
    println("\n[9] Showing Delta Table History...")
    
    val deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
    
    val history = deltaTable.history()
    
    println("✓ Delta Table Transaction History:")
    history.select("version", "timestamp", "operation", "operationMetrics")
      .show(truncate = false)
  }

}
