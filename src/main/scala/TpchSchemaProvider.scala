package main.scala

import org.apache.spark.sql.{Encoders, SparkSession}

// TPC-H table schemas
case class Customer(
                     c_custkey: Long,    // primary key
                     c_name: String,
                     c_address: String,
                     c_nationkey: Long,
                     c_phone: String,
                     c_acctbal: Double,
                     c_mktsegment: String,
                     c_comment: String)

case class Lineitem(
                     l_orderkey: Long,   // primary key
                     l_partkey: Long,
                     l_suppkey: Long,
                     l_linenumber: Long, // primary key
                     l_quantity: Double,
                     l_extendedprice: Double,
                     l_discount: Double,
                     l_tax: Double,
                     l_returnflag: String,
                     l_linestatus: String,
                     l_shipdate: String,
                     l_commitdate: String,
                     l_receiptdate: String,
                     l_shipinstruct: String,
                     l_shipmode: String,
                     l_comment: String)

case class Nation(
                   n_nationkey: Long,    // primary key
                   n_name: String,
                   n_regionkey: Long,
                   n_comment: String)

case class Order(
                  o_orderkey: Long,   // primary key
                  o_custkey: Long,
                  o_orderstatus: String,
                  o_totalprice: Double,
                  o_orderdate: String,
                  o_orderpriority: String,
                  o_clerk: String,
                  o_shippriority: Long,
                  o_comment: String)

case class Part(
                 p_partkey: Long,    // primary key
                 p_name: String,
                 p_mfgr: String,
                 p_brand: String,
                 p_type: String,
                 p_size: Long,
                 p_container: String,
                 p_retailprice: Double,
                 p_comment: String)

case class Partsupp(
                     ps_partkey: Long,   // primary key
                     ps_suppkey: Long,   // primary key
                     ps_availqty: Long,
                     ps_supplycost: Double,
                     ps_comment: String)

case class Region(
                   r_regionkey: Long,    // primary key
                   r_name: String,
                   r_comment: String)

case class Supplier(
                     s_suppkey: Long,    // primary key
                     s_name: String,
                     s_address: String,
                     s_nationkey: Long,
                     s_phone: String,
                     s_acctbal: Double,
                     s_comment: String)

class TpchSchemaProvider(spark: SparkSession, inputDir: String, fileFormat: String) {
  import spark.implicits._

  // for implicits
  var customer:org.apache.spark.sql.DataFrame = null;
  var lineitem:org.apache.spark.sql.DataFrame = null;
  var nation:org.apache.spark.sql.DataFrame = null;
  var region:org.apache.spark.sql.DataFrame = null;
  var order:org.apache.spark.sql.DataFrame = null;
  var part:org.apache.spark.sql.DataFrame = null;
  var partsupp:org.apache.spark.sql.DataFrame = null;
  var supplier:org.apache.spark.sql.DataFrame = null;

  // for csv
  val CustomerSchema = Encoders.product[Customer].schema
  val LineitemSchema = Encoders.product[Lineitem].schema
  val NationSchema = Encoders.product[Nation].schema
  val RegionSchema = Encoders.product[Region].schema
  val OrderSchema = Encoders.product[Order].schema
  val PartSchema = Encoders.product[Part].schema
  val PartsuppSchema = Encoders.product[Partsupp].schema
  val SupplierSchema = Encoders.product[Supplier].schema

  fileFormat match {
    case "tbl" => {
      val dfMap = Map(
        "customer" -> spark.read.textFile(inputDir + "/customer.tbl*").map(_.split('|')).map(p =>
          Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),

        "lineitem" -> spark.read.textFile(inputDir + "/lineitem.tbl*").map(_.split('|')).map(p =>
          Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),

        "nation" -> spark.read.textFile(inputDir + "/nation.tbl*").map(_.split('|')).map(p =>
          Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF(),

        "region" -> spark.read.textFile(inputDir + "/region.tbl*").map(_.split('|')).map(p =>
          Region(p(0).trim.toLong, p(1).trim, p(2).trim)).toDF(),

        "order" -> spark.read.textFile(inputDir + "/orders.tbl*").map(_.split('|')).map(p =>
          Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF(),

        "part" -> spark.read.textFile(inputDir + "/part.tbl*").map(_.split('|')).map(p =>
          Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toLong, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),

        "partsupp" -> spark.read.textFile(inputDir + "/partsupp.tbl*").map(_.split('|')).map(p =>
          Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toDouble, p(4).trim)).toDF(),

        "supplier" -> spark.read.textFile(inputDir + "/supplier.tbl*").map(_.split('|')).map(p =>
          Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()
      )

      // for implicits
      customer = dfMap.get("customer").get
      lineitem = dfMap.get("lineitem").get
      nation = dfMap.get("nation").get
      region = dfMap.get("region").get
      order = dfMap.get("order").get
      part = dfMap.get("part").get
      partsupp = dfMap.get("partsupp").get
      supplier = dfMap.get("supplier").get

      dfMap.foreach {
        case (key, value) => value.createOrReplaceTempView(key)
      }
    }
    case "csv" => {
      val dfMap = Map(
        "customer" ->
          spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(CustomerSchema).load(inputDir + "/customer.csv*")
        ,

        "lineitem" ->
          spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(LineitemSchema).load(inputDir + "/lineitem.csv*")
        ,

        "nation" ->
          spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(NationSchema).load(inputDir + "/nation.csv*")
        ,

        "region" ->
          spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(RegionSchema).load(inputDir + "/region.csv*")
        ,

        "order" ->
          spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(OrderSchema).load(inputDir + "/orders.csv*")
        ,

        "part" ->
          spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(PartSchema).load(inputDir + "/part.csv*")
        ,

        "partsupp" ->
          spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(PartsuppSchema).load(inputDir + "/partsupp.csv*")
        ,

        "supplier" ->
          spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(SupplierSchema).load(inputDir + "/supplier.csv*")
      )

      // for implicits
      customer = dfMap.get("customer").get
      lineitem = dfMap.get("lineitem").get
      nation = dfMap.get("nation").get
      region = dfMap.get("region").get
      order = dfMap.get("order").get
      part = dfMap.get("part").get
      partsupp = dfMap.get("partsupp").get
      supplier = dfMap.get("supplier").get

      dfMap.foreach {
        case (key, value) => value.createOrReplaceTempView(key)
      }
    }
    case "parquet" => {
      val dfMap = Map(
        "customer" ->
          spark.read.parquet(inputDir + "/customer.parquet*")
      ,

        "lineitem" ->
          spark.read.parquet(inputDir + "/lineitem.parquet*")
      ,

        "nation" ->
          spark.read.parquet(inputDir + "/nation.parquet*")
        ,

        "region" ->
          spark.read.parquet(inputDir + "/region.parquet*")
        ,

        "order" ->
          spark.read.parquet(inputDir + "/orders.parquet*")
        ,

        "part" ->
          spark.read.parquet(inputDir + "/part.parquet*")
        ,

        "partsupp" ->
          spark.read.parquet(inputDir + "/partsupp.parquet*")
        ,

        "supplier" ->
          spark.read.parquet(inputDir + "/supplier.parquet*")
      )

      // for implicits
      customer = dfMap.get("customer").get
      lineitem = dfMap.get("lineitem").get
      nation = dfMap.get("nation").get
      region = dfMap.get("region").get
      order = dfMap.get("order").get
      part = dfMap.get("part").get
      partsupp = dfMap.get("partsupp").get
      supplier = dfMap.get("supplier").get

      dfMap.foreach {
        case (key, value) => value.createOrReplaceTempView(key)
      }
    }
    case "avro" => {
      val dfMap = Map(
        "customer" ->
          spark.read.format("avro").load(inputDir + "/customer.avro*")
        ,

        "lineitem" ->
          spark.read.format("avro").load(inputDir + "/lineitem.avro*")
        ,

        "nation" ->
          spark.read.format("avro").load(inputDir + "/nation.avro*")
        ,

        "region" ->
          spark.read.format("avro").load(inputDir + "/region.avro*")
        ,

        "order" ->
          spark.read.format("avro").load(inputDir + "/orders.avro*")
        ,

        "part" ->
          spark.read.format("avro").load(inputDir + "/part.avro*")
        ,

        "partsupp" ->
          spark.read.format("avro").load(inputDir + "/partsupp.avro*")
        ,

        "supplier" ->
          spark.read.format("avro").load(inputDir + "/supplier.avro*")
      )

      // for implicits
      customer = dfMap.get("customer").get
      lineitem = dfMap.get("lineitem").get
      nation = dfMap.get("nation").get
      region = dfMap.get("region").get
      order = dfMap.get("order").get
      part = dfMap.get("part").get
      partsupp = dfMap.get("partsupp").get
      supplier = dfMap.get("supplier").get

      dfMap.foreach {
        case (key, value) => value.createOrReplaceTempView(key)
      }
    }
  }
}
