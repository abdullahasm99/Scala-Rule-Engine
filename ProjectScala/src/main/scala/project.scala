// Importing necessary libraries for JDBC connection, date-time operations, file handling, logging, and mathematical operations
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.io.{BufferedSource, Source}
import java.util.logging._
import scala.math.ceil

// Defining the main object for the project
object project extends App {

  // Setting up logger
  val logger: Logger = Logger.getLogger("RulesEngine")
  val fileHandler: FileHandler = new FileHandler("rules_engine.log")
  val formatter: SimpleFormatter = new SimpleFormatter()
  fileHandler.setFormatter(formatter)
  logger.addHandler(fileHandler)

  // Function to log messages in the specified format
  def log(message: String, level: Level): Unit = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    logger.log(level, s"$timestamp $level $message")
  }

  // JDBC connection parameters
  val url = "jdbc:oracle:thin:@localhost:1521:XE"
  val user = "HR"
  val password = "root"

  // JDBC connection
  Class.forName("oracle.jdbc.driver.OracleDriver")
  val connection: Connection = DriverManager.getConnection(url, user, password)
  log("Database connection established.", Level.INFO)

  // Prepared statement for inserting data into the database table
  val insertStatement: PreparedStatement = connection.prepareStatement(
    "INSERT INTO processed_orders(timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, average_discount, total_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
  )

  // Read data from CSV file
  val source: BufferedSource = Source.fromFile("src/main/resources/TRX1000.csv")
  val lines: List[String] = source.getLines().drop(1).toList

  // Define a case class to represent an order
  case class Order(timestamp: String, product_name: String, expiry_date: String, quantity: Int, unit_price: Float, channel: String, payment_method: String)

  // Function to convert a line of CSV to an Order object
  def toOrder(line: String): Order = {
    val parts = line.split(",")
    Order(parts(0), parts(1), parts(2), parts(3).toInt, parts(4).toFloat, parts(5), parts(6))
  }

  // Function to format a timestamp string to a date string
  def getDate(ts: String): String = {
    val formatter = DateTimeFormatter.ISO_DATE_TIME
    val dateTime = LocalDateTime.parse(ts, formatter)
    val date = dateTime.toLocalDate()
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val formattedDate = date.format(dateFormatter)
    formattedDate
  }

  // Function to calculate the number of days between order date and expiry date
  def calcDaysBetween(date1: String, date2: String): Long = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val localDate1 = LocalDate.parse(date1, formatter)
    val localDate2 = LocalDate.parse(date2, formatter)
    val daysBetween = ChronoUnit.DAYS.between(localDate1, localDate2)
    daysBetween
  }

  // Function to calculate discount based on expiry date
  def toExpire(days: Long): Double = {
    if (days < 30) {
      val discount = (30 - days) * 0.01
      discount
    }
    else 0.0
  }

  // Function to check for a special date(23rd Mar) and provide discount if applicable
  def specialDate(date: String): Double = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val check = LocalDate.parse(date, formatter)
    if (check.getMonthValue == 3 && check.getDayOfMonth == 23) 0.5
    else 0.0
  }

  // Function to extract product name from a string
  def getProduct(product: String): String = {
    product.split("\\s+")(0)
  }

  // Function to provide discount based on product name
  def productIn(name: String): Double = {
    val product = name match {
      case "Wine" => 0.05
      case "Cheese" => 0.1
      case _ => 0.0
    }
    product
  }

  // Function to provide discount based on quantity ordered
  def quantityIn(quantity: Int): Double = {
    if (quantity >= 6 && quantity <= 9) 0.05
    else if (quantity >= 10 && quantity <= 14) 0.07
    else if (quantity >= 15) 0.1
    else 0.0
  }

  // Function to provide discount based on channel of purchase(App)
  def channelApp(name: String, quantity: Int): Double = {
    if (name == "App") {
      val roundedQuantity = ceil(quantity / 5.0) * 5
      val discountPercent = (roundedQuantity / 5) * 0.05
      discountPercent
    }
    else 0.0
  }

  // Function to provide discount based on payment method(Visa)
  def paymentMethod(method: String): Double = {
    if (method == "Visa") 0.05
    else 0.0
  }

  // Function to process an order, calculate discounts, and insert into the database
  def processedOrder(o: Order): Unit = {

    val date = getDate(o.timestamp)
    val days = calcDaysBetween(date, o.expiry_date)
    val discount1 = toExpire(days)
    val discount2 = specialDate(date)
    val productName = getProduct(o.product_name)
    val discount3 = productIn(productName)
    val discount4 = quantityIn(o.quantity)
    val discount5 = channelApp(o.channel,o.quantity)
    val discount6 = paymentMethod(o.payment_method)

    val discounts = Seq(discount1, discount2, discount3, discount4, discount5, discount6).filter(_ != 0.0).sorted.reverse.take(2)
    val nonZeroDiscountCount = discounts.length
    val averageDiscounts = if (nonZeroDiscountCount > 0) discounts.sum / nonZeroDiscountCount.toDouble else 0.0
    val totalAmount = (o.quantity * o.unit_price) - (o.quantity * o.unit_price * averageDiscounts)

    // Setting parameters for the prepared statement
    insertStatement.setString(1, o.timestamp)
    insertStatement.setString(2, o.product_name)
    insertStatement.setString(3, o.expiry_date)
    insertStatement.setInt(4, o.quantity)
    insertStatement.setFloat(5, o.unit_price)
    insertStatement.setString(6, o.channel)
    insertStatement.setString(7, o.payment_method)
    insertStatement.setDouble(8, averageDiscounts)
    insertStatement.setDouble(9, totalAmount)

    // Execute the insert statement for each order
    insertStatement.executeUpdate()
    log(s"Processed order: ${o.timestamp}, ${o.product_name}, ${o.quantity}, ${o.unit_price}, ${o.channel}, ${o.payment_method}", Level.INFO)
  }

  // Process each order and insert into the database
  lines.map(toOrder).foreach(processedOrder)

  // Close resources
  source.close()
  insertStatement.close()
  connection.close()
  log("Engine finished processing orders.", Level.INFO)
}