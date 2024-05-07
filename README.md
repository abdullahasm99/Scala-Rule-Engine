# Rules Engine for Transaction Processing

This project implements a rules engine in Scala for processing transaction data and applying discounts based on predefined rules. The rules engine reads transaction data from a CSV file, applies various discount rules based on product expiry, product type, quantity purchased, and special dates, and then calculates the final price for each transaction. The processed data is inserted into a database table for further analysis.

## Features:
- **Flexible Rule Implementation**: The rules engine supports configurable discount rules for products nearing expiry, specific product types, quantity purchased, and special dates.
- **Database Integration**: Processed transaction data is stored in a relational database for easy querying and analysis.
- ![image](https://github.com/abdullahasm99/Scala-Rule-Engine/assets/153215733/17802a18-f1d5-474f-8934-5fb731a6ba77)
- **Logging**: Detailed logging is implemented to track the processing of transactions, database connections, and any errors encountered.
- ![image](https://github.com/abdullahasm99/Scala-Rule-Engine/assets/153215733/84b768ab-d766-4449-8eb7-6d8351592433)
- **Scalable and Maintainable**: The project is implemented in Scala, offering scalability and maintainability for handling large volumes of transaction data.

## Usage:
1. **Configure Database Connection**: Update the JDBC connection parameters in the code to connect to your database.
2. **Prepare Transaction Data**: Ensure that your transaction data is in CSV format and located in the specified directory.
3. **Define Discount Rules**: Modify the discount rules according to your business requirements.
4. **Run the Application**: Execute the main Scala file to start the rules engine, which will process the transaction data and insert the results into the database.

## Dependencies:
- Java JDK
- Scala

## License:
This project is licensed under the [MIT License](LICENSE).
