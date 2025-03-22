# CSV to MySQL Importer

A robust Python utility for automating the import of CSV files into MySQL database tables with intelligent data type detection, chunked processing for large files, and comprehensive error handling.

## 🌟 Features

- **Automatic Data Type Detection**: Intelligently determines appropriate MySQL data types based on CSV content
- **Date Format Handling**: Automatically detects and properly formats date columns
- **Chunked Processing**: Efficiently handles large files by processing data in manageable chunks
- **Progress Tracking**: Provides real-time feedback on import progress
- **Robust Error Handling**: Implements transaction management for data integrity
- **Detailed Logging**: Maintains comprehensive logs of all operations

## 📋 Requirements

- Python 3.6+
- pandas
- mysql-connector-python
- numpy

## 🔧 Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/csv-to-mysql-importer.git
   cd csv-to-mysql-importer
   ```

2. Install the required dependencies:
   ```
   pip install pandas mysql-connector-python numpy
   ```

3. Configure your MySQL connection details in the script.

## 🚀 Usage

1. Update the `FOLDER_PATH` variable in the script to point to your CSV files directory.

2. Update the `MYSQL_CONFIG` dictionary with your MySQL connection details:
   ```python
   MYSQL_CONFIG = {
       "host": "localhost",
       "user": "your_username",
       "password": "your_password",
       "database": "your_database",
       "connect_timeout": 180,
       "use_pure": True
   }
   ```

3. If you have specific date columns in your CSV files, add them to the `DATE_COLUMNS` dictionary with their formats:
   ```python
   DATE_COLUMNS = {
       'order_date': '%Y-%m-%d',
       'delivery_date': '%Y-%m-%d %H:%M:%S'
   }
   ```

4. Run the script:
   ```
   python csv_import.py
   ```

## ⚙️ How It Works

1. The script scans the specified folder for CSV files
2. For each CSV file:
   - Smart column naming: Converts spaces and hyphens to underscores
   - Data type inference: Detects appropriate MySQL data types
   - Date detection: Identifies and properly formats date columns
   - Table creation: Creates a table in MySQL with optimized column types
   - Chunked importing: Processes large files in manageable chunks
   - Progress tracking: Shows real-time import status

## 📊 Performance Optimization

- **Chunked Processing**: Large files are automatically processed in 5,000-row chunks
- **Transaction Management**: Each chunk is wrapped in a database transaction for data integrity
- **Memory Efficiency**: Replaces NaN values with None to reduce memory usage
- **Optimized Column Types**: VARCHAR lengths are dynamically determined based on data

## 📝 Logging

All operations are logged to `csv_import.log`, including:
- Script start/end times
- File processing details
- Successful imports
- Errors and exceptions

## 🛠️ Error Handling

- Graceful handling of keyboard interruptions
- Transaction rollback on errors to maintain database integrity
- Connection cleanup to prevent resource leaks

## 👨‍💻 Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/yourusername/csv-to-mysql-importer/issues).

## 📄 License

This project is [MIT](LICENSE) licensed.
