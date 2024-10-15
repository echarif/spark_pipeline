# Spark pipeline


This project aims to build an end-to-end ETL (Extract, Transform, Load) pipeline for analyzing customer transactions. The goal is to identify customers who bought AirPods after purchasing an iPhone and customers who bought only these two products, iPhone and AirPods, without any other products. The pipeline reads data from various data sources, applies business logic to filter the relevant data, and loads the processed data into different storage locations.

### Data Sources

The project involves working with three types of data sources:

1. **CSV Files**: These files are used to load transactional data that contains customer purchases.
2. **Delta Tables**: This data source is used to load customer information stored as Delta tables, a format that supports ACID transactions and time travel for big data processing.
3. **Parquet Files**: In future extensions, Parquet files can serve as an efficient format for columnar storage.

### Workflow Overview

The workflow follows the ETL pattern:
- **Extract**: Relevant data is extracted from different data sources.
- **Transform**: Business logic is applied to filter and transform the data.
- **Load**: The transformed data is saved into the required storage formats, including file-based formats and Delta tables.

### Technologies Used

1. **Apache Spark**: Distributed data processing framework used for batch and streaming data operations, enabling fast and scalable analysis of large datasets.
2. **Delta Lake**: An open-source storage layer that brings ACID transactions to Apache Spark, supporting both batch and streaming data processing.
3. **Databricks Community Edition**: Development environment for building and testing the ETL pipeline, where Spark jobs and notebooks are run.
4. **Factory Design Pattern**: This design pattern is used to create different data source and sink objects based on the input type (e.g., CSV, Parquet, or Delta), allowing for easy scalability and maintenance of the code.

---

### Code Breakdown

#### Reader Factory (`reader_factory` notebook)

This notebook contains code that defines how data from different sources (CSV, Parquet, Delta) will be read. The **Factory Design Pattern** is used here to abstract the data loading logic for each source type.

```python
class DataSource:
    """
    Abstract class to represent a data source.
    """
    def __init__(self, path):
        self.path = path

    def get_data_frame(self):
        """
        Abstract method to be implemented in subclasses.
        """
        raise ValueError("Not Implemented")
```
- **`DataSource` Class**: An abstract class that serves as a base for all data source types. It requires a `path` parameter to be provided for loading data and defines an abstract method `get_data_frame` to be implemented by subclasses.

```python
class CSVDataSource(DataSource):
    def get_data_frame(self):
        return (
            spark.read.format("csv").option("header", True).load(self.path)
        )
```
- **`CSVDataSource` Class**: Inherits from `DataSource`. Implements `get_data_frame` to read CSV files using Spark’s read method with headers.

```python
class ParquetDataSource(DataSource):
    def get_data_frame(self):
        return (
            spark.read.format("parquet").load(self.path)
        )
```
- **`ParquetDataSource` Class**: Similar to the `CSVDataSource`, this class implements `get_data_frame` for reading Parquet files.

```python
class DeltaDataSource(DataSource):
    def get_data_frame(self):
        table_name = self.path
        return (
            spark.read.table(table_name)
        )
```
- **`DeltaDataSource` Class**: This class reads data from a Delta table using its name provided as the path.

```python
def get_data_source(data_type, file_path):
    if data_type == "csv":
        return CSVDataSource(file_path)
    elif data_type == "parquet":
        return ParquetDataSource(file_path)
    elif data_type == "delta":
        return DeltaDataSource(file_path)
    else:
        raise ValueError(f"Not implemented for data_type: {data_type}")       
```
- **`get_data_source` Function**: This factory function takes a `data_type` and a `file_path` and returns an instance of the appropriate data source class based on the input type.

#### Extractor (`extractor` notebook)

This notebook defines the **Extract** phase of the ETL pipeline. It extracts data from different sources and stores them in data frames for further processing.

```python
class Extractor:
    """
    Abstract class for data extraction.
    """
    def __init__(self):
        pass

    def extract(self):
        pass
```
- **`Extractor` Class**: An abstract class for defining data extraction logic.

```python
class AirpodsAfterIphoneExtractor(Extractor):
    def extract(self):
        transcatioInputDF = get_data_source(
            data_type="csv",
            file_path="local_path_to/Transaction_Updated.csv"
        ).get_data_frame()

        customerInputDF = get_data_source(
            data_type="delta",
            file_path="default.customer_delta_table_persist"
        ).get_data_frame()

        inputDFs = {
            "transcatioInputDF": transcatioInputDF,
            "customerInputDF": customerInputDF
        }

        return inputDFs
```
- **`AirpodsAfterIphoneExtractor` Class**: Inherits from `Extractor`. This class extracts transaction data from a CSV file and customer data from a Delta table. It uses the `get_data_source` function to get the appropriate data frames and returns them in a dictionary.

#### Transformer (`transform` notebook)

The **Transform** phase of the pipeline applies business logic to filter and transform the data to meet the requirements.

```python
class Transformer:
    def __init__(self):
        pass

    def transform(self, inputDFs):
        pass
```
- **`Transformer` Class**: An abstract class for defining data transformation logic.

```python
class AirpodsAfterIphoneTransformer(Transformer):
    def transform(self, inputDFs):
        transcatioInputDF = inputDFs.get("transcatioInputDF")
        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = transcatioInputDF.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
        )

        filteredDF = transformedDF.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
        )

        customerInputDF = inputDFs.get("customerInputDF")
        joinDF = customerInputDF.join(
            broadcast(filteredDF),
            "customer_id"
        )

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )
```
- **`AirpodsAfterIphoneTransformer` Class**: Inherits from `Transformer`. This class identifies customers who bought AirPods after purchasing an iPhone. It uses a window function to look ahead at the next product purchased by each customer and filters the resulting DataFrame to find the relevant transactions. Finally, it joins this filtered DataFrame with customer information based on `customer_id`.

```python
class OnlyAirpodsAndIphone(Transformer):
    def transform(self, inputDFs):
        transcatioInputDF = inputDFs.get("transcatioInputDF")

        groupedDF = transcatioInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        filteredDF = groupedDF.filter(
            (array_contains(col("products"), "iPhone")) &
            (array_contains(col("products"), "AirPods")) & 
            (size(col("products")) == 2)
        )

        customerInputDF = inputDFs.get("customerInputDF")
        joinDF = customerInputDF.join(
            broadcast(filteredDF),
            "customer_id"
        )

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )
```
- **`OnlyAirpodsAndIphone` Class**: Also inherits from `Transformer`. This class identifies customers who bought only iPhones and AirPods by grouping transactions per customer and checking the product set size. It then joins this information with customer details.

#### Loader Factory (`loader_factory` notebook)

The **Load** phase saves the transformed data into the appropriate sinks, using the **Factory Design Pattern** to abstract the logic of writing data.

```python
class DataSink:
    """
    Abstract class for data sinks.
    """
    def __init__(self, df, path, method, params):
        self.df = df
        self.path = path
        self.method = method
        self.params = params 

    def load_data_frame(self):
        raise ValueError("Not Implemented")
```
- **`DataSink` Class**: An abstract class for defining how data will be loaded into different formats.

```python
class LoadToLocal(DataSink):
    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)
```
- **`LoadToLocal` Class**: Implements `load_data_frame` to save DataFrames as files in local storage.

```python
class LoadToDeltaTable(DataSink):
    def load_data_frame(self):
        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)
```
- **`LoadToDeltaTable` Class**: This class saves a DataFrame as a Delta table in the specified path.

```python
def get_sink_source(sink_type, df, path, method, params=None):
    if sink_type == "local":
        return LoadToLocal(df, path, method, params)


    elif sink_type == "delta":
        return LoadToDeltaTable(df, path, method, params)
    else:
        raise ValueError(f"Not implemented for sink_type: {sink_type}")
```
- **`get_sink_source` Function**: This factory function returns an instance of the appropriate sink class based on the input type (local or Delta).

#### Loader (`loader` notebook)

This notebook contains the logic for saving the transformed data to the appropriate sinks, using the loader classes defined in the loader factory.

```python
class Loader:
    def __init__(self):
        pass

    def load(self):
        pass
```
- **`Loader` Class**: An abstract class for defining data loading logic.

```python
class AirPodsAfterIphoneLoader(Loader):
    def load(self, transformedDF):
        sink = get_sink_source(
            sink_type="local",
            df=transformedDF,
            path="local_path_to/airpods_after_iphone.csv",
            method="overwrite"
        )
        sink.load_data_frame()
```
- **`AirPodsAfterIphoneLoader` Class**: Inherits from `Loader`. This class loads the transformed data into a local CSV file.

```python
class OnlyAirpodsAndIphoneLoader(Loader):
    def load(self, transformedDF):
        sink = get_sink_source(
            sink_type="delta",
            df=transformedDF,
            path="default.only_airpods_and_iphone_delta_table",
            method="overwrite"
        )
        sink.load_data_frame()
```
- **`OnlyAirpodsAndIphoneLoader` Class**: This class loads the data into a Delta table.

#### Workflow Runner (`apple_analysis` notebook)

This is the main orchestration notebook that runs the entire ETL pipeline.

```python
class FirstWorkFlow:
    def __init__(self):
        self.extractor = AirpodsAfterIphoneExtractor()
        self.transformer = AirpodsAfterIphoneTransformer()
        self.loader = AirPodsAfterIphoneLoader()

    def run(self):
        extracted_data = self.extractor.extract()
        transformed_data = self.transformer.transform(extracted_data)
        self.loader.load(transformed_data)
```
- **`FirstWorkFlow` Class**: This class handles the first workflow where customers who bought AirPods after an iPhone are identified. It coordinates extraction, transformation, and loading steps.

```python
class SecondWorkFlow:
    def __init__(self):
        self.extractor = AirpodsAfterIphoneExtractor()
        self.transformer = OnlyAirpodsAndIphone()
        self.loader = OnlyAirpodsAndIphoneLoader()

    def run(self):
        extracted_data = self.extractor.extract()
        transformed_data = self.transformer.transform(extracted_data)
        self.loader.load(transformed_data)
```
- **`SecondWorkFlow` Class**: This class manages the second workflow, focusing on customers who bought only iPhones and AirPods.

```python
class WorkFlowRunner:
    def run(self):
        first_workflow = FirstWorkFlow()
        second_workflow = SecondWorkFlow()

        first_workflow.run()
        second_workflow.run()
```
- **`WorkFlowRunner` Class**: This class orchestrates the execution of both workflows.

---

### Conclusion

This project successfully implements a scalable ETL pipeline using Apache Spark, Delta Lake, and the factory design pattern to handle different data sources and sinks. The flexibility provided by this design allows for future extensions and modifications, ensuring maintainability and scalability for large-scale data processing tasks. The use of local file storage and Delta tables allows for efficient processing and storage of large datasets while enabling ACID compliance and versioning.
