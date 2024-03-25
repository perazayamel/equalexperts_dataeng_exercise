
## :warning: Please read these instructions carefully and entirely first
* Clone this repository to your local machine.
* Use your IDE of choice to complete the assignment.
* When you have completed the assignment, you need to  push your code to this repository and [mark the assignment as completed by clicking here](https://app.snapcode.review/submission_links/77d06b39-d2ed-4f6b-bfd5-b4fe0d60d366).
* Once you mark it as completed, your access to this repository will be revoked. Please make sure that you have completed the assignment and pushed all code from your local machine to this repository before you click the link.

**Table of Contents**
1. [Before you start](#before-you-start), a brief explanation for the exercise and software prerequisites/setup.  
2. [Tips for what we are looking for](#tips-on-what-were-looking-for) provides clear guidance on solution qualities we value 
3. [The Challenge](#begin-the-two-part-challenge) explains the data engineering code challenge to be tackled.
4. [Follow-up Questions](#follow-up-questions) related to the challenge which you should address  
5. [Your approach and answers to follow-up questions](#your-approach-and-answers-to-follow-up-questions-) is where you should include the answers to the follow-up question and clarify your solution approach any assumptions you have made.

## Before you start
### Why complete this task?

We want to make the interview process as simple and stress-free as possible. That’s why we ask you to complete 
the first stage of the process from the comfort of your own home.

Your submission will help us to learn about your skills and approach. If we think you’re a good fit for our 
network, we’ll use your submission in the next interview stages too.

### About the task

You’ll be creating an ingestion process to ingest files containing vote data. You’ll also create a means to 
query the ingested data to determine outlier weeks.

There’s no time limit for this task, but we expect it to take less than 2 hours.

### Software prerequisites
To make it really easy for candidates to do this exercise regardless of their operating system, we've provided a containerised
way to run the exercise. To make use of this, you'll need to have [Docker](https://www.docker.com/products/docker-desktop/) (or a free equivalent like [Rancher](https://rancherdesktop.io/) or [Colima](https://github.com/abiosoft/colima)) installed
on your system.

To start the process, there is a `Dockerfile` in the root of the project.  This defines a linux-based container that includes Python 3.11,
Poetry and DuckDB.

To build the container:
```shell
docker build -t ee-data-engineering-challenge:0.0.1 .
```

To run the container after building it:

_Mac or Linux, or Windows with WSL:_
```shell
docker run --mount type=bind,source="$(pwd)/",target=/home/dataeng -it ee-data-engineering-challenge:0.0.1 bash
```

_Windows (without WSL):_
```shell
docker run --mount type=bind,source="%cd%",target=/home/dataeng -it ee-data-engineering-challenge:0.0.1 bash
```

Running the container opens up a terminal in which you can run the poetry commands that we describe next.

You could also proceed without the container by having Python 3.11 and [Poetry](https://python-poetry.org/)
directly installed on your system. In this case, just run the poetry commands you see described here directly in your
own terminal.

### Poetry

This exercise has been written in Python 3.11 and uses [Poetry](https://python-poetry.org/) as a dependency manager.
If you're unfamiliar with Poetry, don't worry -- the only things you need to know are:

1. Poetry automatically updates the `pyproject.toml` file with descriptions of your project's dependencies to keep 
   track of what libraries are being used.
2. To add a dependency for your project, use `poetry add thelibraryname`
3. To add a "dev" dependency (e.g. a test library or linter rather than something your program depends on)
   use `poetry add --dev thelibraryname`
4. To resolve dependencies and find compatible versions, use `poetry lock`
5. *Please commit any changes to your `pyproject.toml` and `poetry.lock` files and include them in your submission*
   so that we can replicate your environment.


In the terminal (of the running docker image), start off by installing the dependencies:
```shell
poetry install --with dev
```

> **Warning**
> If you're a Mac M1/M2 (arm-based) user, note that
> as of June 2023, DuckDB doesn't release pre-built `aarch64` linux
> wheels for the Python `duckdb` library (yet). This
> means that the dependency installation in the running container can take 
> some time (10mins?) as it compiles `duckdb` from source. 
> If you don't feel like waiting, you can build an `amd64` Docker container with
> by adding the `--platform amd64` flag to both the `docker build` and
> `docker run` commands above (i.e. you'll have to re-run those). 
> This image will run seamlessly on your Mac in emulation mode.


Now type
```shell
poetry run exercise --help
```
    
to see the options in CLI utility we provide to help you run the exercise. For example, you could do

```shell
poetry run exercise ingest-data
```

to run the ingestion process you will write in `ingest.py`


### Bootstrap solution

This repository contains a bootstrap solution that you can use to build upon. 

You can make any changes you like, as long as the solution can still be executed using the `exercise.py` script. 

The base solution uses [DuckDB](https://duckdb.org/) as the database, and for your solution we want you to treat it like a real (OLAP) data warehouse.
The database should be saved in the root folder of the project on the local disk as `warehouse.db`, as shown in the `tests/db_test.py` file.

We also provide the structure for:
* `equalexperts_dataeng_exercise/ingest.py` -  the entry point for running the ingestion process.
* `equalexperts_dataeng_exercise/outliers.py` - the entry point for running the outlier detection query.
* `equalexperts_dataeng_exercise/db.py` - is empty, but the associated test demonstrates interaction with an DuckDB database.

### Tips on what we’re looking for


1. **Test coverage**

    Your solution must have good test coverage, including common execution paths.

2. **Self-contained tests**

    Your tests should be self-contained, with no dependency on being run in a specific order.

3. **Simplicity**

    We value simplicity as an architectural virtue and a development practice. Solutions should reflect the difficulty of the assigned task, and shouldn’t be overly complex. We prefer simple, well tested solutions over clever solutions. 

    Please avoid:

   * unnecessary layers of abstraction
   * patterns
   * custom test frameworks
   * architectural features that aren’t called for
   * libraries like `pandas` or `polars` or frameworks like `PySpark` or `ballista` - we know that this exercise can be
     solved fairly trivially using these libraries and a Dataframe approach, and we'd encourage appropriate 
     use of these in daily work contexts. But for this small exercise we really 
     want to know more about how you structure, write and test your Python code,
     and want you to show some fluency in SQL -- a `pandas`
     solution won't allow us to see much of that.

4. **Self-explanatory code**

    The solution you produce must speak for itself. Multiple paragraphs explaining the solution is a sign 
    that the code isn’t straightforward enough to understand on its own.
    However, please do explain your non-obvious _choices_ e.g. perhaps why you decided to load
    data a specific way.

5. **Demonstrate fluency with data engineering concepts**
   
   Even though this is a toy exercise, treat DuckDB as you would an OLAP 
   data warehouse. Choose datatypes, data loading methods, optimisations and data models that 
   are suited for resilient analytics processing at scale, not transaction processing.

6. **Dealing with ambiguity**

    If there’s any ambiguity, please add this in a section at the bottom of the README. 
    You should also make a choice to resolve the ambiguity and proceed.

Our review process starts with a very simplistic test set in the `tests/exercise_tests` folder which you should also
check before submission. You can run these with:
```shell
poetry run exercise check-ingestion
poetry run exercise check-outliers
```

Expect these to fail until you have completed the exercise.

You should not change the `tests/exercise-tests` folder and your solution should be able to pass both tests.


## Download the dataset for the exercise
Run the command
```
poetry run exercise fetch-data
```

which will fetch the dataset, uncompress it and place it in `uncommitted/votes.jsonl` for you.
Explore the data to see what values and fields it contains (no need to show how you explored it).

## Begin the two-part challenge
There are two parts to the exercise, and you are expected to complete both. 
A user should be able to execute each task independently of the other. 
For example, ingestion shouldn't cause the outliers query to be executed.

### Part 1: Ingestion
Create a schema called `blog_analysis`.
Create an ingestion process that can be run on demand to ingest files containing vote data. 
You should ensure that data scientists, who will be consumers of the data, do not need to consider 
duplicate records in their queries. The data should be stored in a table called `votes` in the `blog_analysis` schema. 

### Part 2: Outliers calculation
Create a view named `outlier_weeks`  in the `blog_analysis` schema. It will contain the output of a SQL calculation for which weeks are regarded as outliers based on the vote data that was ingested.
The view should contain the year, week number and the number of votes for the week _for only those weeks which are determined to be outliers_, according to the following rule:

NB! If you're viewing this Markdown document in a viewer
where the math isn't rendering, try viewing this README in GitHub on your web browser, or [see this pdf](docs/calculating_outliers.pdf).

> 
> **A week is classified as an outlier when the total votes for the week deviate from the average votes per week for the complete dataset by more than 20%.**</br>  
> For the avoidance of doubt, _please use the following formula_: 
>  
> > Say the mean votes is given by $\bar{x}$ and this specific week's votes is given by $x_i$. 
> > We want to know when $x_i$ differs from $\bar{x}$ by more than $20$%. 
> > When this is true, then the ratio $\frac{x_i}{\bar{x}}$ must be further from $1$ by more than $0.2$, i.e.: </br></br> 
> > $\big|1 - \frac{x_i}{\bar{x}}\big| > 0.2$

The data should be sorted in the view by year and week number, with the earliest week first.

Running `outliers.py` should recreate the view and 
just print the contents of this `outlier_weeks` view to the terminal - don't do any more calculations after creating the view.

## Example

The sample dataset below is included in the test-resources folder and can be used when creating your tests.

Assuming a file is ingested containing the following entries:

```
{"Id":"1","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-02T00:00:00.000"}
{"Id":"2","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"4","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"5","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"6","PostId":"5","VoteTypeId":"3","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"7","PostId":"3","VoteTypeId":"2","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"8","PostId":"4","VoteTypeId":"2","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"9","PostId":"2","VoteTypeId":"2","CreationDate":"2022-01-23T00:00:00.000"}
{"Id":"10","PostId":"2","VoteTypeId":"2","CreationDate":"2022-01-23T00:00:00.000"}
{"Id":"11","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-30T00:00:00.000"}
{"Id":"12","PostId":"5","VoteTypeId":"2","CreationDate":"2022-01-30T00:00:00.000"}
{"Id":"13","PostId":"8","VoteTypeId":"2","CreationDate":"2022-02-06T00:00:00.000"}
{"Id":"14","PostId":"13","VoteTypeId":"3","CreationDate":"2022-02-13T00:00:00.000"}
{"Id":"15","PostId":"13","VoteTypeId":"3","CreationDate":"2022-02-20T00:00:00.000"}
{"Id":"16","PostId":"11","VoteTypeId":"2","CreationDate":"2022-02-20T00:00:00.000"}
{"Id":"17","PostId":"3","VoteTypeId":"3","CreationDate":"2022-02-27T00:00:00.000"}
```

Then the following should be the content of your `outlier_weeks` view:


| Year | WeekNumber | VoteCount |
|------|------------|-----------|
| 2022 | 0          | 1         |
| 2022 | 1          | 3         |
| 2022 | 2          | 3         |
| 2022 | 5          | 1         |
| 2022 | 6          | 1         |
| 2022 | 8          | 1         |

**Note that we strongly encourage you to use this data as a test case to ensure that you have the correct calculation!**

## Follow-up Questions

Please include instructions about your strategy and important decisions you made in the README file. You should also include answers to the following questions:

1. What kind of data quality measures would you apply to your solution in production?
2. What would need to change for the solution scale to work with a 10TB dataset with 5GB new data arriving each day?
3. Please tell us in your modified README about any assumptions you have made in your solution (below).

## Your Approach and answers to follow-up questions 

_Please provide an explaination to your implementation approach and the additional questions **Implementation Strategy and Key Decisions

# Overview
The Equal Experts Data Engineering Challenge involves creating a robust data ingestion and analysis system using DuckDB, an in-memory database optimized for OLAP (Online Analytical Processing) tasks. The project is divided into three main components: db for database interactions, ingest for data ingestion, and outlier for identifying outlier weeks based on vote data.

## db Implementation
The db module serves as the backbone of the project, providing foundational database interactions such as connection management, schema setup, and data loading. Key decisions in this implementation include:

### Schema and Table Setup: 
Opted for a flexible setup function that can create any schema and table based on provided definitions. This allows for easy adaptation to changes in data structure.

### JSON Ingestion to Staging Area: 
Implemented a method to load JSON data directly into a staging table, using DuckDB's ability to parse JSON. This staging area is crucial for data quality checks before moving data to the operational tables.

### Data Cleansing and Deduplication: 
Created a comprehensive process to cleanse data, identify duplicates, and mark records with a status code, ensuring only clean, unique data is loaded into the operational area.

### CTAS Approach for Data Movement: 
Adopted a Create Table As Select (CTAS) strategy for efficient data transformation and loading from staging to operational tables, improving performance for large datasets.

## ingest Implementation
The ingest script orchestrates the entire data ingestion process, utilizing the db module's capabilities. It validates file existence, loads data to a staging area, performs data cleansing, and moves clean data to operational tables. A significant decision was to process data in stages to ensure data integrity and facilitate error handling.

## outlier Implementation
The outlier script focuses on analyzing ingested data to identify outlier weeks. It leverages SQL analytical functions to compute weekly vote counts and compare them against the average to find significant deviations. The use of a custom week numbering system, considering the first week of January as week 0 under certain conditions, was a notable decision to align with specific business logic requirements.


# Key Decisions and Assumptions
Throughout the development of our data processing solution for handling vote data, several critical decisions and assumptions were made to guide our approach and design. Below, we outline these key points and the rationale behind them:

### Choice of DuckDB for OLAP:
- **Assumption**: Given the project's analytical nature, it was assumed that an OLAP (Online Analytical Processing) approach would be most suitable. 
- **Decision**: We opted to use DuckDB, as it is explicitly designed to handle OLAP workloads efficiently. DuckDB's columnar storage and ability to execute complex analytical queries quickly made it an ideal choice for our needs, despite its simplicity compared to distributed systems. This choice was reinforced by the project guidelines directing its use.

### Data Quality and Integrity:
- **Assumption**: Data quality is paramount for accurate analysis and reporting. It was assumed that the incoming data could have inconsistencies or errors that need addressing before operational use.
- **Decision**: Implemented a comprehensive data cleansing and deduplication strategy within the staging area before moving data to the operational layer. This ensures that only clean, unique records are available for analysis, improving the reliability of insights derived from the data.

### Reliance on SQL and Built-in Database Functions:
- **Assumption**: Utilizing database-centric operations for data manipulation and transformation would leverage DuckDB's performance optimizations and reduce dependency on external libraries.
- **Decision**: Avoided the use of external data manipulation libraries such as Pandas in favor of SQL and DuckDB's built-in functions. This approach aligns with the project's analytical focus, ensuring efficient data processing and transformation within the database environment.

### Incremental and Idempotent Data Processing:
- **Assumption**: The system should accommodate continuous data growth and ensure that repeated data ingestion operations do not lead to duplicates or data integrity issues.
- **Decision**: Designed the data ingestion process to be both incremental, handling daily data loads efficiently, and idempotent, ensuring that repeated ingestion of the same data does not affect the integrity of the dataset.

### Error Handling and Logging:
- **Assumption**: Effective error handling and logging are crucial for maintaining data integrity and facilitating debugging and issue resolution.
- **Decision**: Implemented robust error handling mechanisms throughout the data ingestion process, along with detailed logging. This provides transparency into the ingestion pipeline's operations and aids in quickly identifying and addressing any issues.

### Scalability and Future Enhancements:
- **Assumption**: While the current solution is tailored to the project's current scope, it is anticipated that future requirements may necessitate scalability and additional features.
- **Decision**: The architecture and design choices, such as the separation of concerns between data cleansing, loading, and outlier calculation, lay a foundation for scalability. Future enhancements could include adopting distributed data processing frameworks or integrating machine learning models for advanced anomaly detection.

By articulating these decisions and assumptions, we aim to provide clarity on our solution's foundation and its alignment with the project's objectives and constraints. These choices reflect a balance between leveraging DuckDB's strengths, ensuring data quality, and maintaining the flexibility to adapt to future requirements.

![Successful Test Completion](https://github.com/EqualExperts-Assignments/equal-experts-lordly-natty-slip-7e5799ffdf9e/blob/solution/Screenshot%202024-03-25.pdf))


## 1. What kind of data quality measures would you apply to your solution in production?
For a production environment where data quality and integrity are paramount, implementing robust data quality measures is essential. Leveraging microservices and specialized tools for data quality and data profiling can provide a modular and scalable approach to ensuring data accuracy and consistency.

While the simple SQL approach demonstrated in the implementation is effective for initial data cleansing and deduplication, it may not scale well for larger datasets or more complex data quality requirements. To address these challenges, integrating parallel processing techniques can significantly improve efficiency and performance, allowing for more comprehensive data quality checks to be executed in less time.

In a typical data warehouse architecture, staging areas play a crucial role in the ETL (Extract, Transform, Load) process, serving as an intermediary step where data can be cleansed, transformed, and enriched before being loaded into the operational database. Incorporating ETL fields in the staging area to track errors and logs is a best practice, enabling detailed monitoring and auditing of the data processing pipeline. This approach facilitates identifying and resolving data quality issues early in the process.

Expanding beyond a simple data warehouse to a more distributed architecture, utilizing a data lake for raw data storage and processing can offer greater flexibility in handling diverse data formats and large volumes of data. A centralized data repository, or data warehouse, can then serve as the curated, single source of truth for downstream analytics and reporting, ensuring that all consumers have access to high-quality, consistent data.

The adoption of object-oriented programming languages like Python for data processing and quality checks provides the flexibility and functionality necessary for dealing with complex data quality scenarios. Python's extensive libraries and frameworks support a wide range of data manipulation, cleansing, and validation tasks, making it an ideal choice for implementing custom data quality rules and integrations with external data quality tools.

In summary, to ensure data quality in production:
- Leverage microservices and specialized tools for scalable and modular data quality and profiling.
- Implement parallel processing for enhanced efficiency and performance in data quality checks.
- Utilize staging areas with ETL fields for error tracking and logging, supporting early identification and resolution of data issues.
- Consider a distributed architecture with a data lake for flexibility in handling diverse and large datasets, alongside a centralized data repository for curated, high-quality data.
- Employ object-oriented programming languages like Python for their flexibility and extensive support for data manipulation and validation tasks, allowing for custom data quality measures tailored to specific requirements.

## 2. What would need to change for the solution scale to work with a 10TB dataset with 5GB new data arriving each day?
Scaling a data processing solution to handle a 10TB dataset with 5GB of new data arriving each day requires a comprehensive approach, focusing on performance, efficiency, and scalability. The initial solution's simple SQL approach and use of Python for orchestration serve as a solid foundation but would need significant enhancements to meet the demands of this scale. Here are the key changes and considerations:

### Distributed Data Processing Systems:
Leveraging distributed data processing systems such as Apache Spark and MPP(s) e.g. Snowflake, BigQuery, ReadShipt can significantly improve processing speed and efficiency. These systems are designed to handle large-scale data operations across clusters, enabling parallel processing and fault tolerance. They can efficiently process large datasets, including streaming data, making them ideal for daily ingestion of 5GB new data.

### Data Lake Architecture:
Implementing a data lake architecture can accommodate the vast volumes of data more flexibly than traditional data warehouses. A data lake allows for storing raw data in its native format, supporting scalable storage solutions like Amazon S3 or Azure Data Lake (ADL). Data lakes facilitate the handling of semi-structured or unstructured data, providing a more adaptable environment for big data analytics.

### Enhanced ETL and Data Quality Checks:
For handling large datasets, optimizing ETL (Extract, Transform, Load) processes is crucial. This involves parallelizing data transformations and employing more sophisticated data quality checks. Utilizing a microservices architecture for data quality services can allow for distributed processing of data validation and cleansing tasks, ensuring data integrity at scale.

### Advanced Data Storage Solutions:
Considering the volume of data, employing columnar storage formats like Parquet or ORC or columnar databases like Snowfalke, ReadShift, etc. (MPP)  can improve read performance and optimize storage. These formats are especially beneficial for analytical queries as they allow for efficient compression and encoding schemes. Integrating these with distributed computing platforms enhances query performance across large datasets.

### Incremental Loading and Change Data Capture (CDC):
To efficiently manage daily data ingestion, implementing incremental loading and Change Data Capture (CDC) techniques can minimize the load on the system and ensure only new or changed data is processed. This reduces the computational overhead and storage requirements, making the system more responsive and cost-effective.

### Automated Scaling and Resource Management:
Employing cloud-based solutions with auto-scaling capabilities can ensure the infrastructure adjusts according to the load, maintaining performance while optimizing costs. Container orchestration tools like Kubernetes can manage microservices for data processing, allowing for dynamic scaling based on the workload.

### Data Governance and Lifecycle Management:
As the data volume grows, implementing robust data governance and lifecycle management policies becomes essential. This includes data retention policies, archiving strategies, and secure data access controls to ensure compliance with data regulations and optimize storage costs.

In summary, scaling to handle a 10TB dataset with daily additions of 5GB requires embracing distributed processing systems, optimizing storage and ETL processes, and adopting scalable and flexible infrastructure solutions. Emphasizing data quality, governance, and lifecycle management ensures the long-term sustainability and integrity of the data ecosystem.
**_
