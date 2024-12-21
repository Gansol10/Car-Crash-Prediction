
Overview

This project aims to create a streamlined pipeline for downloading, processing, and visualizing car crash data using modern data engineering and visualization tools. The primary goal is to demonstrate technical skills in data engineering, data analysis, and data visualization.

Objective

1. Automatically updating datasets using Apache Airflow.

2. Cleaning and validating data using Pandas and Great Expectations.

3. Visualizing data trends through an interactive dashboard in Kibana.

Dataset

Source: Kaggle Car Crash Dataset

1. Size: Approx. 1GB

2. Format: CSV

3. Features:

- Location

- Time of crash

- Weather conditions

- Severity level

Technology Stack

- Programming Languages: Python, SQL

- Libraries: Pandas, Numpy, Matplotlib, Seaborn, Scikit-learn, Scipy

- Tools: Apache Airflow, Docker, Elasticsearch, Kibana

- Database: PostgreSQL

- Validation: Great Expectations

Workflow

1. Data Acquisition:

Utilizing Apache Airflow to schedule and download the dataset from the Kaggle API.

2. Data Validation:

Ensuring data quality with Great Expectations by checking for missing values, duplicates, and schema adherence.

3. Data Cleaning:

Using Pandas to clean and transform the data, handling missing values and creating derived features.

4. Data Storage:

Loading cleaned data into Elasticsearch for indexing.

5. Data Visualization:

Building an interactive dashboard in Kibana to explore trends and patterns in car crash data.

Installation and Setup

1. Clone the repository:

git clone https://github.com/Gansol10/Car-Crash-Visualization-Kibana.git

2. Navigate to the project directory:

cd Car-Crash-Visualization-Kibana

3. Build and run Docker containers:

docker-compose up

4. Access Apache Airflow:

URL: http://localhost:8080

Default login: admin/admin

5. Access Kibana Dashboard:

URL: http://localhost:5601

Results and Insights

- Top Crash Locations: Cities with the highest number of crashes.

- Crash Severity Trends: Insights into factors contributing to high-severity crashes.

- Weather Impact: Analysis of crashes under different weather conditions.
