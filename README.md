# SPARK_ASSIGNMENT

This repository contains a Python-based project focused on extracting, processing, and visualizing COVID-19 data. Utilizing the powerful PySpark library for data processing and Flask for web application development, this project aims to present insightful analyses of COVID-19 statistics.

Project Structure
The project is structured as follows:


-> src 
   - **extract.py**: Responsible for fetching COVID-19 data from the Disease.sh API and storing it in a CSV format (data.csv).
   - **app.py**: A Flask application that converts the PySpark dataframes into HTML tables. It defines API routes to serve these tables, allowing users to interact with the analyzed
     data through a web interface.
   - **templates/index.html**: The HTML template for the application's user interface. It provides links to access the API routes and view the data analysis results.

->   **query.ipynb**: A Jupyter notebook that demonstrates the process of loading the CSV data into dataframes, querying these dataframes using PySpark to answer specific analytical
     questions, and presenting the findings.

-> config.yaml: contains API Link, app_name, port number, details that can or needs to be configured from user to user.
-> main.py: Main file that triggers app.py and extract.py file. First it carves out the API to extract csv file before performing pyspark analysis and outputting it on webpage using REST APIs.

# Installation and Setup
- **Prerequisites**
: Before setting up the project, ensure you have Python installed on your system. You will also need PySpark, Flask and Yaml. You can install these packages using pip command. All dependecies are mentioned in **requirements.txt** file

- **Running the Application**
: Clone the repository to your local machine:
```
git clone https://github.com/owais-ash/SPARK_ASSIGNMENT.git
cd SPARK_ASSIGNMENT
```
Coming to Terminal
1. Run main.py 
```
python3 main.py
```

Open the outputting localhost URL on any browser(if it doesn't opens automatically)
   
![image](https://github.com/owais-ash/SPARK_ASSIGNMENT/assets/158836234/2d57a8bc-e798-4295-b705-4a5c5276b634)
![image](https://github.com/owais-ash/SPARK_ASSIGNMENT/assets/158836234/961c4a97-775c-48e9-890c-e05123efce2b)
