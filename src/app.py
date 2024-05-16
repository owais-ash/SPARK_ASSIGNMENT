from flask import Flask, jsonify, render_template
import pygwalker as pyg
from pyspark.sql import SparkSession
import yaml

app = Flask(__name__)


with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Setup Spark session
spark = SparkSession.builder \
    .appName(config['spark']['app_name']) \
    .master(config['spark']['master']) \
    .getOrCreate()

covidDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("data.csv")
column_order = ['Country','Cases','Deaths','Recovered','Active_Cases','Critical_Cases'] 
most_affected_country = covidDF.select("Country", (covidDF.Deaths / covidDF.Cases).alias("Death Rate")).orderBy("Death Rate", ascending=False).limit(1)
least_affected_country = covidDF.select("Country", (covidDF.Deaths / covidDF.Cases).alias("Death Rate")).orderBy("Death Rate", descending=False).limit(1)
highest_covid_cases = covidDF.select("Country", (covidDF.Deaths).alias("Total Deaths")).orderBy("Total Deaths", ascending=False).limit(1)
minimum_covid_cases = covidDF.select("Country", (covidDF.Deaths).alias("Total Deaths")).orderBy("Total Deaths", ascending=True).limit(1)
total_cases = covidDF.agg({"Cases": "sum"})
most_efficient_country = covidDF.select("Country", (covidDF.Recovered / covidDF.Cases).alias("Recovery Rate")).orderBy("Recovery Rate", ascending=False).limit(1)
least_efficient_country = covidDF.select("Country", (covidDF.Recovered / covidDF.Cases).alias("Recovery Rate")).orderBy("Recovery Rate", ascending=True).limit(1)
least_critical_cases = covidDF.select("Country", (covidDF.Critical_Cases).alias("Critical Cases")).orderBy("Critical Cases", ascending=True).limit(1)
highest_critical_cases = covidDF.select("Country", (covidDF.Critical_Cases).alias("Critical Cases")).orderBy("Critical Cases", ascending=False).limit(1)


def dataframe_to_table(dataframe):
    """Convert a Spark DataFrame to a JSON-serializable Python data structure."""
    # Convert to Pandas DataFrame first for simplicity
    pandas_df = dataframe.toPandas()
    html_str = pandas_df.to_html(index=False, border=1)
    return html_str




@app.route('/api/data', methods=['GET'])
def get_data():
    return dataframe_to_table(covidDF)

@app.route('/api/most_affected', methods=['GET'])
def get_data1():
    return dataframe_to_table(most_affected_country)

@app.route('/api/least_affected', methods=['GET'])
def get_data2():
    return dataframe_to_table(least_affected_country)

@app.route('/api/highest_cases', methods=['GET'])
def get_data3():
    return dataframe_to_table(highest_covid_cases)

@app.route('/api/lowest_cases', methods=['GET'])
def get_data4():
    return dataframe_to_table(minimum_covid_cases)

@app.route('/api/total_cases', methods=['GET'])
def get_data5():
    return dataframe_to_table(total_cases)

@app.route('/api/most_efficient', methods=['GET'])
def get_data6():
    return dataframe_to_table(most_efficient_country)

@app.route('/api/least_efficient', methods=['GET'])
def get_data7():
    return dataframe_to_table(least_efficient_country)

@app.route('/api/least_suffering', methods=['GET'])
def get_data8():
    return dataframe_to_table(least_critical_cases)

@app.route('/api/still_suffering', methods=['GET'])
def get_data9():
    return dataframe_to_table(highest_critical_cases)



@app.route('/')
def home():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)


