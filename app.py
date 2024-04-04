from flask import Flask, jsonify, render_template
import pandas as pd


app = Flask(__name__)

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()
covidDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("data.csv")
column_order = ['Country','Cases','Deaths','Recovered','Active_Cases','Critical_Cases'] 

def dataframe_to_json_file(dataframe, file_path):
    # Write DataFrame directly to JSON file
    dataframe.write.json(path=file_path, mode="overwrite")




@app.route('/api/data', methods=['GET'])
def get_data():
    # Your logic her
    return dataframe_to_json(covidDF, column_order)


@app.route('/')
def home():
    # Serve an HTML page that has a link to the API endpoint
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)


