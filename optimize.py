'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

import os


spark = SparkSession.builder.appName('Optimize I').getOrCreate()

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3]) 

answers_input_path = os.path.join(base_path, 'data\\answers')

questions_input_path = os.path.join(base_path, 'data\\questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()

#optimized quesionsDF by removing the columns not needed
questionsDF_optimized = questionsDF.select('question_id','title')

'''
Answers aggregation

Here we : get number of answers per question per month
'''

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

#now we can obtain the same resultDF, by using the optimized questionsDF
#resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF = questionsDF_optimized.join(answers_month, 'question_id')


resultDF.orderBy('question_id', 'month').show()

'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''
#see optimized.html for details of results from Jupyter Notebook.
