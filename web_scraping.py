import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, date_format, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging

GITHUB_API_TOKEN = 'ghp_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX' # meu token

headers = {
    'Authorization': f'token {GITHUB_API_TOKEN}'
}

def get_followers(username: str) -> dict:
    """
    Retrieves the followers of a given GitHub user.

    Args:
        username (str): The username of the GitHub user.

    Returns:
        dict: A dictionary containing the response from the API call.

    """
    try:
        url = f'https://api.github.com/users/{username}/followers'
        response = requests.get(url, headers=headers)

        return response.json()
    except Exception as e:
      logging.info(f"An error occurred while getting followers: {e}")
      raise e

def get_user_details(username: str) -> dict:
    """
    Retrieves the details of a user from the GitHub API.

    Args:
        username (str): The username of the user.

    Returns:
        dict: A dictionary containing the user details.
    """
    try:
        url = f'https://api.github.com/users/{username}'
        response = requests.get(url, headers=headers)

        return response.json()
    except Exception as e:
      logging.info(f"An error occurred while getting user details: {e}")
      raise e

if __name__ == "__main__":
    username = 'cvscarlos'
    followers = get_followers(username)

    spark = SparkSession.builder.appName("App").getOrCreate()

    schema = StructType([
        StructField('login', StringType(), True),
        StructField('name', StringType(), True),
        StructField('company', StringType(), True),
        StructField('blog', StringType(), True),
        StructField('email', StringType(), True),
        StructField('bio', StringType(), True),
        StructField('public_repos', IntegerType(), True),
        StructField('followers', IntegerType(), True),
        StructField('following', IntegerType(), True),
        StructField('created_at', StringType(), True),
    ])

    spark_df = spark.createDataFrame([], schema=schema)

    for follower in followers:
        user_data = get_user_details(follower['login'])
        user_df = spark.createDataFrame([user_data], schema=schema)
        spark_df = spark_df.union(user_df)

    spark_df = spark_df.withColumn("company", regexp_replace("company", "@", ""))
    spark_df = spark_df.withColumn("created_at", to_date("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    spark_df = spark_df.withColumn("created_at", date_format("created_at", "dd/MM/yyyy"))

    selected_cols = ['login', 'name', 'company', 'blog', 'email', 'bio', 'public_repos', 'followers', 'following', 'created_at']
    spark_df = spark_df.select(*selected_cols)

    spark_df.show(5)
    spark_df.write.csv(f"github_followers_{username}.csv", header=True)
