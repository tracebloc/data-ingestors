from mysql.connector import connect
import os

dbM = connect(
    host=os.getenv("MYSQL_HOST", "mysql"),
    user=os.getenv("MYSQL_USER", "edgeuser"),
    password=os.getenv("MYSQL_PASSWORD"),
    database="xraymetadata",
    autocommit=True,
)

dbP = connect(
    host=os.getenv("MYSQL_HOST", "mysql"),
    user=os.getenv("MYSQL_USER", "edgeuser"),
    password=os.getenv("MYSQL_PASSWORD"),
    database="xraymetadata",
    autocommit=True,
)
