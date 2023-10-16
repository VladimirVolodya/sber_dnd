import time
from sys import argv
from typing import Dict

import psycopg2
from flask import Flask, make_response


def connect_to_db(info: Dict[str, str]) -> psycopg2.connection:
    delay = 20
    while True:
        try:
            conn = psycopg2.connect(
                dbname=info["db"],
                user=info["db_user"],
                password=info["db_password"],
                host=info["host"],
            )
            return conn
        except psycopg2.OperationalError as error:
            print(f"Atempt connect to database failed. {error}")
            time.sleep(delay)
            continue


def init_db(info: Dict[str, str]) -> None:
    with connect_to_db(info) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS
                DATA(
                    KEY VARCHAR(20) PRIMARY KEY,
                    VALUE VARCHAR(100) NOT NULL
                )
                """
            )


app = Flask(__name__)
info = {
    "db": argv[1],
    "db_user": argv[2],
    "db_password": argv[3],
    "host": argv[4],
}
init_db(info)


@app.route("/healthcheck", methods=["GET"])
def healthcheck():
    resp = make_response("Ok\n", 200)
    resp.mimetype = "text/plain"
    return resp


@app.route("/storage/put/<key>/<value>", methods=["POST"])
def put_value(key: str, value: str):
    with connect_to_db(info) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO DATA
                (KEY, VALUE)
                VALUES (%s, %s)
                ON CONFLICT (KEY) DO UPDATE
                SET VALUE = EXCLUDED.VALUE
                """,
                (key, value),
            )
    resp = make_response("Ok\n", 200)
    resp.mimetype = "text/plain"
    return resp


@app.route("storage/get/<key>", method=["GET"])
def get_value(key: str):
    res = None
    with connect_to_db(info) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT VALUE
                FROM DATA
                WHERE KEY = %s
                """,
                (key,),
            )
            for r in cur.fetchone():
                res = r[0]
                break
    if res is not None:
        resp = make_response(res, 200)
        resp.mimetype = "text/plain"
        return resp
    else:
        resp = make_response("Key not found", 404)
        resp.mimetype = "text/plain"
        return resp
