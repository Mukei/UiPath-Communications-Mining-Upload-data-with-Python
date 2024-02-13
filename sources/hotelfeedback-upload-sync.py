import csv
import json
import os
import sys
import uuid
from datetime import datetime

import requests
from html2text import html2text

MAX_BATCH_SIZE = 256
ERRORS_FOLDER = "errors"

# 下記をご自身の<組織名>と<テナント名>に置き換えてください。
BASE_URL = "https://cloud.uipath.com/<組織名>/<テナント名>/reinfer_"

# 以下を自分の<プロジェクト名>と<ソース名>に置き換える。
# 空白は "-" に置き換えられる
SOURCE = "<プロジェクト名>/<ソース名>"

# 環境変数にREINFER_TOKENを追加している場合は、以下の行を利用してください。
# TOKEN = os.environ["REINFER_TOKEN"]
# そうでない場合は、以下を利用してください。
TOKEN = "YOUR API TOKEN"

# フィールドマップ
ID_REVIEW = 0
HOTEL_NAME = 1
REVIEWER_NAME = 2
REVIEWER_GENDER_AGEGROUP = 3
REVIEW_DATE = 4
REVIEW_SCORE = 5
REVIEW_URL = 6
REVIEW_TITLE = 7
REVIEW_PLAIN = 8
REVIEW_HTML = 9

# REVIEW_HTMLカラムにテキストが含まれているかチェックする
def html_body_exists(row):
    review_html = row[REVIEW_HTML].strip()
    body_exists = html2text(review_html).strip() != ""
    return body_exists

# 各行の前処理（ストリップ、日付列のテキスト除去、日付/時刻のフォーマット）
# Communications MiningのAPIが期待するJSON形式でデータを返す
def row_to_document(row):
    review_html = row[REVIEW_HTML].strip()
    id_review = row[ID_REVIEW].strip()
    review_date = row[REVIEW_DATE].strip().replace('投稿日：', '')
    reviewer_name = row[REVIEWER_NAME].strip()
    review_title = row[REVIEW_TITLE].strip()
    review_plain = row[REVIEW_PLAIN].strip()
    review_url = row[REVIEW_URL].strip()
    review_score = row[REVIEW_SCORE].strip()
    reviewer_gender, reviewer_agegroup = row[REVIEWER_GENDER_AGEGROUP].strip().split('/')
    hotel_name = row[HOTEL_NAME].strip()
    
    timestamp = datetime.strptime(review_date, "%Y/%m/%d")
    timestamp_str = timestamp.strftime("%a, %d %b %Y %H:%M:%S %z")

    return {
        "raw_email": {
            "body": {"html": review_html}
            if html_body_exists(row)
            else {"plain": review_plain},
            "headers": {
                "parsed": {
                    "Date": timestamp_str,
                    "From": reviewer_name,
                    "Message-ID": id_review,
                    "Subject": review_title,
                }
            },
        },
        "user_properties": {
            "string:Hotel_Name": str(hotel_name),
            "string:Reviewer_Name": str(reviewer_name),
            "string:Reviewer_Gender": str(reviewer_gender),
            "string:Reviewer_AgeGroup": str(reviewer_agegroup),
            "number:Review_Score": int(review_score) if (review_score != '') else 0,
            "string:Review_URL": str(review_url),
        },
    }

documents = []

# データをCommunications Miningにアップロードする
def upload_batch(documents):
    response = requests.post(
        f"{BASE_URL}/api/v1/sources/{SOURCE}/sync-raw-emails",
        headers={"Authorization": f"Bearer {TOKEN}"},
        json={
            "documents": documents,
            "transform_tag": "generic.0.CONVKER5",
        },
    )

    if response.status_code != 200:
        print("❌❌❌\n\nAPI呼び出しエラー\n\n❌❌❌")

        # errorsフォルダが存在しない場合は作成する
        if not os.path.exists(ERRORS_FOLDER):
            os.makedirs(ERRORS_FOLDER)

        with open(ERRORS_FOLDER + "/" + str(uuid.uuid4()), "a") as error_dump:
            error_dump.write(json.dumps(documents) + "\n")
            return

    print(json.dumps(response.json(), indent=2, sort_keys=True))

# 長い文字列を含むセルを処理するために必要
csv.field_size_limit(sys.maxsize)

# 現在のパスを取得
os.getcwd()

with open("sources/dataset.csv") as csvfile:
    reader = csv.reader(csvfile)

    for idx, row in enumerate(reader):
        if idx == 0:
            print("ヘッダー行のスキップ")
            continue

        if idx % 100 == 0:
            print(f"処理済み{idx}行")

        documents.append(row_to_document(row))

        if len(documents) >= MAX_BATCH_SIZE:
            upload_batch(documents)
            documents = []

upload_batch(documents)
