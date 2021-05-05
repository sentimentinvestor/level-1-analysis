from firebase_db import db
import time
from firebase_admin import firestore
from analysis_tools import calculate_mentions, calculate_sentiment
from flask import jsonify
import base64
import yfinance as yf


# triggered by a pubsub message to the 'level_1_analysis' topic
# uploads the number of mentions and the sentiment for a particular ticker to
# that ticker's document
def level_1_analysis(event, context):
    start_time = time.time()

    ticker = base64.b64decode(event['data']).decode('utf-8')
    raw_data = [doc.to_dict() for doc in get_raw_data(ticker)]
    datatypes = ["reddit_comment", "reddit_post", "tweet", "stocktwits_post", "yahoo_finance_comment"]
    updated_fields = {}

    for dt in datatypes:
        sentiment = calculate_sentiment(raw_data, dt)
        mentions = calculate_mentions(raw_data, dt)
        updated_fields[dt + "_sentiment"] = sentiment
        updated_fields[dt + "_sentiment_timestamp"] = time.time()
        updated_fields[dt + "_mentions"] = mentions
        updated_fields[dt + "_mentions_timestamp"] = time.time()

        # update histories
        db().collection('tickers').document(ticker).collection('history').document(dt + '_mentions').set({
            "history": firestore.ArrayUnion([{
                "timestamp": time.time(),
                "data": mentions
            }])
        }, merge=True)
        db().collection('tickers').document(ticker).collection('history').document(dt + '_sentiment').set({
            "history": firestore.ArrayUnion([{
                "timestamp": time.time(),
                "data": sentiment
            }])
        }, merge=True)

    # finally we add the yahoo finance data
    yahoo_finance_fields = ["quoteType", "bid", "previousClose", "marketCap", "industry", "sector", "logo_url"]
    info = yf.Ticker(ticker).info
    for field in yahoo_finance_fields:
        updated_fields[field] = info.get(field, "na")

    # update the main ticker document
    db().collection('tickers').document(ticker).set(updated_fields, merge=True)

    return jsonify({
        "success": True,
        "time_taken": time.time() - start_time
    })


# gets raw data from that last hour containing the ticker keyword
def get_raw_data(ticker):
    HOUR = 3600
    max_age = time.time() - HOUR
    return db().collection('raw_data')\
        .where('timestamp', '>', max_age)\
        .where('tickers', 'array_contains', ticker)\
        .get()
