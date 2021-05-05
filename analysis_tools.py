import requests


def calculate_sentiment(data, datatype):
    polar_count = 0
    positive_count = 0
    for dp in data:
        if dp["type"] == datatype:
            sentiment = dp.get("sentiment", 0.5)
            if sentiment > 0.5:
                polar_count += 1
                positive_count += 1
            elif sentiment < 0.5:
                polar_count += 1
    if polar_count != 0:
        return positive_count / polar_count
    else:
        return 0.5


def calculate_mentions(data, datatype):
    count = 0
    for dp in data:
        if dp["type"] == datatype:
            count += 1
    return count

