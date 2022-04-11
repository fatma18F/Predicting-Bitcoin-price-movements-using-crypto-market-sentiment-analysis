import time
import numpy as np
from datetime import date, timedelta, datetime
import itertools
import threading, queue
import string
import re
import json

# Make sure to install the latest version of snscrape (v0.4.3.20220106)
import snscrape.modules.twitter as twitter

# Twitter search format: <from:username since:yyyy-mm-dd until:yyyy-mm-dd>
# Source: https://www.teachthought.com/technology/how-to-search-for-tweets-by-date/

RELEVANT_TWEET_FIELDS = ["date", "content", "lang", "replyCount", "retweetCount",
    "likeCount", "quoteCount"]
RELEVANT_USER_FIELDS = ["verified", "followersCount", "friendsCount", "statusesCount"]
ALLOWED_CHARACTERS = string.printable # includes letters, digits, spaces and punctuation

def save_twees_to_file(tweets, filename):
    json_object = {"tweets": tweets}
    with open(filename, "w", encoding='utf-8') as file:
        json.dump(json_object, file, indent=4, default=str)

def load_tweets_from_file(filename):
    with open(filename, "r", encoding='utf-8') as file:
        json_object = json.load(file)
    return json_object["tweets"]

def thread_work(tasks_queue, results_queue, verbose):
    while True:
        query = tasks_queue.get()
        tweets = scrape_query(query, verbose)
        results_queue.put(tweets)
        tasks_queue.task_done()
        if tasks_queue.empty():
            break

def are_in_ascending_temporal_order(tweets):
    return all(tweets[i]["date"] <= tweets[i+1]["date"]
        for i in range(len(tweets) - 1))

def scrape_all(search_texts, from_date, to_date, days_per_query=1,
        verbose=False, n_threads=1):
    """
        set days_per_query = -1 to parse the entire range (to_date - from_date) in one 
            single query. BUT: This makes it impossible to split the parsing task and
            thus only 1 thread will be used

        set n_threads = -1 to have n_threads = n_tasks

        from_date and to_date are both included in the range:
            e.g. 2 days between from_date = 2020.01.01 and to_date 2020.01.02
    """
    start_time = time.time()

    from_date = date.fromisoformat(from_date)
    to_date = date.fromisoformat(to_date)
    total_days = (to_date - from_date).days + 1
    days_per_query = total_days if days_per_query == -1 else days_per_query
    num_tasks = len(search_texts) * (total_days // days_per_query)
    n_threads = num_tasks if n_threads == -1 else n_threads     

    # Start the worker threads
    thread_results = queue.Queue()
    thread_tasks = queue.Queue()
    [threading.Thread(target=thread_work, args=(thread_tasks, thread_results,
        verbose)).start() for i in range(n_threads)]

    # Popoulate the tasks queue
    for search_text in search_texts:
        for i in range(total_days // days_per_query):
            current_date = from_date + timedelta(i * days_per_query)
            search_query = search_text + (
                f" since:{current_date} until:{current_date + timedelta(days_per_query)}" \
                if from_date != None else "")
            thread_tasks.put(search_query)

    # Wait for the threads to complete all the tasks
    thread_tasks.join()
    tota_parsing_time = time.time() - start_time

    # Concatenate the results from the different queries
    tweets = []
    while not thread_results.empty():
        tweets.append(thread_results.get())
    tweets = list(itertools.chain.from_iterable(tweets))

    if verbose:
        print("-"*100)
        print(f"The {n_threads} threads combined parsed {len(tweets)} tweets" \
            + f"in {tota_parsing_time}s.")
        print(f"A parsing rate of {len(tweets) / tota_parsing_time} tweets per second.")
        print("-"*100)

    # Sort the tweets in ascending temporal order
    tweets = sorted(tweets, key=lambda twt: twt["date"])
    assert are_in_ascending_temporal_order(tweets)

    # Clean the tweets
    # This is different from the task of the tokenizer
    # The goal is to remove duplicate tweets/ spam etc.
    clean_tweets(tweets)

    return tweets

def scrape_query(search_query, verbose=False):
    tweets = []
    start_time = time.time()
    last_time = start_time
    if verbose:
        print(f"Parsing query: '{search_query}'")
    for i, tweet in enumerate(twitter.TwitterSearchScraper(search_query).get_items()):
        tweet_dict = tweet.__dict__
        if is_relevant(tweet_dict):
            # Notice here that we don't clean/ tokenize the individual tweets
            # This is performed at a later stage (not in this script) by the bertweet tokenizer
            trim_tweet_fields(tweet_dict) # remove irrelevant fields
            tweets.append(tweet_dict)
    if (verbose):
        print(f"Parsed '{search_query}' for {len(tweets)} tweets in {time.time() - start_time}s.")
    return tweets

def trim_tweet_fields(tweet):
    """
        removes the irrelevant fields from the tweet dict
        this is done in place (no seperate dict is created) to save up memory
    """
    tweet_keys = list(tweet.keys())
    for key in tweet_keys:
        if key != "user" and key not in RELEVANT_TWEET_FIELDS:
            tweet.pop(key, None)

    for key in RELEVANT_USER_FIELDS:
        tweet[key] = tweet["user"].__dict__[key]

    tweet.pop("user", None)

def is_relevant(tweet):
    """
        TODO: Remove spam tweets
        TODO: Are there bots on twitter that we need to "block" ?
        TODO: Remove tweets that have a subjectivity level < threshhold
    """
    # Remove tweets that are not in english
    relevant = tweet["lang"] == "en" and \
        all([ # Remove tweets with absent (None) field values
            tweet[field] != None for field in RELEVANT_TWEET_FIELDS
        ]) \
        and all([ # Remove tweets with absent (None) user field values
            tweet["user"].__dict__[field] != None for field in RELEVANT_USER_FIELDS
        ]) \
        and all([ # Remove tweets that contain foreign characters
            c in ALLOWED_CHARACTERS for c in tweet["content"]
        ])
    # Remove tweets that contain media (e.g. images)
    relevant = relevant and tweet["media"] == None
    return relevant

def clean_tweets(tweets):
    """
        TODO: Remove spam tweets
        TODO: Leave a maximum of 1 tweet per user per time window etc. ?
        TODO: Are there bots on twitter that we need to "block" ?
        TODO: Do some exploratory data analysis (wordcloud visualization etc.)
        TODO: Remove perfect duplicate tweets (by tweet.id)
    """
    return

def explore_tweets(tweets, num_tweets):
    print("#"*100)
    for i, tweet in enumerate(tweets):
        if i == num_tweets:
            break
        print("Date: ", tweet["date"])
        print("Content: ", tweet["content"])
        print("-"*50)
    print("#"*100)

def main():
    tweets = scrape_all(["#Bitcoin"], "2021-12-01", "2022-02-28", 
        days_per_query=1, verbose=True, n_threads=-1)
    filename = "tweets"
    save_twees_to_file(tweets, filename)
    tweets = load_tweets_from_file(filename)
    explore_tweets(tweets, 10)
    explore_tweets(tweets[::-1], 10)
    print(tweets[0].keys())

if __name__ == "__main__":
    main()