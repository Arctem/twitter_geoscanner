tweets_per_second = 8
seconds_per_day = 60 * 60 * 24
num_days = 20

time = 3
lat = 4
lon = 4
hashtag_id_size = 3
num_hashtags_per_tweet = 10

avg_hashtag_length = 25
avg_unique_hashtags_per_tweet = 0.5

def getTweetSizeInBytes():
    return time + lat + lon + (hashtag_id_size * num_hashtags_per_tweet)

def getTotalTweetCount():
    return tweets_per_second * seconds_per_day * num_days

def getTotalTweetSize():
    return getTotalTweetCount() * getTweetSizeInBytes()

def getHashtagMapSize():
    return avg_unique_hashtags_per_tweet * (avg_hashtag_length + 1) * getTotalTweetCount()

def getTotalSize():
    return getTotalTweetSize() + getHashtagMapSize()

# get the total tweet size in megabytes
def main():
    print getTotalSize() / 1024.0 / 1024.0

if __name__ == "__main__":
    main()
