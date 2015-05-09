from datetime import datetime
from pprint import pprint

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship, backref
from sqlalchemy import ForeignKey
from sqlalchemy import Sequence
from sqlalchemy import func, desc

engine = create_engine('sqlite:///twitter.db')
Base = declarative_base()
Session = sessionmaker(bind=engine)

class Tweet(Base):
  __tablename__ = 'tweets'

  id = Column(Integer, Sequence('tweet_id_sequence'), primary_key=True)
  text = Column(String)
  sentiment = Column(String)
  date = Column(DateTime, default=datetime.utcnow)

class Hash(Base):
  __tablename__ = 'hash'

  id = Column(Integer, Sequence('hash_id_sequence'), primary_key=True)
  tag = Column(String)
  tweet_id = Column(Integer, ForeignKey('tweets.id'))
  tweet = relationship("Tweet", backref=backref('hashes', order_by=id))

def chart_top10():
  session = Session()
  top10 = session.query(Hash.tag, func.count(Hash.id)).group_by(Hash.tag)\
    .order_by(desc(func.count(Hash.id))).limit(10).subquery()
  data = {}

  for tag, sent, count in session.query(Hash.tag, Tweet.sentiment,
      func.count(Hash.id))\
      .filter(Tweet.id==Hash.tweet_id)\
      .join(top10, Hash.tag == top10.c.tag)\
      .group_by(Tweet.sentiment, Hash.tag)\
      .order_by(desc(func.count(Hash.id))):
    if tag not in data:
      data[tag] = {}
    data[tag][sent] = str(count)
  pprint(data)

  keys = data.keys()
  with open('sentiment.data', 'w') as out:
    out.write(' '.join(['Sentiment'] + keys) + '\n')
    out.write(' '.join(['pos'] + map(lambda k: data[k]['pos'], keys)) + '\n')
    out.write(' '.join(['neg'] + map(lambda k: data[k]['neg'], keys)) + '\n')

def time_based():
  session = Session()
  top = session.query(Hash.tag, func.count(Hash.id)).group_by(Hash.tag)\
    .order_by(desc(func.count(Hash.id))).limit(1).subquery()
  data = {}

  for tag, sent, count, date in session.query(Hash.tag, Tweet.sentiment,
            func.count(Hash.id), Tweet.date)\
            .filter(Tweet.id==Hash.tweet_id)\
            .join(top, Hash.tag == top.c.tag)\
            .group_by(Tweet.sentiment, Hash.tag, Tweet.date)\
            .order_by(desc(func.count(Hash.id))):
    month, day, hour, minute = date.month, date.day, date.hour, date.minute
    time = str((minute + 60 * hour + 24 * 60 * day) / 10)
    print tag, sent, count, time
    if time not in data:
      data[time] = {'pos': 0, 'neg': 0}
    data[time][sent] += count

  keys = sorted(data.keys())
  with open('time.data', 'w') as out:
    out.write(' '.join(['Sentiment'] + keys) + '\n')
    out.write(' '.join(['pos'] + map(lambda k: str(data[k]['pos']), keys)) + '\n')
    out.write(' '.join(['neg'] + map(lambda k: str(data[k]['neg']), keys)) + '\n')

def ratio():
  session = Session()
  top20 = session.query(Hash.tag, func.count(Hash.id)).group_by(Hash.tag)\
    .order_by(desc(func.count(Hash.id))).limit(20).subquery()
  data = {}

  for tag, sent, count in session.query(Hash.tag, Tweet.sentiment, func.count(Hash.id))\
            .filter(Tweet.id==Hash.tweet_id)\
            .join(top20, Hash.tag == top20.c.tag)\
            .group_by(Tweet.sentiment, Hash.tag)\
            .order_by(desc(func.count(Hash.id))):
    if tag not in data:
      data[tag] = {}
    data[tag][sent] = float(count)
  pprint(data)

  keys = data.keys()
  with open('ratio.data', 'w') as out:
    out.write(' '.join(['Sentiment'] + keys) + '\n')
    out.write(' '.join(['pos'] + map(lambda k: str(2*(-0.5 + data[k]['pos'] /\
      (data[k]['pos'] + data[k]['neg']))), keys)) + '\n')


def main():
  chart_top10()
  time_based()
  ratio()

if __name__ == '__main__':
  main()
