from datetime import datetime, timezone
from typing import Dict, List, Set, IO, Any

import csv
import time
import praw
import praw.models
import prawcore

from setup import logger
from utils import LimitHour


class RedditParser:

    """Represents single subreddit parser"""

    def __init__(self, reddit_client, **kws):
        FILE_SUFFIX: Dict = {
            LimitHour.H1 : "_1_hour.csv",
            LimitHour.H2 : "_2_hour.csv",
            LimitHour.H4 : "_4_hour.csv",
            LimitHour.H6 : "_6_hour.csv",
            LimitHour.H12 : "_12_hour.csv",
            LimitHour.H168 : "_7_day.csv"
        }

        self._now: float = datetime.now(timezone.utc).timestamp()
        self._date = kws.get('date')

        self._client: praw.Reddit  = reddit_client
        self._subreddit_name: str = kws.get('subreddit_name')
        self._type: str  = kws.get('type')
        self._limit: int = kws.get('limit')

        self._threads_csv: Dict[str, IO] = {}
        self._threads_csv_writer: Dict[str, Any] = {}

        for limit_hour in LimitHour:
            self._threads_csv[limit_hour] = open(
                kws.get('threads_csv') + "_" + self._date + FILE_SUFFIX[limit_hour], 'w', encoding='utf8')
            self._threads_csv_writer[limit_hour] = csv.writer(
                self._threads_csv[limit_hour], delimiter='|')
            self._threads_csv_writer[limit_hour].writerow([
                'id', 'text', 'title', 'score',
                'num_comments', 'ups', 'downs',
                'upvote_ratio', 'created_utc'])

        self._processed_thread_ids: List[str] = []
        self._valid_thread_ids: Set[str]  = set()

        self._prepared_comments: Dict[str, List[Any]] = {}

        self._comments_csv: Dict[str, IO] = {}
        self._comments_csv_writer: Dict[str, Any] = {}

        for limit_hour in LimitHour:
            self._comments_csv[limit_hour] = open(
                kws.get('comments_csv') + "_" + self._date + FILE_SUFFIX[limit_hour], 'w', encoding='utf8')
            self._comments_csv_writer[limit_hour] = csv.writer(
                self._comments_csv[limit_hour], delimiter='|')
            self._comments_csv_writer[limit_hour].writerow([
                'id', 'parent_thread_id', 'text',
                'score', 'ups', 'downs', 'created_utc'])

    def parse(self) -> None:
        self._parse_subreddit()
        self._parse_comments()

    def _parse_subreddit(self) -> None:
        if self._type == 'hot':
            subreddit = self._client.subreddit(self._subreddit_name).hot(limit=self._limit)
        else:
            subreddit = self._client.subreddit(self._subreddit_name).new(limit=self._limit)

        iterator = iter(subreddit)

        try:
            while True:
                try:
                    thread = next(iterator)
                except prawcore.exceptions.ResponseException as e:
                    logger.info('Got response exception %s', e)
                    time.sleep(65)
                    continue

                self._process_thread(thread)

                if self.__processed_thread_count() % 90 == 0:
                    logger.info(f'Processed {self.__processed_thread_count()} threads')
                    time.sleep(60)

        except StopIteration:
            logger.info('Finish threads parsing')
            return

        except Exception as e:
            logger.info('Got unexpected exception %s', e)
            return

    def _parse_comments(self) -> None:
        queue = []
        for _, comments in self._prepared_comments.items():
            for comment in comments:
                if not isinstance(comment, praw.models.MoreComments):
                    queue.append(comment)

        processed_ids = set()

        while True:
            if len(queue) == 0:
                break

            comment = queue.pop(0)

            if comment.id in processed_ids:
                continue

            processed_ids.add(comment.id)

            # logger.info(len(queue))
            if isinstance(comment, praw.models.MoreComments):
                continue

            new_row = [
                comment.id,
                comment.submission.id,
                comment.body.replace('\n', ' ').replace('\t', ' ').replace('|', ' '),
                comment.score,
                comment.ups,
                comment.downs,
                comment.created_utc]

            for limit_hour in LimitHour:
                # Skip thread older than a limit
                if not self.__is_valid_limit_hour(comment, limit_hour):
                    continue

                # if thread.id not in self._prepared_comments:

                self._comments_csv_writer[limit_hour].writerow(new_row)

            queue.extend(comment.replies)

        logger.info('Finish comments parsing')


    def _process_thread(self, thread) -> None:
        while True:
            try:
                thread.comments.comment_sort = 'hot'
                thread.comments.comment_limit = 100
                self._prepared_comments[thread.id] = thread.comments.list()
                break
            except Exception as e:
                logger.info("Handling replace_more exception %s", e)
                time.sleep(float(str(e).split()[8]) + 0.5)

        self._processed_thread_ids.append(thread.id)

        new_row = [
            thread.id,
            thread.selftext.replace('\n', ' ').replace('\t', ' ').replace('|', ' '),
            thread.title.replace('\n', ' ').replace('\t', ' ').replace('|', ' '),
            thread.score,
            thread.num_comments,
            thread.ups,
            thread.downs,
            thread.upvote_ratio,
            thread.created_utc]

        for limit_hour in LimitHour:
            # Skip thread older than a limit
            if not self.__is_valid_limit_hour(thread, limit_hour):
                continue

            # if thread.id not in self._prepared_comments:

            self._valid_thread_ids.add(thread.id)
            self._threads_csv_writer[limit_hour].writerow(new_row)

    def __processed_thread_count(self) -> int:
        return len(self._processed_thread_ids)

    def __is_valid_limit_hour(self, object, limit_hour) -> bool:
        LIMIT_HOUR_SECONDS = {
            LimitHour.H1 : 60 * 60,
            LimitHour.H2 : 2 * 60 * 60,
            LimitHour.H4 : 4 * 60 * 60,
            LimitHour.H6 : 6 * 60 * 60,
            LimitHour.H12 : 12 * 60 * 60,
            LimitHour.H168 : 7 * 24 * 60 * 60}
        return self._now - object.created_utc <= LIMIT_HOUR_SECONDS[limit_hour]
