import argparse
import json

import praw

from setup import logger
from reddit_parser import RedditParser


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_path', required=True)
    parser.add_argument('-d', '--date', required=True)
    return parser.parse_args()


def validate_config(config):
    common_check_fields = ['client_id', 'client_secret', 'redirect_uri', 'user_agent', 'subreddits']
    for field in common_check_fields:
        if field not in config:
            logger.info('Config common check failed')
            raise RuntimeError(f'Invalid config: {config}')

    subreddit_check_fields = ['subreddit_name', 'threads_csv', 'comments_csv', 'type', "limit"]
    for subreddit_config in config['subreddits']:
        if not isinstance(subreddit_config, dict):
            logger.info('Config subreddit check failed')
            raise RuntimeError(f'Invalid config: {config}')

        for field in subreddit_check_fields:
            if field not in subreddit_config:
                logger.info('Config subreddit check failed')
                raise RuntimeError(f'Invalid config: {config}')


def parse_config(args):
    with open(args.config_path, 'r', encoding='utf-8') as file:
        return json.load(file)


def create_reddit_client(config):
    reddit = praw.Reddit(
        client_id=config['client_id'],
        client_secret=config['client_secret'],
        redirect_uri=config['redirect_uri'],
        user_agent=config['user_agent'])
    return reddit


def main():
    logger.info('Reddit parser started')

    # Init config
    args = parse_args()
    config = parse_config(args)
    validate_config(config)

    logger.info('Succesfully parse arguments')

    # Init reddit client
    reddit_client = create_reddit_client(config)
    for subreddit in config['subreddits']:
        parser = RedditParser(
            reddit_client,
            subreddit_name=subreddit['subreddit_name'],
            threads_csv=subreddit['threads_csv'],
            comments_csv=subreddit['comments_csv'],
            type=subreddit['type'],
            limit=int(subreddit['limit']),
            date=args.date)

        parser.parse()


if __name__ == '__main__':
    main()
