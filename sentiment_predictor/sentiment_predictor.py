import argparse
import pandas as pd

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from setup import logger


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file_path', required=True)
    return parser.parse_args()


def predict_sentiment(df, sid):
    pos = []
    neg = []
    neu = []
    compound = []

    for i in range(0, len(df)):
        text = ""
        if isinstance(df.iloc[i].text, str):
            text = df.iloc[i].text
        else:
            logger.warning(f'Undefined text value {df.iloc[i].text}')

        predict = sid.polarity_scores(text)
        neg.append(predict['neg'])
        pos.append(predict['pos'])
        neu.append(predict['neu'])
        compound.append(predict['compound'])

    predict_df = pd.DataFrame(
        {
            'pos': pos,
            'neg': neg,
            'neu': neu,
            'compound': compound
        }
    )

    result = pd.concat([df, predict_df], axis=1)
    return result


def main():
    logger.info('Sentiment predictor started')

    # Init config
    args = parse_args()

    df = pd.read_csv(args.file_path, sep='|')
    sid = SentimentIntensityAnalyzer()
    result = predict_sentiment(df, sid)

    result.to_csv(args.file_path.replace('.', '_predicted.'))

    logger.info('Sentiment predictor finished')


if __name__ == '__main__':
    main()