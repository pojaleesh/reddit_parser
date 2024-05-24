current_date=$(date +'%Y-%m-%d-%H-%M')
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

python3 "$parent_path/reddit_parser/main.py" -c "$parent_path/config.conf" -d "$current_date"

find "$parent_path/data" -name "*$current_date*" | xargs -I {} python3 $parent_path/sentiment_predictor/sentiment_predictor.py -f={}

mkdir -p $parent_path/data/$current_date
mv $parent_path/data/*predicted.csv $parent_path/data/$current_date
rm $parent_path/data/*.csv