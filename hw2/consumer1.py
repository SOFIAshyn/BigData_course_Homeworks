from json import loads
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from multiprocessing import Process
import time

consumer = KafkaConsumer('Twitter_Messages_Topic', group_id='my_group', bootstrap_servers=['127.0.0.1:9092'])

def get_list_of_all_accounts(lst_all_messages_data):
    return [account_dict['account'] for account_dict in lst_all_messages_data]

def get_last_10_tweets(lst_all_messages_data):
    time_frame_start = datetime.now() - timedelta(hours=3)
    time_frame_end = datetime.now()
    # lst_all_messages_data = [datetime.strptime(account_dict['date']) for account_dict in lst_all_messages_data]
    posts_lst_in_3_hours = []
    for account_dict in lst_all_messages_data:
        date = datetime.strptime(account_dict['date'])
        if time_frame_start < date < time_frame_end:
            posts_lst_in_3_hours.append(account_dict['account'])
    dict(Counter(posts_lst_in_3_hours))


def prepare_report(lst_all_messages_data):
    return {
        'authors': get_list_of_all_accounts(lst_all_messages_data),
        'last_10_tweets_for_each_of_the_10_accounts_which_posted_the_highest_number_of_tweets_in_the_last_3_hours': \
            get_last_10_tweets(lst_all_messages_data)
    }

def consume():
    def add_tweet_data(dict_of_collected_data):
        dict_of_collected_data['tweets'] = {}
        dict_of_collected_data['tweets']['id'] = message[2]
        dict_of_collected_data['tweets']['date'] = message[0]
        dict_of_collected_data['tweets']['text'] = message[6]
        hashtags = []
        for word in message[6].split():
            if word.startswith('#'):
                hashtags.append(word.replace('#', ''))
        dict_of_collected_data['tweets']['hashtags'] = hashtags

    for message in consumer:
        dict_of_collected_data = {}

        message = (message.value).decode('utf-8').split(',')
        account_name = message[5]
        if account_name not in set(get_list_of_all_accounts(lst_all_messages_data)):
            dict_of_collected_data['account'] = account_name
            dict_of_collected_data['counts'] = 1
            add_tweet_data(dict_of_collected_data)
        else:
            for dict_of_collected_data in lst_all_messages_data:
                if lst_all_messages_data['account'] == account_name:
                    dict_of_collected_data['tweets']['counts'] += 1
                    add_tweet_data(dict_of_collected_data)
        lst_all_messages_data.append(dict_of_collected_data)


def create_report(lst_all_messages_data):
    while True:
        time.sleep(3600)
        prepare_report(lst_all_messages_data)


if __name__ == '__main__':
    print('Consumer:')

    lst_all_messages_data = []

    proc1 = Process(target=consume, args=lst_all_messages_data)
    proc2 = Process(target=create_report, args=lst_all_messages_data)
    proc1.start()
    proc2.start()



