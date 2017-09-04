import sys,os

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import util
import pickle

def convert_user_song_eval_file(dump_file,user_hidden_songs_file, songs_file='/media/shahin/data/msd_recom/data/kaggle_files/kaggle_songs.txt'):
    with open(songs_file, "r") as f:
        its = dict(map(lambda line: list((line.strip().split(' ')[1],line.strip().split(' ')[0])), f.readlines()))
    f.close()

    print 'kaggle songs file read'

    hid = user_hidden_songs_file
    user_index = -1
    user_hidden = []

    noLoop = util.file_len(hid)
    f = open(hid)
    user_list = []
    counter = 0

    for line in f:
        counter += 1
        user, song, _, = line.split("\t")
        if user not in user_list:
            user_list.append(user)
            user_hidden.append([])
            user_index += 1

        user_hidden[user_index].append(song)

        if counter % 5000:
            util.show_reading_progress(counter, noLoop)

    print 'groundtruth file read'

    file_to_save = open(dump_file,'w')
    pickle.dump(user_hidden, file_to_save)
    file_to_save.close()


# file_name='year1_test_triplets_hidden.txt'
file_name='year1_test_triplets_visible.txt'
# file_name="year1_valid_triplets_hidden.txt"
# file_name='year1_valid_triplets_visible.txt'
dump_file= file_name.split('.')[0]+'.dmp'
user_hidden_songs_file= '/media/shahin/data/msd_recom/submissions/test_true_values/EvalDataYear1MSDWebsite/' + file_name

convert_user_song_eval_file(dump_file,user_hidden_songs_file)