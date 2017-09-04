'''
#Evaluation functions for the recommendation. The methods to be written are 
'''
import util
import pickle


def eval_map_test2(ground_truth_dmp_file, prediction_txt_file, k_param=10, songs_file='/media/shahin/data/msd_recom/data/kaggle_files/kaggle_songs.txt'):
    user_songs = []

    with open(songs_file, "r") as f:
        its = dict(map(lambda line: list((line.strip().split(' ')[1],line.strip().split(' ')[0])), f.readlines()))
    f.close()

    print 'kaggle songs file read'


    rec = prediction_txt_file
    with open(rec) as f:
        for line in f:
            song_set = line.strip().split(" ")
            for i in range(len(song_set)):
                song_set[i] = its[song_set[i]]
            user_songs.append(song_set)

    print 'prediction file read'

    user_hidden = pickle.load(open(ground_truth_dmp_file, "r"))

    print 'groundtruth file read'

    # Calculate map values
    sum = 0.0
    noLoop = len(user_hidden)
    counter = 0

    for i in range(0, len(user_hidden)): #for each user
        counter += 1
        ap_current=0
        ap_all=0
        # value = 0.0
        no_hidden_songs=len(user_hidden[i])
        k_user = min(k_param , no_hidden_songs)
        for k in range(0,k_user):
            if user_songs[i][k] in user_hidden[i]:
                ap_current+=1
                ap_all+=float(ap_current)/float(k+1)

        ap_all /= float(k_user)

        sum += ap_all

        if counter % 5000 == 0:
            util.show_reading_progress(counter, noLoop)

    sum /= len(user_hidden)
    return sum

def eval_map_test(ground_truth_dmp_file, prediction_txt_file, k_param=10, songs_file='/media/shahin/data/msd_recom/data/kaggle_files/kaggle_songs.txt'):
    user_songs = []

    with open(songs_file, "r") as f:
        its = dict(map(lambda line: list((line.strip().split(' ')[1],line.strip().split(' ')[0])), f.readlines()))
    f.close()

    print 'kaggle songs file read'


    rec = prediction_txt_file
    with open(rec) as f:
        for line in f:
            song_set = line.strip().split(" ")
            for i in range(len(song_set)):
                song_set[i] = its[song_set[i]]
            user_songs.append(song_set)

    print 'prediction file read'

    user_hidden = pickle.load(open(ground_truth_dmp_file, "r"))

    print 'groundtruth file read'

    # Calculate map values
    sum = 0.0
    noLoop = len(user_hidden)
    counter = 0

    for i in range(0, len(user_hidden)): #for each user
        counter += 1
        ap_current=0
        ap_all=0
        # value = 0.0
        no_hidden_songs=len(user_hidden[i])
        for hidden_song_no in range(0, no_hidden_songs):
            hidden_song = user_hidden[i][hidden_song_no]
            if hidden_song in user_songs[i][: k_param]: #user_songs[i]: #[:no_hidden_songs+1]:
                ap_current+=1
                ap_all+=float(ap_current)/float(hidden_song_no+1)

            # for rec_song in range(0, len(user_songs[i])):
            #     if user_hidden[i][hidden_song] == user_songs[i][rec_song]:
            #         value += (hidden_song + 1.0) / (rec_song + 1.0)

        ap_all /= float(no_hidden_songs)

        sum += ap_all

        if counter % 5000 == 0:
            util.show_reading_progress(counter, noLoop)

    sum /= len(user_hidden)
    return sum


def eval_map(ground_truth_dmp_file, prediction_txt_file,songs_file='/media/shahin/data/msd_recom/data/kaggle_files/kaggle_songs.txt'):
    user_songs = []

    with open(songs_file, "r") as f:
        its = dict(map(lambda line: list((line.strip().split(' ')[1],line.strip().split(' ')[0])), f.readlines()))
    f.close()

    print 'kaggle songs file read'


    rec = prediction_txt_file
    with open(rec) as f:
        for line in f:
            song_set = line.strip().split(" ")
            for i in range(len(song_set)):
                song_set[i] = its[song_set[i]]
            user_songs.append(song_set)

    print 'prediction file read'

    user_hidden = pickle.load(open(ground_truth_dmp_file, "r"))

    print 'groundtruth file read'

    # Calculate map values
    sum = 0.0
    noLoop = len(user_hidden)
    counter = 0
    for i in range(0, len(user_hidden)):
        counter += 1
        value = 0.0
        for hidden_song in range(0, len(user_hidden[i])):
            for rec_song in range(0, len(user_songs[i])):
                if user_hidden[i][hidden_song] == user_songs[i][rec_song]:
                    value += (hidden_song + 1.0) / (rec_song + 1.0)

        value /= len(user_hidden[i])
        sum += value

        if counter % 5000 == 0:
            util.show_reading_progress(counter, noLoop)

    sum /= len(user_hidden)
    return sum





def eval_map_from_txt(prediction_results_file, user_hidden_songs_file='/media/shahin/data/msd_recom/submissions/test_true_values/EvalDataYear1MSDWebsite/year1_test_triplets_hidden.txt', songs_file='/media/shahin/data/msd_recom/data/kaggle_files/kaggle_songs.txt'):
    '''
    This is the most time consuming version of computing Map. It works directly with the text files. I suggest you use the versions that make use of the dumped read data
    :param prediction_results_file:
    :param user_hidden_songs_file:
    :param songs_file:
    :return:
    '''
    user_songs = []

    # build the dict to index songs
    # with open("kaggle_songs.txt", "r") as f:
    with open(songs_file, "r") as f:
        its = dict(map(lambda line: list((line.strip().split(' ')[1],line.strip().split(' ')[0])), f.readlines()))
    f.close()

    print 'kaggle songs file read'


    # Read rec songs for each user
    # rec = "MSD_result.txt"
    rec=prediction_results_file
    with open(rec) as f:
        for line in f:
            song_set = line.strip().split(" ")
            for i in range(len(song_set)):
                song_set[i] = its[song_set[i]]
            user_songs.append(song_set)

    print 'results file read'


    # Read hidden songs for each users from file
    # hid = "Result_hidden.txt"
    hid=user_hidden_songs_file
    user_index = -1
    user_hidden = []

    noLoop = util.file_len(hid)
    f = open(hid)
    user_list = []
    counter=0

    for line in f:
        counter+=1
        user, song,_, = line.split("\t")
        if user not in user_list:
            user_list.append(user)
            user_hidden.append([])
            user_index += 1

        user_hidden[user_index].append(song)

        if counter%5000:
            util.show_reading_progress(counter, noLoop)

    print 'groundtruth file read'

    # Calculate map values
    sum = 0.0
    noLoop=len(user_hidden)
    counter = 0
    for i in range(0, len(user_hidden)):
        counter+=1
        value = 0.0
        for hidden_song in range(0, len(user_hidden[i])):
            for rec_song in range(0, len(user_songs[i])):
                if user_hidden[i][hidden_song] == user_songs[i][rec_song]:
                    value +=  (hidden_song + 1.0) / (rec_song + 1.0)

        value /= len(user_hidden[i])
        sum += value

        if counter % 5000 == 0:
            util.show_reading_progress(counter, noLoop)

    sum /= len(user_hidden)
    return sum


dmp_file_name='year1_test_triplets_hidden.dmp' # 0.081881861477  # 0.0750432861624
#dmp_file_name='year1_test_triplets_visible.dmp' # 0.0787475763706 # 0.0730742494555
#dmp_file_name='year1_valid_triplets_hidden.dmp' # 0.078605335877   # 0.0707670354567
#dmp_file_name='year1_valid_triplets_visible.dmp' # 0.0782884366079 # 0.0722377345167

ground_truth_dmp_file = '/media/shahin/data/msd_recom/git/msd_recom/data_prep/'+ dmp_file_name
prediction_txt_file = '/media/shahin/data/msd_recom/submissions/popularity_by_user_count.txt' #'/media/shahin/data/msd_recom/submissions/popular_songs_by_user.txt'

print ('\n map value is: ' + str(eval_map_test2(ground_truth_dmp_file, prediction_txt_file,k_param=100)))


# print ('\n map value is: ' + str(eval_map_test(ground_truth_dmp_file, prediction_txt_file,k_param=27)))

# print ('\n map value is: ' + str(eval_map(ground_truth_dmp_file, prediction_txt_file)))
