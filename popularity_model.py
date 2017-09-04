'''
#The easiest and most naive and dumb model possible.
#Recommending the most popular songs to all users.
#No personalization is considered
'''

import util
import pickle
import argparse
import sys

def compute_popularity(user_song_file,count_no_listens=False,dump_results_to_disk=True):
    no_lines=util.file_len(user_song_file)
    try:

        f=open(user_song_file)
        song_to_count=dict() #keeping sorted popularity
        user_to_songs = dict() #keeping the songs user has already listened
        counter=0
        for line in f:
            counter +=1
            user,song,pop_str = line.strip().split('\t')
            popularity_increm=int(1) #the value to add to popularity for the current user/song listen.
            if count_no_listens is True:
                popularity_increm= int(pop_str)
            if song in song_to_count:
                song_to_count[song] +=popularity_increm
            else:
                song_to_count[song] =popularity_increm

            if user in user_to_songs:
                user_to_songs[user].add(song)
            else:
                user_to_songs[user] = set([song])

            if counter % 5000 == 0:
                util.show_reading_progress(counter , no_lines)
        f.close()

        songs_ordered=sorted(song_to_count.keys(),key=lambda s: song_to_count[s] , reverse=True)

        if dump_results_to_disk is True:
            file_to_save= open('popularity_dump_noListensConsidered_%r.dmp' % (count_no_listens),'w')
            pickle.dump(song_to_count,file_to_save)
            file_to_save.close()
    except IOError as e:
        print "compute_popularity: I/O error({0}): {1}".format(e.errno, e.strerror)
        exit()
    except:  # handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]
        exit()

    return (songs_ordered , user_to_songs)

def make_submission_file(submission_file,songs_ordered , user_to_songs, users, song_to_index ):
    '''
    Make the submission file
    '''
    try:
        f=open(submission_file,'w')
        no_users = len(users)
        counter=0
        for user in users:
            counter+=1
            songs_to_recommend= []
            for song in songs_ordered:
                if len(songs_to_recommend) >=500: #recommend 500 songs for each user
                    break
                if not song in user_to_songs[user]:
                    songs_to_recommend.append(song)
            #change song Ids to songNo
            indices = map(lambda s: song_to_index[s],songs_to_recommend)
            f.write(' '.join(indices) + '\n')

            if counter % 5000 == 0:
                util.show_reading_progress(counter , no_users)

        f.close()
    except IOError as e:
        print "make_submission_file: I/O error({0}): {1}".format(e.errno, e.strerror)
        exit()
    except:  # handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]
        exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--songs", help="kaggle songs file" , default='/media/shahin/data/msd_recom/data/kaggle_files/kaggle_songs.txt')
    parser.add_argument("--users" , help="validation/test users file",default='/media/shahin/data/msd_recom/data/kaggle_files/kaggle_users.txt')
    parser.add_argument("--ratings" , help="user/song/noListens file",default='/media/shahin/data/msd_recom/data/kaggle_files/kaggle_visible_evaluation_triplets.txt')
    parser.add_argument("--submission" , help="submission file to save" , default = "submission.txt")
    parser.add_argument("--counts" , help="use number of listens in popularity model (Y/N)" , default="N")
    args = parser.parse_args()

    songs_file = args.songs
    users_file = args.users
    user_songs_file = args.ratings
    submission_file = args.submission
    use_count_str = args.counts.lower()
    use_NoListens=False
    if use_count_str == 'y':
        use_NoListens=True
    elif use_count_str != 'n':
        print("--counts should be either N or Y")
        exit()

    songs_order=util.list_songs(songs_file)
    users_order=util.list_users(users_file)
    # user_item_listens = util.list_users_listened_songs()
    (popularity_songs,users_listened_songs) = compute_popularity(user_songs_file,count_no_listens=use_NoListens,dump_results_to_disk=True)

    make_submission_file(submission_file,popularity_songs , users_listened_songs, users_order, songs_order )