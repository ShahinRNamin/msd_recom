'''
Utilities that are probably required for every single model for the MSD last.fm dataset
'''

import subprocess
import pickle
import sys , getopt


def file_len(fname):
    '''
    count number of lines in file
    '''
    p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE)
    result, err = p.communicate()
    if p.returncode != 0:
        raise IOError(err)
    return int(result.strip().split()[0])

def show_reading_progress(currentLineNo,noLines,bar_len=60 ,suffix=''):
    filled_len = int(round(bar_len * currentLineNo/ float(noLines)))
    percents = round(100.0 * currentLineNo / float(noLines), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()


def list_users_listened_songs(user_song_file):
    '''
    return a dictionary of currently listened songs for users
    '''
    try:

        f=open(user_song_file)
        user_to_songs = dict() #keeping the songs user has already listened
        counter=0
        for line in f:
            counter +=1
            user,song,_ = line.strip().split('\t')

            if user in user_to_songs:
                user_to_songs[user].add(song)
            else:
                user_to_songs[user] = set([song])

        f.close()

    except IOError as e:
        print "util.list_users_listened_songs: I/O error({0}): {1}".format(e.errno, e.strerror)
        exit()
    except:  # handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]
        exit()
    return user_to_songs


def list_songs(songs_file):
    '''
    read and generate the songId -> SongNO dictionary
    '''
    try:
        f=open(songs_file,'r')
        song_to_index = dict(map(lambda line: line.strip().split(' ') , f.readlines() ))
        f.close()
        return song_to_index
    except IOError as e:
        print "util.list_songs: I/O error({0}): {1}".format(e.errno, e.strerror)
        exit()
    except:  # handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]
        exit()


def list_users(user_file):
    '''
    read userIds. This has the same ordering as the one required for submission
    '''
    try:
        f=open(user_file, 'r')
        users = map(lambda line: line.strip(), f.readlines())
        f.close()
        return users
    except IOError as e:
        print "util.list_users: I/O error({0}): {1}".format(e.errno, e.strerror)
        exit()
    except:  # handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]
        exit()



def make_submission_file(submission_file,user_recom_songs,songs_ids,users_orders,user_listened_songs):
    '''
    user_recom_songs: the dictionary or recommender songs to users.
    song_ids: the dictionary of songId to songIds for submission, returned by calling list_songs
    users_orders: the list of users ordered as required by submission, returned by calling list_users
    listened_songs: dictionary of users -> currently listened songs. returned by calling list_users_listened_songs
    '''
    try:
        f=open(submission_file,'w')
        no_users = len(users_orders)
        counter=0
        for user in users_orders:
            counter+=1
            songs_to_recommend= []
            for songs_of_user in user_recom_songs[user]:
                for song in songs_of_user:
                    if len(songs_to_recommend) >=500: #recommend 500 songs for each user
                        break
                    if not song in user_listened_songs[user]:
                        songs_to_recommend.append(song)
            #change song Ids to songNo
            indices = map(lambda s: songs_ids[s],songs_to_recommend)
            f.write(' '.join(indices) + '\n')
            if counter % 5000 == 0:
                show_reading_progress(counter , no_users)
        f.close()
    except IOError as e:
        print "util.make_submission_file: I/O error({0}): {1}".format(e.errno, e.strerror)
        exit()
    except:  # handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]
        exit()
