'''
Preparing the mysql dbms dataset by converting the hdf5 files to the tables.
Although the database can be filled by going through the data only once, it is conceptually easier to prepare it by several iterations (though probably not the best case in terms of performance). But as this process will be executed only once, so it is not a big deal.

The steps to take is to:
1. fill terms
2. fill tags
3. fill artists
4. fill albums
5. fill songs
6. fill relationship tables:
    6.1. artist_terms
    6.2. artist_tags
    6.3. similar_artists

'''

import h5py #for reading the hdf5 files
import os
import sys
import time
import glob
import numpy as np
import _mysql
import MySQLdb as mdb
import math

DB_HOST = 'localhost' #192.168.1.123
DB_USER = 'millionsongs'
DB_PASSWORD = 'millionsongs'
DB_NAME = 'millionsongs'

#HDF5_BASE_DIR='/media/shahin/data/msd_recom/data/msd_subset_data/MillionSongSubset/data'
HDF5_BASE_DIR = '/media/shahin/1ad8a514-de7f-485a-ba08-8a0f921ee083/million_songs_dataset'




def get_all_files(basedir,ext='.h5'):
    """
    From a root directory, go through all subdirectories
    and find all files with the given extension.
    Return all absolute paths in a list.
    """
    allfiles = []
    apply_to_all_files(basedir,func=lambda x: allfiles.append(x),ext=ext)
    return allfiles


def apply_to_all_files(basedir,func=lambda x: x,ext='.h5'):
    """
    From a root directory, go through all subdirectories
    and find all files with the given extension.
    Apply the given function func
    If no function passed, does nothing and counts file
    Return number of files
    """
    cnt = 0
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root,'*'+ext))
        for f in files :
            func(f)
            cnt += 1
    return cnt





if __name__ == '__main__':
    try:
        conn = mdb.connect(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)

        '''Code to fill the terms'''

        print 'filling terms'


        def fill_terms(filename):
            # print filename
            f = h5py.File(filename, 'r')
            #read terms, lowercase them and strip (trim) them
            term_vals = map(lambda x: x.strip().lower() , list(f['metadata']['artist_terms']))
            f.close()
            #save terms
            for term in term_vals:
                if len(term) >0:
                    cursor = conn.cursor()
                    cursor.callproc('fill_term',[term])
                    cursor.close()
        # apply_to_all_files(HDF5_BASE_DIR,func=lambda x: fill_terms(x))
        #
        # conn.commit()

        '''code to fill the tags'''

        print 'filling tags'

        def fill_tags(filename):
            # print filename
            f = h5py.File(filename, 'r')
            tag_vals = map(lambda x: x.strip().lower(), list(f['musicbrainz']['artist_mbtags']))
            f.close()
            for tag in tag_vals:
                if len(tag)>0:
                    cursor = conn.cursor()
                    cursor.callproc('fill_tag', [tag])
                    cursor.close()

        # apply_to_all_files(HDF5_BASE_DIR, func=lambda x: fill_tags(x))
        #
        # conn.commit()


        '''code to fill the artists table'''

        print 'filling artists'

        def fill_artists(filename):
            #print filename
            f = h5py.File(filename, 'r')
            artists_attributes = set(zip(f['metadata']['songs']['artist_name']  ,\
                                    f['metadata']['songs']['artist_familiarity'] ,\
                                    f['metadata']['songs']['artist_hotttnesss'] , \
                                    f['metadata']['songs']['artist_mbid'] , \
                                    f['metadata']['songs']['artist_playmeid'] , \
                                    f['metadata']['songs']['artist_7digitalid'] , \
                                    f['metadata']['songs']['artist_id'] ) )
            f.close()
            for artist_info in artists_attributes:
                    artist_info = list(artist_info)
                    if math.isnan(artist_info[1]):
                        artist_info[1]=0.0 #There seems to exist nan for artist familiarity
                    if math.isnan(artist_info[2]):
                        artist_info[2]=0.0
                    cursor = conn.cursor()
                    cursor.callproc('fill_artist', artist_info)
                    cursor.close()

        # apply_to_all_files(HDF5_BASE_DIR, func=lambda x: fill_artists(x))
        #
        # conn.commit()

        '''code to fill the albums table'''

        print 'filling albums'

        def fill_albums(filename):
            # print filename
            f = h5py.File(filename, 'r')
            albums_attributes = set(zip(f['metadata']['songs']['release']  , \
                                        f['metadata']['songs']['release_7digitalid'] ,
                                        f['metadata']['songs']['artist_id']
                                     ) )
            f.close()
            for album_info in albums_attributes:
                    cursor = conn.cursor()
                    cursor.callproc('fill_album', album_info)
                    cursor.close()


        # apply_to_all_files(HDF5_BASE_DIR, func=lambda x: fill_albums(x))
        #
        # conn.commit()

        '''code to fill the songs table'''

        print 'filling songs'

        def fill_songs(filename):
            # print filename
            f = h5py.File(filename, 'r')
            #fetching all the information about the song, and the keys to search for, to find its album and artist
            songs_attributes = set(zip(f['metadata']['songs']['song_id'], \
                                        f['analysis']['songs']['track_id'],\
                                        f['metadata']['songs']['track_7digitalid'] ,\
                                        f['metadata']['songs']['title'] , \
                                        f['musicbrainz']['songs']['year'],\
                                        f['analysis']['songs']['duration'] ,\
                                        f['metadata']['songs']['song_hotttnesss'],\
                                        f['metadata']['songs']['release_7digitalid'] , \
                                        f['metadata']['songs']['artist_id']
                                       ))



            f.close()
            for song_info in songs_attributes:
                song_info=list(song_info)
                if math.isnan(song_info[6]):
                    song_info[6]=0

                cursor = conn.cursor()
                cursor.callproc('fill_song', song_info)
                cursor.close()

        # apply_to_all_files(HDF5_BASE_DIR, func=lambda x: fill_songs(x))
        #
        # conn.commit()

        '''code to fill the artist_terms table'''

        print 'filling artist_terms'


        def fill_artist_terms(filename):
            # print filename
            f = h5py.File(filename, 'r')
            artist_id = f['metadata']['songs']['artist_id'][0]
            terms = list(f['metadata']['artist_terms'])
            freqs = list(f['metadata']['artist_terms_freq'])
            weights = list(f['metadata']['artist_terms_weight'])

            f.close()
            #find artist_db_id
            cursor = conn.cursor()
            cursor.execute("select id from artists where artist_id='%s'" % artist_id)
            artist_db_id=cursor.fetchone()
            cursor.close()
            for term,freq,weight in zip(terms,freqs,weights):
                cursor = conn.cursor()
                cursor.callproc('fill_artist_term', [artist_db_id,term,freq,weight])
                cursor.close()

        apply_to_all_files(HDF5_BASE_DIR, func=lambda x: fill_artist_terms(x))

        conn.commit()


        '''code to fill the artist_tags table'''

        print 'filling artist tags'


        def fill_artist_tags(filename):
            # print filename
            f = h5py.File(filename, 'r')
            artist_id = f['metadata']['songs']['artist_id'][0]
            tags = list(f['musicbrainz']['artist_mbtags'])
            counts = list(f['musicbrainz']['artist_mbtags_count'])
            f.close()
            # find artist_db_id
            cursor = conn.cursor()
            cursor.execute("select id from artists where artist_id='%s'" % artist_id)
            artist_db_id = cursor.fetchone()
            cursor.close()
            for tag,count in zip(tags, counts):
                cursor = conn.cursor()
                cursor.callproc('fill_artist_tag', [artist_db_id, tag, count])
                cursor.close()


        apply_to_all_files(HDF5_BASE_DIR, func=lambda x: fill_artist_tags(x))

        conn.commit()

        '''code to fill the arist similarities'''

        print 'filling artist similarities'

        def fill_artist_similarities(filename):
            # print filename
            f = h5py.File(filename, 'r')
            artist_id = f['metadata']['songs']['artist_id'][0]
            similar_artists=list(f['metadata']['similar_artists'])
            f.close()
            # find artist_db_id
            cursor = conn.cursor()
            cursor.execute("select id from artists where artist_id='%s'" % artist_id)
            artist_db_id = cursor.fetchone()
            cursor.close()
            for similar_artist in similar_artists:
                cursor = conn.cursor()
                cursor.callproc('fill_artist_similarity', [artist_db_id, similar_artist])
                cursor.close()


        # apply_to_all_files(HDF5_BASE_DIR, func=lambda x: fill_artist_similarities(x))
        #
        # conn.commit()
        conn.close()



    except mdb.Error, e:
        print 'Problem with db interaction'
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)

