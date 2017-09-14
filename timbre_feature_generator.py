'''
Generating features for each song and saving it into a csv file
'''


import os
import sys
import time
import glob
import copy
import tables
import sqlite3
import datetime
import multiprocessing
import numpy as np
import h5py
import csv

HDF5_BASE_DIR = '/media/shahin/1ad8a514-de7f-485a-ba08-8a0f921ee083/million_songs_dataset'
CSV_OUTPUT = '/media/shahin/1ad8a514-de7f-485a-ba08-8a0f921ee083/songs_features.csv'


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



def save_all_features(basedir,csv_writer, func=lambda x: x,ext='.h5'):
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

            features=func(f)
            csv_writer.writerow(features)

            cnt += 1
    return cnt

def compute_features(file_name):
    """
    From an open HDF5 song file, extract average and covariance of the
    timbre vectors.
    RETURN 1x90 vector or None if there is a problem
    """
    f=h5py.File(file_name,'r')
    track_id=f['analysis']['songs']['track_id'][0]


    feats =np.array(f['analysis']['segments_timbre'])
    feats = np.transpose(feats)
    # features length
    ftlen = feats.shape[1]
    ndim = feats.shape[0]
    assert ndim == 12, 'WRONG FEATURE DIMENSION, transpose issue?'
    finaldim = 90
    # too small case
    if ftlen < 3:
        print 'occures'
        features=np.zeros(90)
        output=list()
        output.append(track_id)
        output.extend(features)
        return output
    # avg
    avg = np.average(feats, 1)
    # cov
    cov = np.cov(feats)
    covflat = []
    for k in range(12):
        covflat.extend(np.diag(cov, k))
    covflat = np.array(covflat)
    # concatenate avg and cov
    feats = np.concatenate([avg, covflat])
    # done, reshape & return
    features= feats.reshape(finaldim)
    output = list()
    output.append(track_id)
    output.extend(features)
    return output


if __name__ == '__main__':

    #open csv file and pass it
    with open(CSV_OUTPUT , 'w') as csv_file:
        writer=csv.writer(csv_file,delimiter=',')
        save_all_features(HDF5_BASE_DIR,writer, func=lambda x: compute_features(x) ,ext='.h5')


