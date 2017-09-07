import h5py

filename = '/media/shahin/data/msd_recom/data/msd_subset_data/MillionSongSubset/data/A/A/B/TRAABCL128F4286650.h5' #'/home/shahin/Desktop/TRAXLZU12903D05F94.h5'
f = h5py.File(filename, 'r')



# List all groups
print("Keys: %s" % f.keys())
a_group_key = f.keys()[0]

# Get the data
data = list(f[a_group_key])
f.close()