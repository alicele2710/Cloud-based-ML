import h5py
import os
import boto3
import csv
import numpy as np

"""A Complete list of features we are interested in.

'artist_familiarity',
'artist_hotttnesss',
'artist_id',
'artist_latitude',
'artist_location',
'artist_longitude',
'artist_name',
'title',
"artist_terms",
"artist_terms_freq",
"artist_terms_weight",
'danceability',
'duration',
'end_of_fade_in',
'energy',
'key',
'key_confidence',
'loudness',
'mode',
'mode_confidence',
'start_of_fade_out',
'tempo',
'time_signature',
'time_signature_confidence'
'year',
"""

def process_h5_file(h5_file):
    """Process a single h5 file to extract features listed above from the raw MSD.

     For example, to get `artist_familiarity`, refer to:

     https://github.com/tbertinmahieux/MSongsDB/blob/master/PythonSrc/hdf5_getters.py

     So we see that it does h5.root.metadata.songs.cols.artist_familiarity[songidx]
     and it would translate to:

       num_songs = len(file['metadata']['songs'])
       file['metadata']['songs'][:num_songs]['artist_familiarity']

     Since there is one song per file, it simplifies to:

       file['metadata']['songs'][:1]['artist_familiarity']

     We recommend downloading one file, opening it with h5py, and explore/practice

     To see the datatype and shape:

     http://millionsongdataset.com/pages/field-list/
     http://millionsongdataset.com/pages/example-track-description/
     """

    # return the row as a list of values
    row = []

    """
    You should include all fields mentioned at the top of this file.
    You may store the feature names as lists of strings and process the
    features by groups with loops.
    """

    # Example group name
    metadata = [
        'artist_familiarity',
        'artist_hotttnesss',
        'artist_id',
        'artist_latitude',
        'artist_location',
        'artist_longitude',
        'artist_name',
        'title',
    ]


    metadata_array = [
        "artist_terms",
        "artist_terms_freq",
        "artist_terms_weight",
    ]
    analysis = [
        'danceability',
        'duration',
        'end_of_fade_in',
        'energy',
        'key',
        'key_confidence',
        'loudness',
        'mode',
        'mode_confidence',
        'start_of_fade_out',
        'tempo',
        'time_signature',
        'time_signature_confidence'
    ]
    musicbrainz = [
        'year',
    ]

    """
    Extract field values

    The values of some fields need to be decoded as 'utf-8'.

    You should inspect each feature to make sure that the values
    make sense.

    If song_hotttnesss is NaN, return [] immediately.

    """

    # hotness
    hotness = h5_file['metadata']['songs'][:1]['song_hotttnesss'][0]
    if np.isnan(hotness):
        return []
    row.append(hotness)
    # meta data
    for col_name in metadata:
        res = h5_file['metadata']['songs'][:1][col_name][0]
        if type(res) == np.bytes_:
            res = res.decode('utf-8')
        row.append(res)
    # analysis
    for col_name in analysis:
        row.append(h5_file['analysis']['songs'][:1][col_name][0])
    # metadata_array
    for col_name in metadata_array:
        arr = h5_file['metadata'][col_name][:]
        if col_name == 'artist_terms':
            arr = map(lambda f: f.decode('utf-8'), arr)
        row.append(list(arr))
    # musicbrainz
    for col_name in musicbrainz:
        row.append(h5_file['musicbrainz']['songs'][:1][col_name][0])
    return row


def process_h5_file_wrapper(path):
    """
    Wrapper function that processes a local h5 file. 
    
    Note that we are treating the h5 file as local
    because we are mounting the MSD snapshot on our instance.

    Do defensive programming by wrapping your call to `process_h5_file` 
    in try/except. Think about why this is useful.
    """
    try:
        with h5py.File(path) as h5:
            return process_h5_file(h5)
    except Exception as e:
        print(e)
        return []

def save_rows(chunk_id, rows, save_local=False):
    """
    Save a list of rows into a temporary local CSV and optionally upload to S3.

    - chunk_id: Chunk id, also the name of our csv file
    - rows: A list of rows which are results of `transform_local`
    - save_local: False if upload to S3. True if save to local (for testing).

    HINT: You may use the `csv` module.
    (You can use other libs like pandas if you like)
    """

    path = f'processed/{chunk_id}.csv'

    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(rows)

    if save_local:
        print(f'csv saved to: {path}')
        return

    """
    You will need your S3 bucket name.
    
    You may find the "Bucket Instance Version" section of the 
    following tutorial helpful:
    https://realpython.com/python-boto3-aws-s3/
    """ 

    ### YOUR CODE HERE

    # ----- BEGIN SOLUTION -----
    bucket='hw4msd'
    # ------ END SOLUTION ------
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).upload_file(path,path)
    print('csv saved to s3: ', path)

    # Remove the tempory csv file after we upload it to S3
    os.remove(path)

"""Convert all files

In this step, we will divide the h5 data points to chunks, where each chunk
will produce a `csv` file that gets stored into your S3 bucket.

We will use `argparse` to parse our command line arguments. The two arguments
are the number of workers and the worker's ID.

For example, a sample run may be:
    `python million_song_reader.py 4 0`

Note: If you have 4 workers, you are expected to run the scripts 4 times with
worker ids 0 to 3, either on a single machine (multiple thread)
or on multiple machines.

Your job is to implement scripts that can partition the conversion task
into `num_workers` parts so that they can run in parallel.
This will speed up the conversion procedure with the same budget by taking
full use of resources.

You will use `process_h5_file_wrapper` and `save_rows`.
You may find `os.walk('YOUR_PATH')` helpful.

You should accumulate `CHUNK_SIZE` rows before calling `save_rows` to write
a chunk to disk. Do not change `CHUNK_SIZE` for grading purposes.
"""
if __name__ == "__main__":
    CHUNK_SIZE = 10000
    progress_ckpt_size = 100
    save_to_local = False

    ### YOUR CODE HERE

    # ----- BEGIN SOLUTION -----
    data_path='songs/data/'
    # ------ END SOLUTION ------

    import argparse

    parser = argparse.ArgumentParser(description='null')

    parser.add_argument('num_workers', metavar='N', type=int, help='num_workers')
    parser.add_argument('worker_id', metavar='i', type=int, help='worker_id')
    args = parser.parse_args()


    thread_cnt = args.num_workers
    letter_chunk_size = 26//thread_cnt
    letters = [chr(ord('A')+i) for i in range(26)]
    works = []
    for i in range(thread_cnt):
        if i == thread_cnt-1:
            works.append(letters[i*letter_chunk_size:])
        else:
            works.append(letters[i*letter_chunk_size:(i+1)*letter_chunk_size])

    letters = works[args.worker_id]
    print(letters)
    processed = []
    acc = 0
    for letter in letters:
        for root, dirs, files in os.walk(data_path+letter):
            for f in files:
                path = os.path.join(root, f)
                # print('processing:',path)
                result = process_h5_file_wrapper(path)
                if result:
                    processed.append(result)
                    acc += 1

                    if acc % progress_ckpt_size == 0:
                        print('processed: ', acc)

                    if len(processed) % CHUNK_SIZE == 0:
                        n = acc // CHUNK_SIZE
                        chunk_id = f'{letter}_{n}'
                        save_rows(chunk_id, processed, save_to_local)
                        # upload to s3. make sure to not overwrite the name
                        processed = []
        if processed:
            print('final write')
            n = acc // CHUNK_SIZE + 1
            chunk_id = f'{letter}_{n}'
            save_rows(chunk_id, processed, save_to_local)
            processed = []




