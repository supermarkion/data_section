from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import glob
import os
import pandas as pd
import numpy as np

TIME_COLUMN = 'capture_time'
SAMPLE_TRUTH_COLUMN = 'truth'
DENSE_TRUTH_COLUMN = 'truth_step_score'
TIME_UNIT = 1e6

def parallel_process(array, function, n_jobs=16, use_kwargs=False):
    """
        A parallel version of the map function with a progress bar. 

        Args:
            array (array-like): An array to iterate over.
            function (function): A python function to apply to the elements of array
            n_jobs (int, default=16): The number of cores to use
            use_kwargs (boolean, default=False): Whether to consider the elements of array as dictionaries of 
                keyword arguments to function 
            front_num (int, default=3): The number of iterations to run serially before kicking off the parallel job. 
                Useful for catching bugs
        Returns:
            [function(array[0]), function(array[1]), ...]
    """

    #Assemble the workers
    with ProcessPoolExecutor(max_workers=n_jobs) as pool:
        #Pass the elements of array into function
        if use_kwargs:
            futures = [pool.submit(function, **a) for a in array]
        else:
            futures = [pool.submit(function, a) for a in array]
        kwargs = {
            'total': len(futures),
            'unit': 'it',
            'unit_scale': True,
            'leave': True
        }
        #Print out the progress as tasks complete
        for _ in tqdm(as_completed(futures), **kwargs):
            pass
    out = []
    #Get the results from the futures. 
    for e_index, future in tqdm(enumerate(futures)):
        try:
            out.append(future.result())
        except Exception as e:
            print(e, 'happen in', array[e_index])
            out.append(e)
    return out


def fill_window_truth(df, prior_mins, post_mins):
    """
        A method to fill the sparse truth values in prior and post time windows.

        Args:
            df (Pandas DataFrame): The input of data which has sparse truth
            prior_mins (int): the prior time window - M, its unit is Minute
            post_mins (int): the post time windows - N, its unit is Minute

        Returns:
            df (Pandas DataFrame): the DataFrame has dense truth data
    """

    df[DENSE_TRUTH_COLUMN] = [np.NaN] * len(df)
    
    # get dense truth value firstly
    raw_truth_df = df[df[SAMPLE_TRUTH_COLUMN] == df[SAMPLE_TRUTH_COLUMN]]
    
    # Forward, ascending order, fill post values
    # We assign the value from left to right, to make sure we can handle the case which N < X 
    for raw_truth_index in range(len(raw_truth_df)):

        capture_time = raw_truth_df.iloc[raw_truth_index][TIME_COLUMN] # Truth sample time
        end_boundary_capture_time = capture_time + (post_mins * 60 * TIME_UNIT) # Truth sample time + N minutes
        
        # the post time window range in dataframe format
        assign_df = df[(df[TIME_COLUMN] >= capture_time) & (df[TIME_COLUMN] <= end_boundary_capture_time)]
    
        end_capture_time = assign_df.iloc[-1][TIME_COLUMN] # grab the last frame time
        end_index = df.index[df[TIME_COLUMN] == end_capture_time].tolist()[0] # use the last frame time to find end frame index value
        current_index = df.index[df[TIME_COLUMN] == capture_time].tolist()[0] # use current sample time to find start frame index value

        # assign the sample truth value to post time window
        df.loc[current_index:end_index + 1, DENSE_TRUTH_COLUMN] = round(raw_truth_df.iloc[raw_truth_index][SAMPLE_TRUTH_COLUMN])

    # Backward, descending order, fill prior values
    # We assign the value from right to left, to make sure we can handle the case which M < X
    for raw_truth_index in range(len(raw_truth_df)):
        reverse_index = len(raw_truth_df) - 1 - raw_truth_index # start from the last Truth sample
    
        capture_time = raw_truth_df.iloc[reverse_index][TIME_COLUMN] # Truth sample time
        start_boundary_capture_time = capture_time - (prior_mins * 60 * TIME_UNIT) # Truth sample time - M minutes
        
        # the prior time window range in dataframe format
        assign_df = df[(df[TIME_COLUMN] >= start_boundary_capture_time) & (df[TIME_COLUMN] <= capture_time)]
        
        start_capture_time = assign_df.iloc[0][TIME_COLUMN]
        start_index = df.index[df[TIME_COLUMN] == start_capture_time].tolist()[0]
        current_index = df.index[df[TIME_COLUMN] == capture_time].tolist()[0]

        # assign the sample truth value to prior time window
        df.loc[start_index:current_index + 1, DENSE_TRUTH_COLUMN] = round(raw_truth_df.iloc[reverse_index][SAMPLE_TRUTH_COLUMN])

    df['Elapsed minutes'] = (df[TIME_COLUMN] - df.iloc[0][TIME_COLUMN]) / (60*TIME_UNIT) # convert the time to elapsed minutes as a human read-able data

    return df

def process_truth_file(truth_file):
    """
        A wrap-up method which can be passed to parallel running.

        Args:
            truth_file (str): the path of dense truth file

        Returns:
            df (Pandas DataFrame): the DataFrame has dense truth data
    """
    return assign_values(pd.read_csv(truth_file), prior_mins=10, post_mins=10) # assume the M=10, N=10

def main():
    all_truth_files = glob.glob("UntitleSource/**/*.csv", recursive=True)
    parallel_process(all_truth_files, process_truth_file, n_jobs=16)

if __name__ == '__main__':
    main()

