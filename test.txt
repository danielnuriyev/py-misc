import pytest
from datetime import datetime
import numpy as np

# Your task is to write the group adjustment method below. There are multiple
# unit tests below, your job is to make sure they all pass. Feel free
# to add more tests.
# Your solution can be pure python, pure NumPy, pure Pandas
# or any combination of the three.  There are multiple ways of solving this
# problem, be creative, use comments to explain your code.

# Group Adjust Method
# The algorithm needs to do the following:
# 1.) For each group-list provided, calculate the means of the values for each
# unique group.
#
#   For example:
#   vals       = [  1  ,   2  ,   3  ]
#   ctry_grp   = ['USA', 'USA', 'USA']
#   state_grp  = ['MA' , 'MA' ,  'CT' ]
#
#   There is only 1 country in the ctry_grp list.  So to get the means:
#     USA_mean == mean(vals) == 2
#     ctry_means = [2, 2, 2]
#   There are 2 states, so to get the means for each state:
#     MA_mean == mean(vals[0], vals[1]) == 1.5
#     CT_mean == mean(vals[2]) == 3
#     state_means = [1.5, 1.5, 3]
#
# 2.) Using the weights, calculate a weighted average of those group means
#   Continuing from our example:
#   weights = [.35, .65]
#   35% weighted on country, 65% weighted on state
#   ctry_means  = [2  , 2  , 2]
#   state_means = [1.5, 1.5, 3]
#   weighted_means = [2*.35 + .65*1.5, 2*.35 + .65*1.5, 2*.35 + .65*3]
#
# 3.) Subtract the weighted average group means from each original value
#   Continuing from our example:
#   val[0] = 1
#   ctry[0] = 'USA' --> 'USA' mean == 2, ctry weight = .35
#   state[0] = 'MA' --> 'MA'  mean == 1.5, state weight = .65
#   weighted_mean = 2*.35 + .65*1.5 = 1.675
#   demeaned = 1 - 1.675 = -0.675
#   Do this for all values in the original list.
#
# 4.) Return the demeaned values

# Hint: See the test cases below for how the calculation should work.


def _group_adjust(vals, groups, weights):
    """
    Calculate a group adjustment (demean).

    Parameters
    ----------

    vals    : List of floats/ints

        The original values to adjust

    groups  : List of Lists

        A list of groups. Each group will be a list of ints

    weights : List of floats

        A list of weights for the groupings.

    Returns
    -------

    A list-like demeaned version of the input values
    """
    pass

def group_adjust(vals, groups, weights):

    groupCount = len(groups)
    if groupCount != len(weights):
        raise ValueError('groups and weights should have the same length')

    l = len(vals)
    for g in range(groupCount):
        if len(groups[g]) != l:
            # it is better to check in advance than discover on the last group
            raise ValueError('groups and vals must have the same shape')

    max = l - 1
    means = {}  # group index -> key -> mean
    # data will look like: {0: {'USA': 3.8}, 1: {'MA': 2.0, 'RI': 6.5}, 2: {'WEYMOUTH': 1.0, 'BOSTON': 2.5, 'PROVIDENCE': 6.5}}
    # where 0 is the index of the group, USA is the key and a9 is the sum of USA

    # loop by index
    for i in range(l):
        val = vals[i]
        inc = 1

        if val is None:
            val = 0
            inc = 0

        # within each index, iterate over groups
        for g in range(groupCount):
            group = groups[g]

            # from each group take the item at the current index
            groupVal = group[i]

            if not g in means:
                 means[g] = {groupVal : {'sum':val, 'count':inc}}
            else:
                map = means[g]
                if not groupVal in map:
                    map[groupVal] = {'sum':val, 'count':inc}
                else:
                    tmp = map[groupVal]
                    map[groupVal] = {'sum':tmp['sum'] + val, 'count':tmp['count'] + inc}


            if i == max: # to avoid looping over all data to find the avg
                groupMeans = means[g]
                for key, value in groupMeans.items():
                    groupMeans[key] = value['sum'] / value['count']

    #print(means)

    # create array of means per group
    meanLists = []

    for g in range(groupCount):
        group = groups[g]
        groupMeans = means[g]

        meanList = []
        for i in range(l):
            groupVal = group[i]
            mean = groupMeans[groupVal]

            meanList.append(mean)

        meanLists.append(meanList)

    #print(meanLists)

    #weighed avgs
    weightedList = []
    for i in range(l):

        allWeighted = 0
        for g in range(groupCount):
            meanList = meanLists[g]
            weighted = meanList[i] * weights[g]
            allWeighted += weighted

        if vals[i] is None:
            weightedList.append(None)
        else:
            weightedList.append(vals[i] - allWeighted)

    #print(weightedList)
    return weightedList

def test_three_groups():
    vals = [1, 2, 3, 8, 5]
    grps_1 = ['USA', 'USA', 'USA', 'USA', 'USA']
    grps_2 = ['MA', 'MA', 'MA', 'RI', 'RI']
    grps_3 = ['WEYMOUTH', 'BOSTON', 'BOSTON', 'PROVIDENCE', 'PROVIDENCE']
    weights = [.15, .35, .5]

    adj_vals = group_adjust(vals, [grps_1, grps_2, grps_3], weights)
    # 1 - (USA_mean*.15 + MA_mean * .35 + WEYMOUTH_mean * .5)
    # 2 - (USA_mean*.15 + MA_mean * .35 + BOSTON_mean * .5)
    # 3 - (USA_mean*.15 + MA_mean * .35 + BOSTON_mean * .5)
    # etc ...
    # Plug in the numbers ...
    # 1 - (.15*2 + .35*2 + .5*1)   # -0.5
    # 2 - (.15*2 + .35*2 + .5*2.5) # -.25
    # 3 - (.15*2 + .35*2 + .5*2.5) # 0.75
    # etc...

    answer = [-0.770, -0.520, 0.480, 1.905, -1.095]
    for ans, res in zip(answer, adj_vals):
        assert abs(ans - res) < 1e-5


def test_two_groups():
    vals = [1, 2, 3, 8, 5]
    grps_1 = ['USA', 'USA', 'USA', 'USA', 'USA']
    grps_2 = ['MA', 'RI', 'CT', 'CT', 'CT']
    weights = [.65, .35]

    adj_vals = group_adjust(vals, [grps_1, grps_2], weights)
    # 1 - (.65*2 + .35*1)   # -0.65
    # 2 - (.65*2 + .35*2.5) # -.175
    # 3 - (.65*2 + .35*2.5) # -.825
    answer = [-1.81999, -1.16999, -1.33666, 3.66333, 0.66333]
    for ans, res in zip(answer, adj_vals):
        assert abs(ans - res) < 1e-5


def test_missing_vals():
    # If you're using NumPy or Pandas, use np.NaN
    # If you're writing pyton, use None
    #vals = [1, np.NaN, 3, 5, 8, 7]
    vals = [1, None, 3, 5, 8, 7]
    grps_1 = ['USA', 'USA', 'USA', 'USA', 'USA', 'USA']
    grps_2 = ['MA', 'RI', 'RI', 'CT', 'CT', 'CT']
    weights = [.65, .35]

    adj_vals = group_adjust(vals, [grps_1, grps_2], weights)

    # This should be None or np.NaN depending on your implementation
    # please feel free to change this line to match yours
    #answer = [-2.47, np.NaN, -1.170, -0.4533333, 2.54666666, 1.54666666]
    answer = [-2.47, None, -1.170, -0.4533333, 2.54666666, 1.54666666]

    for ans, res in zip(answer, adj_vals):
        if ans is None:
            assert res is None
        elif np.isnan(ans):
            assert np.isnan(res)
        else:
            assert abs(ans - res) < 1e-5


def test_weights_len_equals_group_len():
    # Need to have 1 weight for each group

    # vals = [1, np.NaN, 3, 5, 8, 7]
    vals = [1, None, 3, 5, 8, 7]
    grps_1 = ['USA', 'USA', 'USA', 'USA', 'USA', 'USA']
    grps_2 = ['MA', 'RI', 'RI', 'CT', 'CT', 'CT']
    weights = [.65]

    with pytest.raises(ValueError):
        group_adjust(vals, [grps_1, grps_2], weights)


def test_group_len_equals_vals_len():
    # The groups need to be same shape as vals
    vals = [1, None, 3, 5, 8, 7]
    grps_1 = ['USA']
    grps_2 = ['MA', 'RI', 'RI', 'CT', 'CT', 'CT']
    weights = [.65, .35]

    with pytest.raises(ValueError):
        group_adjust(vals, [grps_1, grps_2], weights)


def test_performance(k):
    # vals = 1000000*[1, None, 3, 5, 8, 7]
    # If you're doing numpy, use the np.NaN instead
    vals = k * [1, None, 3, 5, 8, 7]
    grps_1 = k * [1, 1, 1, 1, 1, 1]
    grps_2 = k * [1, 1, 1, 1, 2, 2]
    grps_3 = k * [1, 2, 2, 3, 4, 5]
    weights = [.20, .30, .50]

    start = datetime.now()
    group_adjust(vals, [grps_1, grps_2, grps_3], weights)
    end = datetime.now()
    diff = end - start
    print('Total performance test time: {}'.format(diff.total_seconds()))

if __name__== "__main__":

    test_three_groups()
    test_two_groups()
    test_missing_vals()
    test_weights_len_equals_group_len()
    test_group_len_equals_vals_len()
    test_performance(1000000)
