
# I learned it from here: https://onlinecourses.science.psu.edu/stat507/node/71

def prepareData(actual, predicted):

    n = len(actual)

    truePositives = 0
    falsePositives = 0
    falseNegatives = 0
    trueNegatives = 0

    # a better solution is a map/reduce on multiple cores
    # in python this would require a process pull
    # in jvm there's a parallel stream
    for i in range(n):
        a = actual[i]
        p = predicted[i]

        if a == 1:
            if p == 1:
                truePositives = truePositives + 1
            else:
                falseNegatives = falseNegatives + 1
        elif a == 0:
            if p == 0:
                trueNegatives = trueNegatives + 1
            else:
                falsePositives = falsePositives + 1

    return {
        'truePositives' : truePositives,
        'trueNegatives' : trueNegatives,
        'falsePositives' : falsePositives,
        'falseNegatives' : falseNegatives
    }

def sensitivity(actual, predicted):

    d = prepareData(actual, predicted)

    return d['truePositives'] / (d['truePositives'] + d['falseNegatives'])


def specificity(actual, predicted):

    d = prepareData(actual, predicted)

    return d['trueNegatives'] / (d['trueNegatives'] + d['falsePositives'])

def solution(A, B, q):

    # TODO: verify arrays equal len

    if q == True: return specificity(A, B)
    else: return sensitivity(A, B)
