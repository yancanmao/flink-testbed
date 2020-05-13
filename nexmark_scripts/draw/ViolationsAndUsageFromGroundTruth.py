# -*- coding: utf-8 -*-
import matplotlib

matplotlib.use('Agg')
import numpy as np
import matplotlib.pyplot as plt
import os
from os import listdir
import sys
from RateAndWindowDelay import draw as ratedraw

SMALL_SIZE = 25
MEDIUM_SIZE = 30
BIGGER_SIZE = 35

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

figureName = sys.argv[1]
warmup = int(sys.argv[2])
runtime = int(sys.argv[3])

userLatency1 = int(sys.argv[4])
userLatency2 = int(sys.argv[5])

userLatencyMap = {"c21234bcbf1e8eb4c61f1927190efebd": userLatency1, "b71731f1c0df9c3076c4a455334d0ad6": userLatency2}

userWindow = 1000
base = 1000  # timeslot size
warmUpIntervals = [[0, warmup]]
calculateInterval = [0, runtime]  # The interval we calculate violation percentage from 1st tuple completed
# totalLength = 7100
substreamAvgLatency = {"c21234bcbf1e8eb4c61f1927190efebd": {}, "b71731f1c0df9c3076c4a455334d0ad6": {}}  # Dict { operatorId : substreamId : [[Arrival, Completed]...]}

numberOfOEsMap = {}

# inputDir = '/home/samza/workspace/flink-extended/build-target/log/'
inputDir = '/home/samza/workspace/flink-testbed/nexmark_scripts/draw/logs/' + figureName + '/'
# inputDir = '/home/myc/workspace/SSE-anaysis/src/nexmark_scripts/log/'
outputDir = 'figures/' + figureName + '/'

keyAverageLatencyFlag = True
keyAverageLatencyThreshold = 0.2
keyLatencyIntervalFlag = False
calibrateFlag = False

startTime = sys.maxint

maxMigrationTime = 0
maxMigrationExecutor = ""
migrationTimes = []
for fileName in listdir(inputDir):
    if fileName == "flink-samza-taskexecutor-0-camel-sane.out":
        inputFile = inputDir + fileName
        counter = 0
        print("Processing file " + inputFile)
        startPoint = []
        endPoint = []
        startLogicTime = sys.maxint
        startOETime = sys.maxint
        t1 = 0
        with open(inputFile) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split(' ')

                if split[0] in substreamAvgLatency:
                    # process ground truth records [GroundTruth: , timeslot, keygroup, nRecords, avgLatency]
                    if split[1] == 'GroundTruth:':
                        jobid = split[0]
                        timeslot = int(split[2])
                        keygroup = int(split[3])
                        nRecords = int(split[4])
                        avgLatency = float(split[5])

                        if timeslot < startTime:
                            startTime = timeslot
                        if keygroup not in substreamAvgLatency[jobid]:
                            substreamAvgLatency[jobid][keygroup] = {}

                        alignedTimeslot = (timeslot - startTime) / 1000

                        if alignedTimeslot in substreamAvgLatency[jobid][keygroup]:
                            print("+++++++ Ground truth error, check whether it is buggy")
                        substreamAvgLatency[jobid][keygroup][alignedTimeslot] = avgLatency

                    if split[0] == 'Entering':
                        startPoint += [int(split[3])]
                    if split[0] == 'Shutdown':
                        endPoint += [int(split[2])]
        migrationTime = []
        for i in range(0, len(endPoint)):
            if i + 1 < len(startPoint):
                migrationTime += [startPoint[i + 1] - endPoint[i]]
                migrationTimes += [migrationTime[-1] / 1000.0]
        if len(migrationTime) > 0:
            mmaxMigrationTime = max(migrationTime)
            if (mmaxMigrationTime > maxMigrationTime):
                maxMigrationTime = mmaxMigrationTime
                maxMigrationExecutor = fileName
            print(fileName, mmaxMigrationTime)
            print(startPoint, endPoint)

print(maxMigrationTime, maxMigrationExecutor)

def operator_ground_truth(jobid):
    userLatency = userLatencyMap[jobid]
    totalTime = 0
    totalViolation = 0
    violationInWarmUp = []
    totalInPeak = []

    # Translate time from second to user window index
    for peakI in range(0, len(warmUpIntervals)):
        violationInWarmUp += [0]
        totalInPeak += [0]
        warmUpIntervals[peakI] = [warmUpIntervals[peakI][0] * base / userWindow, warmUpIntervals[peakI][1] * base / userWindow]
    xaxes = [calculateInterval[0] * 1000 / userWindow, calculateInterval[-1] * 1000 / userWindow]

    substreamTime = []
    substreamViolation = []
    substreamLatency = []
    totalViolationSubstream = {}
    # Draw average latency
    for substream in sorted(substreamAvgLatency[jobid]):
        curSubStreamlatency = substreamAvgLatency[jobid][substream]
        print("Calculate substream " + str(substream))

        # x is time slot, y is avg latency list
        x = []
        y = []

        # the total time interval for violation calculation
        thisTime = (xaxes[1] - xaxes[0] + 1)
        for peak in range(0, len(warmUpIntervals)):
            totalInPeak[peak] += (warmUpIntervals[peak][1] - warmUpIntervals[peak][0] + 1)

        # already be avg latency, just need to form the x and y
        thisViolation = 0
        thisViolationInterval = []
        for time in curSubStreamlatency:
            avgLatency = curSubStreamlatency[time]

            if time not in totalViolationSubstream:
                totalViolationSubstream[time] = []
            if avgLatency > 1000 and substream not in totalViolationSubstream[time]:
                totalViolationSubstream[time].append(substream)

            x += [time]
            y += [avgLatency]
            if xaxes[0] <= time <= xaxes[1] and avgLatency > userLatency:
                thisViolation += 1
                if len(thisViolationInterval) > 0 and thisViolationInterval[-1][1] == time - 1:
                    thisViolationInterval[-1][1] = time
                else:
                    thisViolationInterval.append([time, time])
            # Calculate peak interval
            for i in range(0, len(warmUpIntervals)):
                if warmUpIntervals[i][0] <= time <= warmUpIntervals[i][1] and avgLatency > userLatency:
                    violationInWarmUp[i] += 1

        substreamTime += [thisTime]
        substreamViolation += [thisViolation]
        percentage = 0.0
        if thisTime > 0:
            percentage = thisViolation / float(thisTime)
        print(str(substream), percentage, thisTime)
        totalTime += thisTime
        totalViolation += thisViolation

        if keyAverageLatencyFlag:
            print("Draw ", substream, " violation percentage...")
            outputFile = outputDir + jobid + '_windowLatency/' + str(substream) + '.png'
            if not os.path.exists(outputDir + jobid + '_windowLatency/'):
                os.makedirs(outputDir + jobid + '_windowLatency')
            legend = ['Window Average Latency']
            fig = plt.figure(figsize=(16, 9))
            plt.plot(x, y, 'bs')

            # Add user requirement
            userLineX = [xaxes[0], xaxes[1]]
            userLineY = [userLatency, userLatency]
            userLineC = 'r'
            plt.plot(userLineX, userLineY, linewidth=3.0, color=userLineC, linestyle='--')

            plt.legend(legend, loc='upper left')
            # print(arrivalRateT, arrivalRate)
            plt.grid(True)
            axes = plt.gca()
            axes.set_xlim(xaxes)
            axes.set_ylim([1, 10 ** 6])
            axes.set_yscale('log')
            plt.xlabel('Timeslot Index')
            plt.ylabel('Average Latency')
            plt.title('Window Average Latency')
            plt.savefig(outputFile)
            plt.close(fig)
        if (keyLatencyIntervalFlag):
            x = []
            for i in range(0, len(thisViolationInterval)):
                # print(thisViolationInterval[i])
                x += [thisViolationInterval[i][1] - thisViolationInterval[i][0] + 1]
            outputFile = outputDir + 'latencyInterval/' + substream + '.png'
            if not os.path.exists(outputDir + 'latencyInterval'):
                os.makedirs(outputDir + 'latencyInterval')
            legend = ['Latency Interval']
            fig = plt.figure(figsize=(16, 9))
            plt.hist(x, bins=range(0, 200))
            axes = plt.gca()
            axes.set_xticks(range(0, 200))
            axes.set_yticks(np.arange(0, 200, 5).tolist())
            plt.grid(True)
            plt.xlabel('Latency Interval Length')
            plt.ylabel('# of Interval')
            plt.title('Latency Interval')
            plt.savefig(outputFile)
            plt.close(fig)
    # draw substream violation
    outputFile = outputDir + jobid + '_violation.png'
    legend = ['Total substream violation']
    figList = []
    for substreamList in totalViolationSubstream:
        figList.append(len(totalViolationSubstream[substreamList]))
    fig = plt.figure(figsize=(16, 9))
    plt.plot(figList)
    plt.xlabel('Timeslot Index')
    plt.ylabel('#substream violation')
    plt.title('Total substream violation')
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputFile)
    # Draw substream violation percetage histogram
    if (True):
        print("Draw overall violation percentage figure...")

        outputFile = outputDir + jobid + '_keyViolationPercentage.png'
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)

        legend = ['Violation Percentage']
        fig = plt.figure(figsize=(32, 18))
        x = []
        for i in range(0, len(substreamTime)):
            x += [substreamViolation[i] / float(substreamTime[i])]
        bins = np.arange(0, 0.2, 0.01).tolist() + np.arange(0.2, 1, 0.05).tolist()
        plt.hist(x, bins=bins)
        axes = plt.gca()
        axes.set_xticks(bins)
        axes.set_yticks(np.arange(0, 1000, 50).tolist())
        plt.grid(True)
        plt.xlabel('Violation Percentage')
        plt.ylabel('# of Keys')
        plt.title('Keys Violation Percentage')
        plt.savefig(outputFile)
        plt.close(fig)
    # run RateAndWindowDelay, and get the stats:
    avgViolationPercentage = totalViolation / float(totalTime)
    sumDeviation = 0.0
    print('avg success rate=', 1 - avgViolationPercentage)
    print('total violation number=' + str(totalViolation))
    # print >> f, ('avg success rate=', 1 - avgViolationPercentage)
    # print >> f, ('total violation number=' + str(totalViolation))
    violationNotPeak = totalViolation
    timeNotPeak = totalTime
    if (totalViolation > 0):
        for peakI in range(0, len(warmUpIntervals)):
            print('violation percentage in warm up ' + str(peakI) + ' is ' + str(
                violationInWarmUp[peakI] / float(totalViolation)) + ', number is ' + str(violationInWarmUp[peakI]))
            violationNotPeak -= violationInWarmUp[peakI]

            # print >> f, ('violation percentage in warm up ' + str(peakI) + ' is ' + str(
            #     violationInWarmUp[peakI] / float(totalViolation)) + ', number is ' + str(violationInWarmUp[peakI]))

            timeNotPeak -= totalInPeak[peakI]
    successRate = 1 - violationNotPeak / float(timeNotPeak)
    print('Execept warm up avg success rate=', successRate)
    # print >> f, ('Execept warm up avg success rate=', 1 - violationNotPeak / float(timeNotPeak))
    retValue = ratedraw(100, figureName, warmup, runtime, jobid)  # [AvgOEs, NumLB, NumSI, NumSO, OEsList]
    # # parse figurename and get configurations
    # def parseName(figureName):
    #


    if len(retValue[4]) >= runtime:
        numberOfOEsMap[jobid] = retValue[4][warmup:runtime]
    else:
        numberOfOEsMap[jobid] = retValue[4][warmup:]

    # print(numberOfOEsMap[jobid])

    maxOEs = max(numberOfOEsMap[jobid])
    minOEs = min(numberOfOEsMap[jobid])

    stats_logs_path = outputDir + 'stats.txt'
    with open(stats_logs_path, 'a') as f:
        f.write("%s\t%s\t%d\t%d\t%d\t%s\t%d-%d\t%.15f\n" %
                (figureName, jobid, retValue[1], retValue[2], retValue[3], retValue[0], minOEs, maxOEs, successRate))


def app_ground_truth():
    # sum avg latency of two job, and then calculate succ rate
    userLatency = userLatency1 + userLatency2

    appAvglatency = {} # [keygroup -> timeslot -> avglatency]
    for jobid in substreamAvgLatency:
        for keygroup in substreamAvgLatency[jobid]:
            for timeslot in substreamAvgLatency[jobid][keygroup]:
                if keygroup not in appAvglatency:
                    appAvglatency[keygroup] = {}
                if timeslot not in appAvglatency[keygroup]:
                    appAvglatency[keygroup][timeslot] = 0
                appAvglatency[keygroup][timeslot] += substreamAvgLatency[jobid][keygroup][timeslot]

    totalTime = 0
    totalViolation = 0
    violationInWarmUp = []
    totalInPeak = []

    # Translate time from second to user window index
    for peakI in range(0, len(warmUpIntervals)):
        violationInWarmUp += [0]
        totalInPeak += [0]
        warmUpIntervals[peakI] = [warmUpIntervals[peakI][0] * base / userWindow, warmUpIntervals[peakI][1] * base / userWindow]
    xaxes = [calculateInterval[0] * 1000 / userWindow, calculateInterval[-1] * 1000 / userWindow]

    substreamTime = []
    substreamViolation = []
    substreamLatency = []
    totalViolationSubstream = {}

    # Draw average latency
    for substream in sorted(appAvglatency):
        curSubStreamlatency = appAvglatency[substream]
        print("Calculate substream " + str(substream))

        # x is time slot, y is avg latency list
        x = []
        y = []

        # the total time interval for violation calculation
        thisTime = (xaxes[1] - xaxes[0] + 1)
        for peak in range(0, len(warmUpIntervals)):
            totalInPeak[peak] += (warmUpIntervals[peak][1] - warmUpIntervals[peak][0] + 1)

        # already be avg latency, just need to form the x and y
        thisViolation = 0
        thisViolationInterval = []
        for time in curSubStreamlatency:
            avgLatency = curSubStreamlatency[time]

            if time not in totalViolationSubstream:
                totalViolationSubstream[time] = []
            if avgLatency > 1000 and substream not in totalViolationSubstream[time]:
                totalViolationSubstream[time].append(substream)

            x += [time]
            y += [avgLatency]
            if xaxes[0] <= time <= xaxes[1] and avgLatency > userLatency:
                thisViolation += 1
                if len(thisViolationInterval) > 0 and thisViolationInterval[-1][1] == time - 1:
                    thisViolationInterval[-1][1] = time
                else:
                    thisViolationInterval.append([time, time])
            # Calculate peak interval
            for i in range(0, len(warmUpIntervals)):
                if warmUpIntervals[i][0] <= time <= warmUpIntervals[i][1] and avgLatency > userLatency:
                    violationInWarmUp[i] += 1

        substreamTime += [thisTime]
        substreamViolation += [thisViolation]
        percentage = 0.0
        if thisTime > 0:
            percentage = thisViolation / float(thisTime)
        print(str(substream), percentage, thisTime)
        totalTime += thisTime
        totalViolation += thisViolation
        if keyAverageLatencyFlag:
            print("Draw ", substream, " violation percentage...")
            outputFile = outputDir + 'app_windowLatency/' + str(substream) + '.png'
            if not os.path.exists(outputDir + 'app_windowLatency/'):
                os.makedirs(outputDir + 'app_windowLatency/')
            legend = ['Window Average Latency']
            fig = plt.figure(figsize=(16, 9))
            plt.plot(x, y, 'bs')

            # Add user requirement
            userLineX = [xaxes[0], xaxes[1]]
            userLineY = [userLatency, userLatency]
            userLineC = 'r'
            plt.plot(userLineX, userLineY, linewidth=3.0, color=userLineC, linestyle='--')

            plt.legend(legend, loc='upper left')
            # print(arrivalRateT, arrivalRate)
            plt.grid(True)
            axes = plt.gca()
            axes.set_xlim(xaxes)
            axes.set_ylim([1, 10 ** 6])
            axes.set_yscale('log')
            plt.xlabel('Timeslot Index')
            plt.ylabel('Average Latency')
            plt.title('Window Average Latency')
            plt.savefig(outputFile)
            plt.close(fig)

        # Draw substream violation percetage histogram
    if (True):
        print("Draw overall violation percentage figure...")

        outputFile = outputDir + 'app_keyViolationPercentage.png'
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)

        legend = ['Violation Percentage']
        fig = plt.figure(figsize=(32, 18))
        x = []
        for i in range(0, len(substreamTime)):
            x += [substreamViolation[i] / float(substreamTime[i])]
        bins = np.arange(0, 0.2, 0.01).tolist() + np.arange(0.2, 1, 0.05).tolist()
        plt.hist(x, bins=bins)
        axes = plt.gca()
        axes.set_xticks(bins)
        axes.set_yticks(np.arange(0, 1000, 50).tolist())
        plt.grid(True)
        plt.xlabel('Violation Percentage')
        plt.ylabel('# of Keys')
        plt.title('Keys Violation Percentage')
        plt.savefig(outputFile)
        plt.close(fig)
    # run RateAndWindowDelay, and get the stats:
    avgViolationPercentage = totalViolation / float(totalTime)
    sumDeviation = 0.0
    print('avg success rate=', 1 - avgViolationPercentage)
    print('total violation number=' + str(totalViolation))
    # print >> f, ('avg success rate=', 1 - avgViolationPercentage)
    # print >> f, ('total violation number=' + str(totalViolation))
    violationNotPeak = totalViolation
    timeNotPeak = totalTime
    if (totalViolation > 0):
        for peakI in range(0, len(warmUpIntervals)):
            print('violation percentage in warm up ' + str(peakI) + ' is ' + str(
                violationInWarmUp[peakI] / float(totalViolation)) + ', number is ' + str(violationInWarmUp[peakI]))
            violationNotPeak -= violationInWarmUp[peakI]

            # print >> f, ('violation percentage in warm up ' + str(peakI) + ' is ' + str(
            #     violationInWarmUp[peakI] / float(totalViolation)) + ', number is ' + str(violationInWarmUp[peakI]))

            timeNotPeak -= totalInPeak[peakI]
    successRate = 1 - violationNotPeak / float(timeNotPeak)
    print('Execept warm up avg success rate=', successRate)

    # wholeViolation = totalViolation/float(timeWithoutPeak)
    # print('whole graph violation: ', wholeViolation)
    # print('interval: ' , timeWithoutPeak, ' num of violation: ', totalViolation, ' whole graph success rate: ', 1 - wholeViolation)

    sumNumOfOEs = {}
    for jobid in numberOfOEsMap:
        if len(numberOfOEsMap[jobid]) < runtime-warmup:
            print("wrong...")
        for idx, oes in enumerate(numberOfOEsMap[jobid]):
            if idx not in sumNumOfOEs:
                sumNumOfOEs[idx] = 0
            sumNumOfOEs[idx] += oes
    # print(sumNumOfOEs.values())

    stats_logs_path = outputDir + 'stats.txt'
    with open(stats_logs_path, 'a') as f:
        f.write("whole graph success rate: %.15f, min-max OEs: %d-%d\n" %
                (successRate, min(sumNumOfOEs.values()), max(sumNumOfOEs.values())))

def migration_time():
    # read migration time from taskexecutor.out
    # read life time from taskexecutor.log

    pass

for jobid in substreamAvgLatency:
    operator_ground_truth(jobid)


app_ground_truth()
