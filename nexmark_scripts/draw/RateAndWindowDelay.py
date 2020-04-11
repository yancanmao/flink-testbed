# -*- coding: utf-8 -*-

import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

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


jobname = sys.argv[1]
warmup = int(sys.argv[2])
runtime = int(sys.argv[3])
input_file = '/home/samza/workspace/flink-extended/build-target/log/flink-samza-standalonesession-0-camel-sane.out'
# input_file = '/home/samza/workspace/newGT/log/flink-samza-standalonesession-0-camel-sane.out'
# input_file = '/home/samza/workspace/build-target/log/flink-samza-standalonesession-0-camel-sane.out'
# input_file = 'GroundTruth/stdout'
output_path = 'figures/' + jobname + '/'
xaxes = [0, runtime]
maxOEs = 10

executorsFigureFlag = True

colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
lineType = ['--', '-.', ':']
markerType = ['+', 'o', 'd', '*', 'x']

schema = ['Time', 'Container Id', 'Arrival Rate', 'Service Rate', 'Window Estimate Delay']
containerArrivalRate = {}
containerServiceRate = {}
containerWindowDelay = {}
containerRealWindowDelay = {}
containerResidual = {}
containerArrivalRateT = {}
containerServiceRateT = {}  
containerLongtermDelay = {}
containerLongtermDelayT = {}
containerWindowDelayT = {}
containerRealWindowDelayT = {}
containerResidualT = {}
overallWindowDelay = []
overallRealWindowDelay = []
overallWindowDelayT = []
overallRealWindowDelayT = []

numberOfOEs = []
numberOfOEsT = []
decisionT = []
decision = []
numberOfSevere = []
numberOfSevereT = []

totalArrivalRate = {}
userWindowSize = 2000

migrationDecisionTime = {}
migrationDeployTime = {}

scalingDecisionTime = {}
scalingDeployTime = {}


initialTime = -1
def parseContainerArrivalRate(split, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    containers = info.split(',')
    total = 0
    for container in containers:
        if(len(container)>0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            if(value == 'NaN'): value = '0'
            total += float(value) * 1000
            if(Id not in containerArrivalRate):
                containerArrivalRate[Id] = []
                containerArrivalRateT[Id] = []
            containerArrivalRate[Id] += [float(value)* 1000]
            containerArrivalRateT[Id] += [(long(time) - initialTime)/base]
            if( (long(time) - initialTime)/base not in totalArrivalRate):
                totalArrivalRate[(long(time) - initialTime)/base] = 0.0
            totalArrivalRate[(long(time) - initialTime)/base] += float(value) * 1000

def parseContainerServiceRate(split, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    containers = info.split(',')
    total = 0
    for container in containers:
        if(len(container)>0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            if(value == 'NaN'): value = '0'
            total += float(value) * 1000
            if(Id not in containerServiceRate):
                containerServiceRate[Id] = []
                containerServiceRateT[Id] = []
            containerServiceRate[Id] += [float(value)* 1000]
            containerServiceRateT[Id] += [(long(time) - initialTime)/base]

def parseContainerWindowDelay(split, base):
    global initialTime, overallWindowDelayT, overallWindowDelay
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    containers = info.split(',')
    total = 0
    for container in containers:
        if(len(container)>0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            total += float(value)
            if(Id not in containerWindowDelay):
                containerWindowDelay[Id] = []
                containerWindowDelayT[Id] = []
            containerWindowDelay[Id] += [float(value)]
            containerWindowDelayT[Id] += [(long(time) - initialTime)/base]

            if(len(overallWindowDelayT) == 0 or overallWindowDelayT[-1] < (long(time) - initialTime)/base):
                overallWindowDelayT += [(long(time) - initialTime)/base]
                overallWindowDelay += [float(value)]
            elif(overallWindowDelay[-1] < float(value)):
                overallWindowDelay[-1] = float(value)

def parseContainerLongtermDelay(split, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    containers = info.split(',')
    total = 0
    for container in containers:
        if(len(container)>0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            total += float(value)
            if(Id not in containerLongtermDelay):
                containerLongtermDelay[Id] = []
                containerLongtermDelayT[Id] = []
            value = float(value)
            if(value>1000000): value = 1000000
            elif(value<0): value = 0
            containerLongtermDelay[Id] += [float(value)]
            containerLongtermDelayT[Id] += [(long(time) - initialTime)/base]

def readContainerRealWindowDelay(Id):
    global initialTime, overallWindowDelayT, overallWindowDelay, overallRealWindowDelayT, overallRealWindowDelay, userWindowSize
    fileName = "container" + Id + ".txt"
    counter = 1
    processed = 0
    size = 0
    base = 1    
    queue = []
    total = 0;
    lastTime = -100000000
    with open(fileName) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(' ')
            if(split[0] == 'Partition'):
                split = split[1].split(',')
                partition = split[0]
                processed += 1
                time = split[2]
                time = (long(time) - initialTime)/base
                queue += [[time, float(split[1])]]
                total += float(split[1])
                while(queue[0][0] < time - userWindowSize):
                    total -= queue[0][1]
                    queue = queue[1:]
            
                
                if(lastTime <= time - 400 and len(queue) > 0):
                    if(Id not in containerRealWindowDelay):
                        containerRealWindowDelayT[Id] = []
                        containerRealWindowDelay[Id] = []
                    containerRealWindowDelayT[Id] += [time]
                    containerRealWindowDelay[Id] += [total/len(queue)]

                    
print("Reading from file:" + input_file)
counter = 0
arrived = {}

base = 1
with open(input_file) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split(' ')
        counter += 1
        if(counter % 100 == 0):
            print("Processed to line:" + str(counter))

        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Arrival' and split[5] == 'Rate:'):
            parseContainerArrivalRate(split, base)
        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Service' and split[5] == 'Rate:'):
            parseContainerServiceRate(split, base)
        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Instantaneous' and split[5] == 'Delay:'):
            parseContainerWindowDelay(split, base)
        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Longterm' and split[5] == 'Delay:'):
            parseContainerLongtermDelay(split, base)

        #Add migration marker
        if (split[0] == 'Migration!'):
            j = split.index("time:")
            src = split[j+4]
            tgt = split[j+7].rstrip()
            time = (long(split[j+1]) - initialTime)/base
            if(src not in migrationDecisionTime):
                migrationDecisionTime[src] = []
            migrationDecisionTime[src] += [time]
            if(tgt not in migrationDecisionTime):
                migrationDecisionTime[tgt] = []
            migrationDecisionTime[tgt] += [-time]
            print(src + ' !!! ' + tgt)

            decisionT += [time]
            if(split[1] == 'Scale' and split[2] == 'in'):
                decision += [-1]
            elif(split[1] == 'Scale' and split[2] == 'out'):
                decision += [1]
            else:
                decision += [0]

        if (split[0] == 'Number' and split[2] == 'severe'):
            time = int(lines[i-1].split(' ')[2])
            numberOfSevereT += [time]
            numberOfSevere += [int(split[4])]

        if (split[0] == 'Executors' and split[1] == 'stopped'):
            i = split.index('from')
            src = split[i+1]
            tgt = split[i+3].rstrip()
            print('Migration complete from ' + src + ' to ' + tgt)
            time = (long(split[4]) - initialTime)/base
            if(src not in migrationDeployTime):
                migrationDeployTime[src] = []
            migrationDeployTime[src] += [time]
            if (tgt not in migrationDeployTime):
                migrationDeployTime[tgt] = []
            migrationDeployTime[tgt] += [-time]
            if(len(numberOfOEs) == 0):
                numberOfOEs += [len(containerArrivalRate)]
                numberOfOEsT += [0]
            if(split[6] == 'scale-in'):
                numberOfOEs += [numberOfOEs[-1]]
                numberOfOEsT += [time]
                numberOfOEs += [numberOfOEs[-1] - 1]
                numberOfOEsT += [time]
            if(split[6] == 'scale-out'):
                numberOfOEs += [numberOfOEs[-1]]
                numberOfOEsT += [time]
                numberOfOEs += [numberOfOEs[-1] + 1]
                numberOfOEsT += [time]

def addMigrationLine(Id, ly):
    lines = []
    if(Id not in migrationDecisionTime):
        return lines
    for i in range(len(migrationDecisionTime[Id])):
        if(migrationDecisionTime[Id][i] > 0): #Migrate out
            X = [migrationDecisionTime[Id][i]]
            X += [migrationDecisionTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'g']]
        else:
            X = [-migrationDecisionTime[Id][i]]
            X += [-migrationDecisionTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'y']]
    if(Id not in migrationDeployTime):
        return lines
    for i in range(len(migrationDeployTime[Id])):
        if(migrationDeployTime[Id][i] > 0): #Migrate out
            X = [migrationDeployTime[Id][i]]
            X += [migrationDeployTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'b']]
        else:
            X = [-migrationDeployTime[Id][i]]
            X += [-migrationDeployTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'r']]
          
    return lines

import collections

#Parameters for figure size


if(executorsFigureFlag):
    #Arrival Rate and Processing Rate
    numberOfContainers = len(containerArrivalRate)
    index = 1
    sortedIds = sorted(containerArrivalRate)
    maxId = min(len(sortedIds), 25)
    fig = plt.figure(figsize=(45, 10 * (maxId+1)))
    for Id in sortedIds[:maxId]:
        print('Draw arrival rate: {0} {1}'.format(index, Id))
        plt.subplot(numberOfContainers, 1, index)
        index += 1
        legend = ['Arrival Rate', 'Processing Rate']
        plt.plot(containerArrivalRateT[Id], containerArrivalRate[Id],'r^-',containerServiceRateT[Id],containerServiceRate[Id],'bs-')
        lines = addMigrationLine(Id, 1000)
        for line in lines:
            plt.plot(line[0], line[1], linewidth=3.0, color=line[2])
        plt.legend(legend, loc='upper left')
        plt.xlabel('Index (100ms)')
        plt.ylabel('Rate (messages per second)')
        plt.title('Container ' + Id + ' Arrival and Service Rate')
        axes = plt.gca()
        axes.set_xlim([xaxes[0] * 10, xaxes[1] * 10])
        axes.set_yscale('log')
        # axes.set_ylim([1, 1000])
        axes.set_yticks([1, 10, 100, 1000, 10000, 100000, 1000000])
        import matplotlib.ticker as ticker
        #axes.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
        #plt.show()
        plt.grid(True)
    import os
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    plt.savefig(output_path + 'ContainerArrivalAndServiceRate.png')
    plt.close(fig)

# Utilization
if (False):
    inputFile = 'GroundTruth/1h_32_L1T10A0.5/samza-job-coordinator.log'
    utilizationT = {}
    utilization = {}
    startTime = -1
    with open(inputFile) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(' ')
            counter += 1
            if (counter % 50000 == 0):
                print("Processed to line:" + str(counter))
            if (len(split) > 6 and split[5] == 'Retrieved' and split[6] == 'Metrics:'):
                startPoint = line.find('Utilization={')
                endPoint = line.find('}', startPoint)
                ttime = split[1]
                time = sum(float(x) * 60 ** i for i, x in enumerate(reversed(ttime.split(":")))) * 10
                if (startTime == -1):
                    startTime = time
                time -= startTime
                t = line[startPoint + len('Utilization={'): endPoint].split(', ')
                for tt in t:
                    ttt = tt.split('=')
                    if (ttt[0] not in utilizationT):
                        utilizationT[ttt[0]] = []
                        utilization[ttt[0]] = []
                    utilizationT[ttt[0]] += [time]
                    utilization[ttt[0]] += [float(ttt[1])]
    #print(utilizationT)
    #print(utilization)
    numberOfContainers = len(utilization)
    index = 1
    sortedIds = sorted(utilization)
    maxId = min(len(sortedIds), 30)
    fig = plt.figure(figsize=(45, 10 * min((maxId+1), 30)))
    for Id in sortedIds[:min(maxId, 30)]:
        print('Draw utilization: {0} {1}'.format(index, Id))
        plt.subplot(numberOfContainers, 1, index)
        index += 1
        legend = ['Arrival Rate', 'Processing Rate']
        plt.plot(utilizationT[Id], utilization[Id], 'b^-')
        lines = addMigrationLine(Id, 0.3)
        for line in lines:
            plt.plot(line[0], line[1], linewidth=3.0, color=line[2])
        plt.legend(legend, loc='upper left')
        plt.xlabel('Index (100ms)')
        plt.ylabel('Utilization (percentage)')
        plt.title('Container ' + Id + ' Utilization')
        axes = plt.gca()
        axes.set_xlim([xaxes[0] * 10, xaxes[1] * 10])
        axes.set_ylim([0.00000001, 1.0])
        axes.set_yscale('log')
        import matplotlib.ticker as ticker

        # axes.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
        # plt.show()
        plt.grid(True)
    import os

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    plt.savefig(output_path + 'ContainerUtilization.png')
    plt.close(fig)

if(executorsFigureFlag):
    #Delays
    fig = plt.figure(figsize=(45, 10 * (maxId+1)))
    index = 1
    sortedIds = sorted(containerWindowDelay)
    for Id in sortedIds[:maxId]:
        print('Draw window delay: {0} {1}'.format(index, Id))
        plt.subplot(numberOfContainers, 1, index)
        index += 1
        #readContainerRealWindowDelay(Id)
        legend = ['Window Delay', #'Real Window Delay',
                   'Long Term Delay']
        #fig = plt.figure(figsize=(25,15))

        plt.plot(containerWindowDelayT[Id], containerWindowDelay[Id],'bs',
                 #containerRealWindowDelayT[Id], containerRealWindowDelay[Id], 'r^',
                 containerLongtermDelayT[Id], containerLongtermDelay[Id], 'cd')
        lines = addMigrationLine(Id, 50000)
        for line in lines:
            plt.plot(line[0], line[1], linewidth=3.0, color=line[2])
        plt.legend(legend, loc='upper left')
        plt.xlabel('Index (x100ms)')
        plt.ylabel('Delay (ms)')
        axes = plt.gca()
        axes.set_xlim(xaxes[0] * 10, xaxes[1] * 10)
        axes.set_ylim([1,100000])
        axes.set_yscale('log')
        plt.title('Container ' + Id + ' Window Delay')
        #plt.show()
        plt.grid(True)
    plt.savefig(output_path + jobname + '_ContainerWindowDelay.png')
    plt.close(fig)

# Number of OEs
print("Draw # of OEs")
if(len(numberOfOEs) == 0):
    numberOfOEs += [len(containerArrivalRate)]
    numberOfOEsT += [0]
lastTime = 1
for Id in containerArrivalRateT:
    if(lastTime < containerArrivalRateT[Id][-1]):
        lastTime = containerArrivalRateT[Id][-1]
numberOfOEs += [numberOfOEs[-1]]
numberOfOEsT += [lastTime]
#print(numberOfOEsT)
#print(numberOfOEs)


legend = ['Number of Executor']
fig = plt.figure(figsize=(16,9))
numberOfOEsT = [i / 10 for i in numberOfOEsT]
plt.plot(numberOfOEsT, numberOfOEs, 'b')

#print(decisionT)
#print(numberOfOEsT)
# Add decision marker
decisionT = [i / 10 for i in decisionT]
oeIndex = 0
for i in range(0, len(decisionT)):
    while (oeIndex < len(numberOfOEsT) and numberOfOEsT[oeIndex] < decisionT[i]):
        oeIndex += 1
    markerX = decisionT[i]
    if(oeIndex < len(numberOfOEsT)):
        markerY = numberOfOEs[oeIndex]
    else:
        markerY = numberOfOEs[oeIndex - 1]
    if(decision[i] == 0):
        markerLabel = 'y|'
    elif(decision[i] == 1):
        markerLabel = 'r|'
    else:
        markerLabel = 'g|'
    plt.plot(markerX, markerY, markerLabel, ms=30)
# Add severe number
for i in range(0, len(numberOfSevereT)):
    x = numberOfSevereT[i] / 10.0
    y = numberOfSevere[i]
    plt.plot([x, x + 1], [y, y], 'r')
    plt.plot([x, x], [y, 0], 'r')
    plt.plot([x + 1, x + 1], [y, 0], 'r')
plt.legend(legend, loc='upper left')
plt.grid(True)
axes = plt.gca()
axes.set_yticks(np.arange(0, maxOEs))
axes.set_xlim(xaxes)
axes.set_ylim([0,maxOEs + 1])
plt.xlabel('Index (s)')
plt.ylabel('# of Running OEs')
plt.title('Number of OEs')
plt.savefig(output_path + jobname + '_NumberOfOE.png')
plt.close(fig)

#Calculate avg # of OEs
sumOEs = 0
for i in range(1, len(numberOfOEsT)):
    sumOEs += numberOfOEs[i] * (numberOfOEsT[i] - numberOfOEsT[i-1])
avgOEs = sumOEs / float(numberOfOEsT[-1] - numberOfOEsT[0])
print("Avg number of OEs=" + str(avgOEs))
#Calculate scale-in scale-out load-balance ratio
numScaleIn = 0
numScaleOut = 0
numLoadBalance = 0
for i in range(0, len(decision)):
    if(decisionT[i] <= xaxes[1] and decisionT[i] >= xaxes[0]):
        if(decision[i] == 0):
            numLoadBalance += 1
        elif(decision[i] == 1):
            numScaleOut += 1
        else:
            numScaleIn += 1
print("Load-balance, Scale in, Scale out=", numLoadBalance, numScaleIn, numScaleOut)

# save stats to file
stats_logs_path = output_path + 'model_stats.txt'
with open(stats_logs_path, 'w+') as f:
    print >> f, ("Avg number of OEs=" + str(avgOEs))
    print >> f, ("Load-balance, Scale in, Scale out=", numLoadBalance, numScaleIn, numScaleOut)

#Draw total arrival rate
print("Draw total arrival rate")
arrivalRate = []
arrivalRateT = []
for x in sorted(totalArrivalRate):
    arrivalRateT += [x]
    arrivalRate += [totalArrivalRate[x]]

legend = ['Arrival Rate']
fig = plt.figure(figsize=(48,27))
arrivalRateT = [i / 10 for i in arrivalRateT]
plt.plot(arrivalRateT, arrivalRate , 'b')
plt.legend(legend, loc='upper left')
#print(arrivalRateT, arrivalRate)
plt.grid(True)
axes = plt.gca()
axes.set_xlim(xaxes)
# axes.set_yscale('log')
# axes.set_yticks([1, 10000, 100000, 500000])
axes.set_ylim([0, 500000])
plt.xlabel('Index (s)')
plt.ylabel('Rate (messages per second)')
plt.title('Total Arrival Rate')
plt.savefig(output_path + jobname + '_TotalArrivalRate.png')
plt.close(fig)

#Draw worst delay
legend = ['Window Delay', 'Real Window Delay']
fig = plt.figure(figsize=(48,27))
overallWindowDelayT = [i / 10 for i in overallWindowDelayT]
overallWindowDelay = [i / 1000 for i in overallWindowDelay]
plt.plot(overallWindowDelayT, overallWindowDelay, 'bs')#, containerRealWindowDelayT[Id], containerRealWindowDelay[Id], 'r^')
plt.legend(legend, loc='upper left')
plt.xlabel('Index (s)')
plt.ylabel('Delay (s)')
axes = plt.gca()
axes.set_xlim(xaxes)
#axes.set_ylim([0,200])    
plt.title('Overall Window Delay')
plt.grid(True)
plt.savefig(output_path + jobname + '_WorstWindowDelay.png')
plt.close(fig)



