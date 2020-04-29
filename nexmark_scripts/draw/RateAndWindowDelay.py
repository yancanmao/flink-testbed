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
MARKERSIZE=4

def draw(deltaT, jobname, warmup, runtime):
    #jobname = '4h_16_L5T5l120'
    retValue = []
    input_file = '/home/samza/workspace/flink-extended/build-target/log/flink-samza-standalonesession-0-camel-sane.out'
    # input_file ='/home/samza/workspace/flink-testbed/nexmark_scripts/draw/logs/exp0427/' + jobname + '/flink-samza-standalonesession-0-camel-sane.out'
    output_path = 'figures/' + jobname + '/'
    xaxes = [0000, runtime]
    import numpy as np
    minorstep = 60 #1 min ticks
    xticks = np.arange(xaxes[0] * 1000 /deltaT, xaxes[1] * 1000 / deltaT, minorstep * 1000 / deltaT)
    majorstep = 600
    xlabels = [''] * (runtime / minorstep)
    for x in range(0, runtime, majorstep):
        xlabels[x / minorstep] = str(x)
    #deltaT = 100
    startTime = -1
    executorsFigureFlag = True
    utilizationFlag = False
    cpuFlag = False
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
    overallWindowDelay = []
    overallWindowDelayT = []

    numberOfOEs = []
    numberOfOEsT = []
    decisionT = []
    decision = []
    numberOfSevere = []
    numberOfSevereT = []

    util = {}
    processCpuUsage = {}
    systemCpuUsage = {}

    totalArrivalRate = {}
    userWindowSize = 2000

    migrationDecisionTime = {}
    migrationDeployTime = {}

    scalingDecisionTime = {}
    scalingDeployTime = {}

    totalArrivedProcessed = {}
    totalBacklog = {}

    initialTime = -1
    def parseContainerArrivalRate(split, base, containerArrivalRate, containerArrivalRateT, totalArrivalRate):
        time = split[2]
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

    def parseContainerServiceRate(split, base, containerServiceRate, containerServiceRateT):
        time = split[2]
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

    def parseBacklog(split, base):
        time = split[2]
        info = "".join(split[6:]).replace(' ','')
        info = info.replace('{','')
        info = info.replace('}','')
        containers = info.split(',')
        total = 0
        for container in containers:
            if(len(container)>0):
                Id = container.split('=')[0]
                value = container.split('=')[1]
                if(value == 'null'):
                    value = 0
                    total = -1
                if total != -1:
                    total += int(value)
                if long(time) not in totalArrivedProcessed:
                    totalArrivedProcessed[long(time)] = {}
                totalArrivedProcessed[long(time)][split[5]] = total

    def parseContainerWindowDelay(split, base, containerWindowDelay, containerWindowDelayT, overallWindowDelay, overallWindowDelayT):
        time = split[2]
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

    def parseContainerLongtermDelay(split, base, containerLongtermDelay, containerLongtermDelayT):
        time = split[2]
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

    print("Reading from file:" + input_file)
    counter = 0
    arrived = {}
    base = 1
    currentTime = 0

    with open(input_file) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(' ')
            counter += 1
            if(counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if(split[0] == 'Model,' and initialTime == -1):
                initialTime = long(split[2])
            if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Arrival' and split[5] == 'Rate:'):
                parseContainerArrivalRate(split, base, containerArrivalRate, containerArrivalRateT, totalArrivalRate)
            if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Service' and split[5] == 'Rate:'):
                parseContainerServiceRate(split, base, containerServiceRate, containerServiceRateT)
            if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Instantaneous' and split[5] == 'Delay:'):
                parseContainerWindowDelay(split, base, containerWindowDelay, containerWindowDelayT, overallWindowDelay, overallWindowDelayT)
            if ((split[0] == 'DelayEstimateModel,' or split[0] == 'Model,') and split[4] == 'Longterm' and split[5] == 'Delay:'):
                parseContainerLongtermDelay(split, base, containerLongtermDelay, containerLongtermDelayT)

            if ((split[0] == 'DelayEstimateModel,' or split[0] == 'State,') and (split[5] == 'Arrived:' or split[5] == "Completed:")):
                parseBacklog(split, base)

            if (split[0] == 'State,' and split[5] == 'Utilizations:'):
                currentTime = long(split[2])
                info = "".join(split[6:]).replace(' ', '')
                info = info.replace('{', '')
                info = info.replace('}', '')
                oes = info.split(',')
                for oe in oes:
                    if (len(oe) > 0):
                        Id = oe.split('=')[0].zfill(6)
                        value = float(oe.split('=')[1])
                        if (Id not in util):
                            util[Id] = [[], []]
                        util[Id][0].append(long(split[2]))
                        util[Id][1].append(value)
            if (split[0] == 'Process'):
                info = "".join(split[3:]).replace(' ', '')
                info = info.replace('{', '')
                info = info.replace('}', '')
                oes = info.split(',')
                for oe in oes:
                    if (len(oe) > 0):
                        Id = oe.split('=')[0].zfill(6)
                        value = float(oe.split('=')[1])
                        if (Id not in processCpuUsage):
                            processCpuUsage[Id] = [[], []]
                        processCpuUsage[Id][0].append(currentTime)
                        processCpuUsage[Id][1].append(value)

            if (cpuFlag and split[0] == 'System' and split[1] == 'CPU'):
                info = "".join(split[3:]).replace(' ', '')
                info = info.replace('{', '')
                info = info.replace('}', '')
                oes = info.split(',')
                for oe in oes:
                    if (len(oe) > 0):
                        Id = oe.split('=')[0].zfill(6)
                        value = float(oe.split('=')[1])
                        if (Id not in systemCpuUsage):
                            systemCpuUsage[Id] = [[], []]
                        systemCpuUsage[Id][0].append(currentTime)
                        systemCpuUsage[Id][1].append(value)

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

    import numpy as np
    import matplotlib.pyplot as plt
    #plt.rcParams.update({'font.size': 15})

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
    if(executorsFigureFlag and utilizationFlag):

        #currentTime = 0
        # for i in range(0, 20):
        #     tt = 'samza-job-coordinator.log'
        #     if(i>0):
        #         tt = tt + '.' + str(i)
        #     import os
        #     if(os.path.isfile('GroundTruth/' + jobname + '/' + tt)):
        #         with open('GroundTruth/' + jobname + '/' + tt) as input:
        #             counter = 0
        #             lines = input.readlines()
        #             for line in lines:
        #                 split = line.rstrip().split(' ')
        #                 counter += 1
        #                 if (counter % 10000 == 0):
        #                     print("Processed to line:" + str(counter))
        #                 if (len(split) > 6 and split[3] == 'LatencyGuarantor' and split[5] == 'Current' and split[6] == 'time' and split[7] != 'slots'):
        #                     currentTime = long(split[7])
        #
        #                 if (len(split) > 7 and split[3] == 'LatencyGuarantor' and split[7] == 'utilization:'):
        #                     info = "".join(split[8:]).replace(' ', '')
        #                     info = info.replace('{', '')
        #                     info = info.replace('}', '')
        #                     oes = info.split(',')
        #                     for oe in oes:
        #                         if (len(oe) > 0):
        #                             Id = oe.split('=')[0].zfill(6)
        #                             value = float(oe.split('=')[1])
        #                             if (Id not in util):
        #                                 util[Id] = [[], []]
        #                             util[Id][0].append(currentTime)
        #                             util[Id][1].append(value)
        #print(util)
        numberOfContainers = len(util)
        sortedIds = sorted(util)
        groups = {}
        maxId = min(len(sortedIds), 25)
        for index in range(0, len(sortedIds)):
            if(index / maxId not in groups):
                groups[index / maxId] = []
            groups[index / maxId].append(sortedIds[index])
        for groupId in range(0, len(groups)):
            fig = plt.figure(figsize=(45, 10 * (maxId+1)))
            index = 1
            for Id in groups[groupId]:
                print('Draw utilization {0}: {1} {2}'.format(groupId, index, Id))
                plt.subplot(numberOfContainers, 1, index)
                index += 1
                legend = ['Utilization']
                plt.plot(util[Id][0], util[Id][1],'r^', markersize=MARKERSIZE)
                lines = addMigrationLine(Id, 1000)
                for line in lines:
                    plt.plot(line[0], line[1], linewidth=1.0, color=line[2])
                plt.legend(legend, loc='upper left')
                plt.xlabel('Index (' + '1' + 's)')
                plt.ylabel('Util (messages per second)')
                plt.title('Container ' + Id + ' Utilization')
                axes = plt.gca()
                axes.set_xlim(xaxes[0] * 1000 / deltaT, xaxes[1] * 1000 / deltaT)
                axes.set_xticks(xticks)
                axes.set_xticklabels(xlabels)
                #axes.set_yscale('log')
                axes.set_ylim([0, 2])
                #axes.set_yticks([1, 10, 100, 1000, 10000, 100000])
                import matplotlib.ticker as ticker
                #axes.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
                #plt.show()
                plt.grid(True)
            import os
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            plt.savefig(output_path + 'Utilization_' + str(groupId) + '.png')
            plt.close(fig)

    if(executorsFigureFlag and cpuFlag):
        numberOfContainers = len(processCpuUsage)
        sortedIds = sorted(processCpuUsage)
        groups = {}
        maxId = min(len(sortedIds), 25)
        for index in range(0, len(sortedIds)):
            if (index / maxId not in groups):
                groups[index / maxId] = []
            groups[index / maxId].append(sortedIds[index])
        for groupId in range(0, len(groups)):
            fig = plt.figure(figsize=(45, 10 * (maxId+1)))
            index = 1
            for Id in groups[groupId]:
                print('Draw cpu usage {0}: {1} {2}'.format(groupId, index, Id))
                plt.subplot(numberOfContainers, 1, index)
                index += 1
                legend = ['Process CPU Usage', 'Utilization']
                plt.plot(processCpuUsage[Id][0], [x / 3.125 for x in processCpuUsage[Id][1]], 'r^', markersize=MARKERSIZE)
                plt.plot(util[Id][0], util[Id][1], 'bo', markersize=MARKERSIZE)
                #plt.plot(systemCpuUsage[Id][0], systemCpuUsage[Id][1], 'bo', markersize=1)
                lines = addMigrationLine(Id, 1000)
                for line in lines:
                    plt.plot(line[0], line[1], linewidth=1.0, color=line[2])

                # add warmup marker
                plt.plot([warmup*10, warmup*10], [0, 10000], 'k--', linewidth=5.0)

                plt.legend(legend, loc='upper left')
                plt.xlabel('Index (' + '1' + 's)')
                plt.ylabel('CPU Usage (Ratio)')
                plt.title('Container ' + Id + ' CPU Usage')
                axes = plt.gca()
                axes.set_xlim(xaxes[0] * 1000 / deltaT, xaxes[1] * 1000 / deltaT)
                axes.set_xticks(xticks)
                # axes.set_xticklabels(xlabels)
                # axes.set_yscale('log')
                axes.set_ylim([0, 1.0])
                axes.set_yticks(np.arange(0, 1.0, 0.05))
                # axes.set_yticks([1, 10, 100, 1000, 10000, 100000])
                import matplotlib.ticker as ticker
                # axes.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
                # plt.show()
                plt.grid(True)
            import os
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            plt.savefig(output_path + 'CPUUsage_' + str(groupId) + '.png')
            plt.close(fig)

    if(executorsFigureFlag):
        #Arrival Rate and Processing Rate
        numberOfContainers = len(containerArrivalRate)
        index = 1
        sortedIds = sorted(containerArrivalRate)
        maxId = min(len(sortedIds), 25)
        groups = {}
        maxId = min(len(sortedIds), 25)
        for index in range(0, len(sortedIds)):
            if (index / maxId not in groups):
                groups[index / maxId] = []
            groups[index / maxId].append(sortedIds[index])
        for groupId in range(0, len(groups)):
            fig = plt.figure(figsize=(45, 10 * (maxId+1)))
            index = 1
            for Id in groups[groupId]:
                print('Draw arrival rate {0}: {1} {2}'.format(groupId, index, Id))
                plt.subplot(numberOfContainers, 1, index)
                index += 1
                legend = ['Arrival Rate', 'Processing Rate', 'Service Rate Use Useful Time']
                plt.plot(containerArrivalRateT[Id], containerArrivalRate[Id],'r^', markersize = MARKERSIZE)
                plt.plot(containerServiceRateT[Id],containerServiceRate[Id],'bs', markersize = MARKERSIZE)
                #print(processCpuUsage)
                #serviceRate = [containerServiceRate[Id][x] * (util[Id][1][x]) / (processCpuUsage[Id][1][x]) if (processCpuUsage[Id][1][x]>0.0) else 0.0 for x in range(0, len(containerServiceRate[Id])) ]
                #plt.plot(containerServiceRateT[Id], serviceRate, 'gs', markersize=1)
                lines = addMigrationLine(Id, 10000)
                for line in lines:
                    plt.plot(line[0], line[1], linewidth=1.0, color=line[2])

                # add warmup marker
                plt.plot([warmup*10, warmup*10], [0, 10000], 'k--', linewidth=5.0)

                plt.legend(legend, loc='upper left')
                plt.xlabel('Index (' + '1' + 's)')
                plt.ylabel('Rate (messages per second)')
                plt.title('Container ' + Id + ' Arrival and Service Rate')
                axes = plt.gca()
                axes.set_xlim(xaxes[0] * 1000 / deltaT, xaxes[1] * 1000 / deltaT)
                axes.set_xticks(xticks)
                # axes.set_xticklabels(xlabels)
                #axes.set_yscale('log')
                axes.set_ylim([1, 10000])
                #axes.set_yticks([1, 10, 100, 1000, 10000, 100000])
                import matplotlib.ticker as ticker
                #axes.get_yaxis().set_major_formatter(ticker.ScalarFormatter())
                #plt.show()
                plt.grid(True)
            import os
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            plt.savefig(output_path + 'ContainerArrivalAndServiceRate_' + str(groupId) + '.png')
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
        import numpy as np
        import matplotlib.pyplot as plt
        #print(utilizationT)
        #print(utilization)
        numberOfContainers = len(utilization)
        index = 1
        sortedIds = sorted(utilization)
        maxId = min(len(sortedIds), 30)
        fig = plt.figure(figsize=(45, 10 * min(maxId, 30)))
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
            plt.xlabel('Index (1s)')
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
        sortedIds = sorted(containerWindowDelay)
        groups = {}
        maxId = min(len(sortedIds), 25)
        for index in range(0, len(sortedIds)):
            if (index / maxId not in groups):
                groups[index / maxId] = []
            groups[index / maxId].append(sortedIds[index])
        for groupId in range(0, len(groups)):
            fig = plt.figure(figsize=(45, 10 * (maxId+1)))
            index = 1
            for Id in groups[groupId]:
                print('Draw window delay{0}: {1} {2}'.format(groupId, index, Id))
                plt.subplot(numberOfContainers, 1, index)
                index += 1
                #readContainerRealWindowDelay(Id)
                legend = ['Window Delay', #'Real Window Delay',
                          'Long Term Delay']
                #fig = plt.figure(figsize=(25,15))

                plt.plot(containerWindowDelayT[Id], containerWindowDelay[Id],'bs', markersize=MARKERSIZE)
                #containerRealWindowDelayT[Id], containerRealWindowDelay[Id], 'r^',
                plt.plot(containerLongtermDelayT[Id], containerLongtermDelay[Id], 'cd', markersize=MARKERSIZE)
                lines = addMigrationLine(Id, 50000)
                for line in lines:
                    plt.plot(line[0], line[1], linewidth=1.0, color=line[2])

                # add warmup marker
                plt.plot([warmup*10, warmup*10], [0, 100000], 'k--', linewidth=5.0)

                plt.legend(legend, loc='upper left')
                plt.xlabel('Index (x' + '1' + 's)')
                plt.ylabel('Delay (ms)')
                axes = plt.gca()
                axes.set_xlim(xaxes[0] * 1000 / deltaT, xaxes[1] * 1000 / deltaT)
                axes.set_xticks(xticks)
                # axes.set_xticklabels(xlabels)
                axes.set_ylim([1,100000])
                axes.set_yscale('log')
                plt.title('Container ' + Id + ' Window Delay')
                #plt.show()
                plt.grid(True)
            plt.savefig(output_path + jobname + '_ContainerWindowDelay_' + str(groupId)+ '.png')
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
    numberOfOEsT = [i * deltaT / 1000.0 for i in numberOfOEsT]
    plt.plot(numberOfOEsT, numberOfOEs, 'b')

    #print(decisionT)
    #print(numberOfOEsT)
    # Add decision marker
    decisionT = [i * deltaT / 1000.0 for i in decisionT]
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
        x = numberOfSevereT[i] * deltaT / 1000.0
        y = numberOfSevere[i]
        plt.plot([x, x + 1], [y, y], 'r')
        plt.plot([x, x], [y, 0], 'r')
        plt.plot([x + 1, x + 1], [y, 0], 'r')

    # add warmup marker
    plt.plot([warmup, warmup], [0, 100], 'k--', linewidth=5.0)

    plt.legend(legend, loc='upper left')
    plt.grid(True)
    axes = plt.gca()
    maxOEs = 10
    axes.set_yticks(np.arange(0, maxOEs))
    axes.set_xlim([xaxes[0] , xaxes[1]])
    #axes.set_xticks(xticks)
    #axes.set_xticklabels(xlabels)
    axes.set_ylim([0,maxOEs + 1])
    plt.xlabel('Index (s)')
    plt.ylabel('# of Running OEs')
    plt.title('Number of OEs')
    import os
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    plt.savefig(output_path + jobname + '_NumberOfOE.png')
    plt.close(fig)

    #Calculate avg # of OEs
    sumOEs = 0
    for i in range(1, len(numberOfOEsT)):
        sumOEs += numberOfOEs[i] * (min(xaxes[1], numberOfOEsT[i]) - min(xaxes[1],numberOfOEsT[i-1]))
    avgOEs = sumOEs / float(min(xaxes[1], numberOfOEsT[-1]) - numberOfOEsT[0])
    print("Avg number of OEs=" + str(avgOEs))
    retValue += [str(avgOEs)]
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
    retValue += [numLoadBalance, numScaleIn, numScaleOut]

    # save stats to file
    # stats_logs_path = output_path + 'stats.txt'
    # with open(stats_logs_path, 'a') as f:
    #     print >> f, ("Avg number of OEs=" + str(avgOEs))
    #     print >> f, ("Load-balance, Scale in, Scale out=", numLoadBalance, numScaleIn, numScaleOut)

    #Draw total arrival rate
    print("Draw total arrival rate")
    arrivalRate = []
    arrivalRateT = []
    for x in sorted(totalArrivalRate):
        arrivalRateT += [x]
        arrivalRate += [totalArrivalRate[x]]

    legend = ['Arrival Rate']
    fig = plt.figure(figsize=(60,40))
    arrivalRateT = [i * deltaT / 1000 for i in arrivalRateT]
    plt.plot(arrivalRateT, arrivalRate , 'b')

    # add warmup marker
    plt.plot([warmup, warmup], [0, 50000], 'k--', linewidth=5.0)

    plt.legend(legend, loc='upper left')
    #print(arrivalRateT, arrivalRate)
    plt.grid(True)
    axes = plt.gca()
    axes.set_xlim(xaxes)
    axes.set_ylim([0,100000])
    plt.xlabel('Index (s)')
    plt.ylabel('Rate (messages per second)')
    plt.title('Total Arrival Rate')
    plt.savefig(output_path + jobname + '_TotalArrivalRate.png')
    plt.close(fig)

    #Draw worst delay
    legend = ['Window Delay', 'Real Window Delay']
    fig = plt.figure(figsize=(60,40))
    overallWindowDelayT = [i * deltaT / 1000 for i in overallWindowDelayT]
    overallWindowDelay = [i / 1000 for i in overallWindowDelay]

    # add warmup marker
    # plt.plot([warmup, warmup], [0, 10], 'k--', linewidth=5.0)

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

    for timestamp in totalArrivedProcessed:
        arrivedProcessed = totalArrivedProcessed[timestamp]
        if arrivedProcessed["Arrived:"] == -1 or arrivedProcessed["Completed:"] == -1:
            continue
        if timestamp > runtime*10:
            continue
        backlog = arrivedProcessed["Arrived:"] - arrivedProcessed["Completed:"]
        print(arrivedProcessed)
        if backlog < 0:
            backlog = 0
        totalBacklog[timestamp] = backlog

    # plot out backlog figure
    fig = plt.figure(figsize=(32,18))
    plt.plot(totalBacklog.keys(), totalBacklog.values())
    plt.xlabel('Index (100ms)')
    plt.ylabel('Backlog (tuples)')
    plt.title('Overall backlog')
    plt.grid(True)
    plt.savefig(output_path + jobname + '_backlog.png')

    return retValue
if __name__ == "__main__":
    jobname = sys.argv[1]
    warmup = int(sys.argv[2])
    runtime = int(sys.argv[3])
    draw(100, jobname, warmup, runtime)

