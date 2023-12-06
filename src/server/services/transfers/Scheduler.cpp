/*
 * Copyright (c) CERN 2013-2015
 *
 * Copyright (c) Members of the EMI Collaboration. 2010-2013
 *  See  http://www.eu-emi.eu/partners for details on the copyright
 *  holders.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Scheduler.h"
#include "common/Logger.h"
#include "config/ServerConfig.h"
#include "db/generic/SingleDbInstance.h"
#include <queue>

using namespace fts3::common;
using namespace db;

namespace fts3 {
namespace server {

std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> Scheduler::allQueueDeficits = std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>>();

Scheduler::SchedulerAlgorithm Scheduler::getSchedulerAlgorithm() {
    std::string schedulerConfig = config::ServerConfig::instance().get<std::string>("TransfersServiceSchedulingAlgorithm");
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: TransfersServiceSchedulingAlgorithm is " << schedulerConfig << "(lzhou)" << commit;

    if (schedulerConfig == "RANDOMIZED") {
        return Scheduler::SchedulerAlgorithm::RANDOMIZED;
    }
    else if(schedulerConfig == "DEFICIT") {
        return Scheduler::SchedulerAlgorithm::DEFICIT;
    }
    else {
        return Scheduler::SchedulerAlgorithm::RANDOMIZED;
    }
}

Scheduler::SchedulerFunction Scheduler::getSchedulerFunction() {
    Scheduler::SchedulerFunction function;
    switch (Scheduler::getSchedulerAlgorithm()) {
        case Scheduler::RANDOMIZED:
            function = &Scheduler::doRandomizedSchedule;
            break;
        case Scheduler::DEFICIT:
            function = &Scheduler::doDeficitSchedule;
            break;
        default:
            // Use randomized algorithm as default
            function = &Scheduler::doRandomizedSchedule;
            break;
    }

    return function;
}

std::map<Scheduler::VoName, std::list<TransferFile>> Scheduler::doRandomizedSchedule(
    std::map<Pair, int> &slotsPerLink, 
    std::vector<QueueId> &queues
){
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: in doRandomizedSchedule (lzhou)" << commit;
    
    std::map<VoName, std::list<TransferFile> > scheduledFiles;
    std::vector<QueueId> unschedulable;

    // Apply VO shares at this level. Basically, if more than one VO is used the same link,
    // pick one each time according to their respective weights
    queues = applyVoShares(queues, unschedulable);
    // Fail all that are unschedulable
    failUnschedulable(unschedulable, slotsPerLink);

    if (queues.empty())
        return scheduledFiles;

    time_t start = time(0);
    DBSingleton::instance().getDBObjectInstance()->getReadyTransfers(queues, scheduledFiles, slotsPerLink);
    time_t end = time(0);
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "DBtime=\"TransfersService\" "
                                    << "func=\"doRandomizedSchedule\" "
                                    << "DBcall=\"getReadyTransfers\" " 
                                    << "time=\"" << end - start << "\"" 
                                    << "(lzhou)"
                                    << commit;

    return scheduledFiles;
}

std::map<Scheduler::VoName, std::list<TransferFile>> Scheduler::doDeficitSchedule(
    std::map<Pair, int> &slotsPerLink, 
    std::vector<QueueId> &queues
){
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: in doDeficitSchedule (lzhou)" << commit;

    auto db = DBSingleton::instance().getDBObjectInstance();
    std::map<VoName, std::list<TransferFile>> scheduledFiles;

    if (queues.empty())
        return scheduledFiles;

    // (1) For each VO, fetch activity weights.
    //     We do this here because this produces a mapping between each vo and all activities in that vo,
    //     which we need for later steps. Hence this reduces redundant queries into the database.
    //     The activity weight of each VO also does not depend on the pair, hence does not need to go into the loop below.
    std::map<VoName, std::map<ActivityName, double>> voActivityWeights = getActivityWeights(queues);
    
    // For each link, compute deficit of its queues and perform scheduling
    for (auto i = slotsPerLink.begin(); i != slotsPerLink.end(); i++) {
        const Pair p = i->first;
        const int maxSlots = i->second;

        FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduling for (src=" << p.source << ", dst=" << p.destination << "), "
                                        << "maxSlots for the pair is " << maxSlots << " "
                                        << "(lzhou)" << commit;

        // (2) Fetch weights of all vo's in this pair.
        //     This is needed to compute should-be-allocated slots.
        std::map<VoName, double> voWeights;
        std::vector<ShareConfig> shares = db->getShareConfig(p.source, p.destination);
        for (auto j = shares.begin(); j != shares.end(); j++) {
            voWeights[j->vo] = j->weight;
        }

        // (3) Compute the number of active / submitted transfers for each activity in each vo,
        //     as well as the number of pending (submitted) transfers for each activity in each vo.
        //     We do this here because we need this for computing should-be-allocated slots.
        std::map<VoName, std::map<ActivityName, long long>> queueActiveCounts = computeActiveCounts(p.source, p.destination, voActivityWeights);
        std::map<VoName, std::map<ActivityName, long long>> queueSubmittedCounts = computeSubmittedCounts(p.source, p.destination, voActivityWeights);

        for (auto j = queueActiveCounts.begin(); j != queueActiveCounts.end(); j++) {
            VoName voName = j->first;
            for (auto k = j->second.begin(); k != j->second.end(); k++) {
                ActivityName activityName = k->first;
                long long activeCount = queueActiveCounts[voName][activityName];
                long long submittedCount = queueSubmittedCounts[voName][activityName];
                FTS3_COMMON_LOGGER_NEWLOG(INFO) << "queue[vo=" << voName << "]"
                                                << "[activity=" << activityName << "]: "
                                                << "active = " << activeCount << ", "
                                                << "submitted = " << submittedCount << " "
                                                << "(lzhou)" << commit;
            }
        }

        // (4) Compute the number of should-be-allocated slots.
        std::map<VoName, std::map<ActivityName, int>> queueShouldBeAllocated = computeShouldBeSlots(
            maxSlots,
            voWeights,
            voActivityWeights,
            queueActiveCounts,
            queueSubmittedCounts);

        // (5) Compute deficit; this will update allQueueDeficits.
        computeDeficits(queueShouldBeAllocated, queueActiveCounts, queueSubmittedCounts);

        // (6) Assign available slots to queues using a priority queue of deficits.
        std::map<VoName, std::map<ActivityName, int>> assignedSlotCounts = assignSlotsUsingDeficitPriorityQueue(
            maxSlots, queueActiveCounts, queueSubmittedCounts);

        // (7) Fetch TransferFiles based on the number of slots assigned to each queue.
        getTransferFilesBasedOnSlots(p, assignedSlotCounts, scheduledFiles);
    }

    return scheduledFiles;
}

std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, double>> Scheduler::getActivityWeights(std::vector<QueueId> &queues)
{
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: in getActivityWeights (lzhou)" << commit;

    std::map<VoName, std::map<ActivityName, double>> voActivityWeights;
    auto db = DBSingleton::instance().getDBObjectInstance();

    for (auto i = queues.begin(); i != queues.end(); i++) {
        if (voActivityWeights.count(i->voName) > 0) {
            // VO already exists in voActivityWeights; don't need to fetch again
            continue;
        }
        // Fetch activity weights for that VO
        std::map<ActivityName, double> activityWeights = db->getActivityShareForVo(i->voName);
        voActivityWeights[i->voName] = activityWeights;

        for (auto j = activityWeights.begin(); j != activityWeights.end(); j++) {
            ActivityName activityName = j->first;
            FTS3_COMMON_LOGGER_NEWLOG(INFO) << "activityWeights[vo=" << i->voName << "]"
                                            << "[activity=" << activityName << "]"
                                            << " = " << voActivityWeights[i->voName][activityName] << " "
                                            << "(lzhou)" << commit;
        }
    }

    return voActivityWeights;
}

void Scheduler::getTransferFilesBasedOnSlots(
    Pair pair,
    std::map<VoName, std::map<ActivityName, int>>& assignedSlotCounts,
    std::map<VoName, std::list<TransferFile>>& scheduledFiles
){
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: in getTransferFilesBasedOnSlots (lzhou)" << commit;

    auto db = DBSingleton::instance().getDBObjectInstance();

    for (auto i = assignedSlotCounts.begin(); i != assignedSlotCounts.end(); i++) {
        VoName voName = i->first;
        std::map<ActivityName, int> activitySlotCounts = i->second;

        time_t start = time(0);
        db->getTransferFilesForVo(
            pair.source,
            pair.destination,
            voName,
            activitySlotCounts,
            scheduledFiles);
        time_t end =time(0);
        FTS3_COMMON_LOGGER_NEWLOG(INFO) << "DBtime=\"TransfersService\" "
                                        << "func=\"getTransferFilesBasedOnSlots\" "
                                        << "DBcall=\"getTransferFilesForVo\" " 
                                        << "time=\"" << end - start << "\"" 
                                        << "(lzhou)"
                                        << commit;

        FTS3_COMMON_LOGGER_NEWLOG(INFO) << "scheduledFiles[vo=" << voName << "]"
                                        << " has " << scheduledFiles[voName].size() << "files "
                                        << "(lzhou)" << commit;
    }
}

std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> Scheduler::assignSlotsUsingDeficitPriorityQueue(
    int maxSlots,
    std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
    std::map<VoName, std::map<ActivityName, long long>> &queueSubmittedCounts
){
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: in assignSlotsUsingDeficitPriorityQueue (lzhou)" << commit;

    std::map<VoName, std::map<ActivityName, int>> assignedSlotCounts;

    // (1) Store deficit into priority queue.
    std::priority_queue<std::tuple<int, VoName, ActivityName>> deficitPq;

    for (auto j = Scheduler::allQueueDeficits.begin(); j != Scheduler::allQueueDeficits.end(); j++) {
        VoName voName = j->first;
        std::map<ActivityName, int> activityDeficits = j->second;
        for (auto k = activityDeficits.begin(); k != activityDeficits.end(); k++) {
            ActivityName activityName = k->first;
            int deficit = k->second;

            // Only include a queue in priority queue if the queue has pending transfers.
            if (queueSubmittedCounts[voName][activityName] > 0) {
                deficitPq.push(std::make_tuple(deficit, voName, activityName));
            }
        }
    }

    // (2) Compute how many slots are actually available to this link after excluding active transfers.
    long long totalActiveCount = 0;
    for (auto i = queueActiveCounts.begin(); i != queueActiveCounts.end(); i++) {
        std::map<ActivityName, long long> activityActiveCounts = i->second;
        for (auto j = activityActiveCounts.begin(); j != activityActiveCounts.end(); j++) {
            totalActiveCount += j->second;
        }
    }
    int availableSlots = maxSlots - totalActiveCount;
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Actual available slots (maxSlots-totalActiveSlots) = " << availableSlots << " (lzhou)" << commit;

    if (availableSlots <= 0) {
        // No more available slots to assign
        return assignedSlotCounts;
    }

    // (3) Assign each of the available slot using priority queue.
    for (int i = 0; i < availableSlots; i++) {
        if (deficitPq.empty()) {
            // No more queues with pending transfers
            FTS3_COMMON_LOGGER_NEWLOG(INFO) << "No more pending queues, exit scheduling (lzhou)" << commit;
            break;
        }

        // (i) Pop top element from priority queue.
        std::tuple<int, VoName, ActivityName> nextPqElement = deficitPq.top();
        deficitPq.pop();
        int deficit = std::get<0>(nextPqElement);
        VoName voName = std::get<1>(nextPqElement);
        ActivityName activityName = std::get<2>(nextPqElement);

        FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Assign a slot to next queue: "
                                        << "vo=" << voName << " "
                                        << "activity=" << activityName << " "
                                        << "deficit=" << deficit << " "
                                        << "(lzhou)" << commit;

        // (ii) Assign a slot to this queue.
        if (assignedSlotCounts.find(voName) == assignedSlotCounts.end() || assignedSlotCounts[voName].find(activityName) == assignedSlotCounts[voName].end()) {
            assignedSlotCounts[voName][activityName] = 0;
        }
        assignedSlotCounts[voName][activityName] += 1;

        // (iii) Only push the updated deficit back into the priority queue if there are more pending transfers.
        if (assignedSlotCounts[voName][activityName] < queueSubmittedCounts[voName][activityName]) {
            deficit -= 1;
            deficitPq.push(std::make_tuple(deficit, voName, activityName));
        }
    }

    for (auto i = assignedSlotCounts.begin(); i != assignedSlotCounts.end(); i++) {
        VoName voName = i->first;
        for (auto j = i->second.begin(); j != i->second.end(); j++) {
            ActivityName activityName = j->first;
            FTS3_COMMON_LOGGER_NEWLOG(INFO) << "assignedSlotCounts[vo=" << voName << "]"
                                            << "[activity=" << activityName << "]"
                                            << " = " << assignedSlotCounts[voName][activityName] << " "
                                            << "(lzhou)" << commit;
        }
    }

    return assignedSlotCounts;
}

std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> Scheduler::computeActiveCounts(
    std::string src,
    std::string dest,
    std::map<VoName, std::map<ActivityName, double>> &voActivityWeights
){
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: in computeActiveCounts (lzhou)" << commit;

    auto db = DBSingleton::instance().getDBObjectInstance();
    std::map<VoName, std::map<ActivityName, long long>> result;

    for (auto i = voActivityWeights.begin(); i != voActivityWeights.end(); i++) {
        VoName voName = i->first;
        result[voName] = db->getActiveCountForEachActivity(src, dest, voName);
    }

    return result;
}

std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> Scheduler::computeSubmittedCounts(
    std::string src,
    std::string dest,
    std::map<VoName, std::map<ActivityName, double>> &voActivityWeights
){
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: in computeSubmittedCounts (lzhou)" << commit;

    auto db = DBSingleton::instance().getDBObjectInstance();
    std::map<VoName, std::map<ActivityName, long long>> result;

    for (auto i = voActivityWeights.begin(); i != voActivityWeights.end(); i++) {
        VoName voName = i->first;
        result[voName] = db->getSubmittedCountInActivity(src, dest, voName);
    }

    return result;
}

std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> Scheduler::computeShouldBeSlots(
    int maxPairSlots,
    std::map<VoName, double> &voWeights,
    std::map<VoName, std::map<ActivityName, double>> &voActivityWeights,
    std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
    std::map<VoName, std::map<ActivityName, long long>> &queueSubmittedCounts
){
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: in computeShouldBeSlots (lzhou)" << commit;

    std::map<VoName, std::map<ActivityName, int>> result;

    // (1) Assign slots to vo's
    std::map<VoName, int> voShouldBeSlots = assignShouldBeSlotsToVos(voWeights, maxPairSlots, queueActiveCounts, queueSubmittedCounts);

    // (2) Assign slots of each vo to its activities
    for (auto j = voShouldBeSlots.begin(); j != voShouldBeSlots.end(); j++) {
        VoName voName = j->first;
        int voMaxSlots = j->second;

        std::map<ActivityName, int> activityShouldBeSlots = assignShouldBeSlotsToActivities(voActivityWeights[voName], voMaxSlots, queueActiveCounts[voName], queueSubmittedCounts[voName]);

        // Add to result
        for (auto k = activityShouldBeSlots.begin(); k != activityShouldBeSlots.end(); k++) {
            ActivityName activityName = k->first;
            int activitySlots = k->second;

            result[voName][activityName] = activitySlots;
            FTS3_COMMON_LOGGER_NEWLOG(INFO) << "shouldBeSlots[vo=" << voName << "]"
                                            << "[activity=" << activityName << "]"
                                            << " = " << result[voName][activityName] << " "
                                            << "(lzhou)" << commit;
        }
    }

    return result;
}

std::map<Scheduler::VoName, int> Scheduler::assignShouldBeSlotsToVos(
    std::map<VoName, double> &voWeights,
    int maxPairSlots,
    std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
    std::map<VoName, std::map<ActivityName, long long>> &queueSubmittedCounts
){
    // Compute total number of active and pending transfers for each vo
    std::map<VoName, long long> activeAndPendingCounts;

    for (auto i = voWeights.begin(); i != voWeights.end(); i++) {
        VoName voName = i->first;
        activeAndPendingCounts[voName] = 0;

        if (queueActiveCounts.count(voName) > 0) {
            std::map<ActivityName, long long> activityActiveCounts = queueActiveCounts[voName];
            for (auto j = activityActiveCounts.begin(); j != activityActiveCounts.end(); j++) {
                activeAndPendingCounts[voName] += j->second;
            }
        }
        if (queueSubmittedCounts.count(voName) > 0) {
            std::map<ActivityName, long long> activitySubmittedCounts = queueSubmittedCounts[voName];
            for (auto j = activitySubmittedCounts.begin(); j != activitySubmittedCounts.end(); j++) {
                activeAndPendingCounts[voName] += j->second;
            }
        }
    }

    return assignShouldBeSlotsUsingHuntingtonHill(voWeights, maxPairSlots, activeAndPendingCounts);
}

std::map<Scheduler::ActivityName, int> Scheduler::assignShouldBeSlotsToActivities(
    std::map<ActivityName, double> &activityWeights,
    int voMaxSlots,
    std::map<ActivityName, long long> &activityActiveCounts,
    std::map<ActivityName, long long> &activitySubmittedCounts
){
    // Compute total number of active and pending transfers for each activity
    std::map<ActivityName, long long> activeAndPendingCounts;

    for (auto i = activityWeights.begin(); i != activityWeights.end(); i++) {
        ActivityName activityName = i->first;
        activeAndPendingCounts[activityName] = 0;

        if (activityActiveCounts.count(activityName) > 0) {
            activeAndPendingCounts[activityName] += activityActiveCounts[activityName];
        }
        if (activitySubmittedCounts.count(activityName) > 0) {
            activeAndPendingCounts[activityName] += activitySubmittedCounts[activityName];
        }
    }

    return assignShouldBeSlotsUsingHuntingtonHill(activityWeights, voMaxSlots, activeAndPendingCounts);
}

std::map<std::string, int> Scheduler::assignShouldBeSlotsUsingHuntingtonHill(
    std::map<std::string, double> &weights,
    int maxSlots,
    std::map<std::string, long long> &activeAndPendingCounts
){
    std::map<std::string, int> allocation;

    // Default all queues to 0 in allocation
    for (auto i = weights.begin(); i != weights.end(); i++) {
        std::string queueName = i->first;
        allocation[queueName] = 0;
    }

    if (maxSlots == 0) {
        return allocation;
    }

    // Compute qualification threshold;
    // this step only includes non-empty queues (with either active or pending transfers).
    double weightSum = 0;
    for (auto i = activeAndPendingCounts.begin(); i != activeAndPendingCounts.end(); i++) {
        std::string queueName = i->first;
        double count = i->second;
        if (count > 0) {
            weightSum += weights[queueName];
        }
    }
    double threshold = weightSum / maxSlots;

    // Assign one slot to every queue that meets the threshold; compute A_{1}
    std::priority_queue<std::tuple<double, std::string>> pq;

    for (auto i = weights.begin(); i != weights.end(); i++) {
        std::string queueName = i->first;
        double weight = i->second;

        if (activeAndPendingCounts[queueName] > 0 && weight >= threshold) {
            allocation[queueName] = 1;
            maxSlots -= 1;

            activeAndPendingCounts[queueName] -= 1;
            if (activeAndPendingCounts[queueName] > 0) {
                // Compute priority and push to priority queue
                double priority = pow(weight, 2) / 2.0;
                pq.push(std::make_tuple(priority, queueName));
            }
        }
    }

    // Assign remaining slots:
    // only assign slot to a queue if the queue has active or pending transfers.
    for (int i = 0; i < maxSlots; i++) {
        if (pq.empty()) {
            break;
        }

        std::tuple<double, std::string> p = pq.top();
        pq.pop();
        double priority = std::get<0>(p);
        std::string queueName = std::get<1>(p);

        allocation[queueName] += 1;
        activeAndPendingCounts[queueName] -= 1;

        if (activeAndPendingCounts[queueName] > 0) {
            // Recompute priority and push back to pq
            double n = (double) allocation[queueName];
            priority *= (n-1) / (n+1);
            pq.push(std::make_tuple(priority, queueName));
        }
    }

    return allocation;
}

void Scheduler::computeDeficits(
    std::map<VoName, std::map<ActivityName, int>> &queueShouldBeAllocated,
    std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
    std::map<VoName, std::map<ActivityName, long long>> &queueSubmittedCounts
){
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: in computeDeficits (lzhou)" << commit;

    std::map<VoName, std::map<ActivityName, int>> &deficits = Scheduler::allQueueDeficits;

    for (auto i = queueActiveCounts.begin(); i != queueActiveCounts.end(); i++) {
        VoName voName = i->first;
        std::map<ActivityName, long long> activityActiveCount = i->second;

        for (auto j = activityActiveCount.begin(); j != activityActiveCount.end(); j++) {
            ActivityName activityName = j->first;
            long long activeCount = j->second;

            if (activeCount + queueSubmittedCounts[voName][activityName] == 0) {
                // Queue is empty; reset deficit to 0
                deficits[voName][activityName] = 0;
            } else {
                int shouldBeAllocatedCount = queueShouldBeAllocated[voName][activityName];
                if (deficits.find(voName) == deficits.end() || deficits[voName].find(activityName) == deficits[voName].end()) {
                    deficits[voName][activityName] = 0;
                }
                deficits[voName][activityName] += shouldBeAllocatedCount - activeCount;
            }

            FTS3_COMMON_LOGGER_NEWLOG(INFO) << "deficit[vo=" << voName << "]"
                                            << "[activity=" << activityName << "]"
                                            << " = " << deficits[voName][activityName] << " "
                                            << "(lzhou)" << commit;
        }
    }
}

void Scheduler::failUnschedulable(const std::vector<QueueId> &unschedulable, std::map<Pair, int> &slotsPerLink)
{
    Producer producer(config::ServerConfig::instance().get<std::string>("MessagingDirectory"));

    std::map<std::string, std::list<TransferFile> > voQueues;
    DBSingleton::instance().getDBObjectInstance()->getReadyTransfers(unschedulable, voQueues, slotsPerLink);

    for (auto iterList = voQueues.begin(); iterList != voQueues.end(); ++iterList) {
        const std::list<TransferFile> &transferList = iterList->second;
        for (auto iterTransfer = transferList.begin(); iterTransfer != transferList.end(); ++iterTransfer) {
            events::Message status;

            status.set_transfer_status("FAILED");
            status.set_timestamp(millisecondsSinceEpoch());
            status.set_process_id(0);
            status.set_job_id(iterTransfer->jobId);
            status.set_file_id(iterTransfer->fileId);
            status.set_source_se(iterTransfer->sourceSe);
            status.set_dest_se(iterTransfer->destSe);
            status.set_transfer_message("No share configured for this VO");
            status.set_retry(false);
            status.set_errcode(EPERM);

            producer.runProducerStatus(status);
        }
    }
}

} // end namespace server
} // end namespace fts3