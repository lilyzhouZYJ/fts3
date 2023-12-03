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

Scheduler::SchedulerAlgorithm getSchedulerAlgorithm() {
    std::string schedulerConfig = config::ServerConfig::instance().get<std::string>("TransfersServiceSchedulingAlgorithm");
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
    switch (getSchedulerAlgorithm()) {
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

std::map<VoName, std::list<TransferFile>> Scheduler::doRandomizedSchedule(
    std::map<Pair, int> &slotsPerLink, 
    std::vector<QueueId> &queues, 
    int availableUrlCopySlots
){
    std::map<std::string, std::list<TransferFile> > scheduledFiles;
    std::vector<QueueId> unschedulable;

    // Apply VO shares at this level. Basically, if more than one VO is used the same link,
    // pick one each time according to their respective weights
    queues = applyVoShares(queues, unschedulable);
    // Fail all that are unschedulable
    failUnschedulable(unschedulable, slotsPerLink);

    if (queues.empty())
        return scheduledFiles;

    auto db = DBSingleton::instance().getDBObjectInstance();

    time_t start = time(0);
    db->getReadyTransfers(queues, scheduledFiles, slotsPerLink); // TODO: move this out of db?
    time_t end =time(0);
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "DBtime=\"TransfersService\" "
                                    << "func=\"doRandomizedSchedule\" "
                                    << "DBcall=\"getReadyTransfers\" " 
                                    << "time=\"" << end - start << "\"" 
                                    << commit;

    return scheduledFiles;
}

std::map<VoName, std::list<TransferFile>> Scheduler::doDeficitSchedule(
    std::map<Pair, int> &slotsPerLink, 
    std::vector<QueueId> &queues, 
    int availableUrlCopySlots
){
    std::map<VoName, std::list<TransferFile>> scheduledFiles;

    if (queues.empty())
        return scheduledFiles;

    // (1) For each VO, fetch activity share.
    //     We do this here because this produces a mapping between each vo and all activities in that vo,
    //     which we need for later steps. Hence this reduces redundant queries into the database.
    //     The activity share of each VO also does not depend on the pair, hence does not need to go into the loop below.
    std::map<std::string, std::map<std::string, double>> voActivityShare;
    for (auto i = queues.begin(); i != queues.end(); i++) {
        if (voActivityShare.count(i->voName) > 0) {
            // VO already exists in voActivityShare; don't need to fetch again
            continue;
        }
        // Fetch activity share for that VO
        std::map<std::string, double> activityShare = DBSingleton::instance().getDBObjectInstance()->getActivityShareForVo(i->voName);
        voActivityShare[i->voName] = activityShare;
    }

    // For each link, compute deficit of its queues and perform scheduling
    for (auto i = slotsPerLink.begin(); i != slotsPerLink.end(); i++) {
        const Pair p = i->first;
        const int maxSlots = i->second;

        // (2) Compute the number of active / submitted transfers for each activity in each vo,
        //     as well as the number of pending (submitted) transfers for each activity in each vo.
        //     We do this here because we need this for computing should-be-allocated slots.
        std::map<VoName, std::map<ActivtyName, int>> queueActiveCounts = computeActiveCounts(p.source, p.destination, voActivityShare);
        std::map<VoName, std::map<ActivtyName, int>> queueSubmittedCounts = computeSubmittedCounts(p.source, p.destination, voActivityShare);

        // (3) Compute the number of should-be-allocated slots.
        std::map<VoName, std::map<ActivtyName, int>> queueShouldBeAllocated = computeShouldBeSlots(
            p, maxSlots,
            voActivityShare,
            queueActiveCounts,
            queueSubmittedCounts);

        // (4) Compute deficit; this will update Scheduler::allQueueDeficits.
        computeDeficits(queueShouldBeAllocated, queueActiveCounts, queueSubmittedCounts);

        // (5) Store deficit into priority queue.
        std::priority_queue<std::tuple<int, VoName, ActivtyName>> deficitPq;

        for (auto j = Scheduler::allQueueDeficits.begin(); j != Scheduler::allQueueDeficits.end(); j++) {
            VoName voName = j->first;
            std::map<ActivityName, int> activityDeficits = j->second;
            for (auto k = activityDeficits.begin(); k != deficits.end(); k++) {
                ActivityName activityName = k->first;
                int deficit = k->second;

                // Only include a queue in priority queue if the queue has pending transfers.
                if (queueSubmittedCounts[voName][activityName] > 0) {
                    deficitPq.push(std::make_tuple(deficit, voName, activityName));
                }
            }
        }

        // (6) Assign available slots to queues using the priority queue.
        std::map<VoName, std::map<ActivityName, int>> assignedSlotCounts = assignSlotsUsingDeficit(
            maxSlots, deficitPq, queueActiveCounts, queueSubmittedCounts);

        // (7) Fetch TransferFiles based on the number of slots assigned to each queue.
        getTransferFilesBasedOnSlots(p, assignedSlotCounts, scheduledFiles);
    }

    return scheduledFiles;
}

void Scheduler::getTransferFilesBasedOnSlots(
    Pair pair,
    std::map<VoName, std::map<ActivityName, int>>& assignedSlotCounts,
    std::map<VoName, std::list<TransferFile>>& scheduledFiles
){
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
                                        << commit;
    }
}

std::map<VoName, std::map<ActivityName, int>> Scheduler::assignSlotsUsingDeficit(
    int maxSlots,
    std::priority_queue<std::tuple<int, VoName, ActivtyName>>& deficitPq,
    std::map<VoName, std::map<ActivityName, int>> &queueActiveCounts,
    std::map<VoName, std::map<ActivityName, int>> &queueSubmittedCounts
){
    std::map<VoName, std::map<ActivityName, int>> assignedSlotCounts;

    // (1) Compute how many slots are actually available to this link after excluding active transfers.
    int totalActiveCount = 0;
    for (auto i = queueActiveCounts.begin(); i != queueActiveCounts.end(); i++) {
        std::map<ActivityName, int> activityActiveCounts = i->second;
        for (auto j = activityActiveCounts.begin(); j != activityActiveCounts.end(); j++) {
            totalActiveCount += j->second;
        }
    }
    int availableSlots = maxSlots - totalActiveCount;

    if (availableSlots <= 0) {
        // No more available slots to assign
        return assignedSlotCounts;
    }

    // (2) Assign each of the available slot using priority queue.
    for (int i = 0; i < availableSlots; i++) {
        if (deficitPq.empty()) {
            // No more queues with pending transfers
            break;
        }

        // (i) Pop top element from priority queue.
        std::tuple<int, VoName, ActivityName> nextPqElement = deficitPq.top();
        deficitPq.pop();
        int deficit = std::get<0>(nextPqElement);
        VoName voName = std::get<1>(nextPqElement);
        ActivityName activityName = std::get<2>(nextPqElement);

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

    return assignedSlotCounts;
}

std::map<VoName, std::map<ActivityName, long long>> Scheduler::computeActiveCounts(
    std::string src,
    std::string dest,
    std::map<VoName, std::map<ActivityName, double>> &voActivityShare
){
    auto db = DBSingleton::instance().getDBObjectInstance(); // TODO: fix repeated db
    std::map<VoName, std::map<ActivityName, long long>> result;

    for (auto i = voActivityShare.begin(); i != voActivityShare.end(); i++) {
        VoName voName = i->first;
        result[voName] = db->getActiveCountForEachActivity(src, dest, vo);
    }

    return result;
}

std::map<VoName, std::map<ActivityName, long long>> Scheduler::computeSubmittedCounts(
    std::string src,
    std::string dest,
    std::map<VoName, std::map<ActivityName, double>> &voActivityShare
){
    auto db = DBSingleton::instance().getDBObjectInstance(); // TODO: fix repeated db
    std::map<VoName, std::map<ActivityName, long long>> result;

    for (auto i = voActivityShare.begin(); i != voActivityShare.end(); i++) {
        VoName voName = i->first;
        result[voName] = db->getSubmittedCountInActivity(src, dest, vo);
    }

    return result;
}

std::map<VoName, std::map<ActivityName, int>> Scheduler::computeShouldBeSlots(
    Pair &p,
    int maxPairSlots,
    std::map<VoName, std::map<ActivityName, double>> &voActivityShare,
    std::map<VoName, std::map<ActivityName, int>> &queueActiveCounts,
    std::map<VoName, std::map<ActivityName, int>> &queueSubmittedCounts
){
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Scheduler: computing should-be-allocated slots" << commit;

    std::map<VoName, std::map<ActivityName, int>> result;

    // (1) Fetch weights of all vo's in that pair
    std::vector<ShareConfig> shares = DBSingleton::instance().getDBObjectInstance()->getShareConfig(p.source, p.destination);
    std::map<VoName, double> voWeights;
    for (auto j = shares.begin(); j != shares.end(); j++) {
        voWeights[j->vo] = j->weight;
    }

    // (2) Assign slots to vo's
    std::map<VoName, int> voShouldBeSlots = assignShouldBeSlotsToVos(voWeights, maxPairSlots, queueActiveCounts, queueSubmittedCounts);

    // (3) Assign slots of each vo to its activities
    for (auto j = voShouldBeSlots.begin(); j != voShouldBeSlots.end(); j++) {
        VoName voName = j->first;
        int voMaxSlots = j->second;

        std::map<ActivityName, double> activityWeights = voActivityShare[voName];
        std::map<ActivityName, int> activityShouldBeSlots = assignShouldBeSlotsToActivities(activityWeights, voMaxSlots, queueActiveCounts, queueSubmittedCounts);

        // Add to result
        for (auto k = activityShouldBeSlots.begin(); k != activityShouldBeSlots.end(); k++) {
            ActivityName activityName = k->first;
            int activitySlots = k->second;

            result[voName][activityName] = activitySlots;
        }
    }

    return result;
}

std::map<VoName, int> Scheduler::assignShouldBeSlotsToVos(
    std::map<VoName, double> &voWeights,
    int maxPairSlots,
    std::map<VoName, std::map<ActivityName, int>> &queueActiveCounts,
    std::map<VoName, std::map<ActivityName, int>> &queueSubmittedCounts
){
    // Compute total number of active and pending transfers for each vo
    std::map<VoName, int> activeAndPendingCounts;

    for (auto i = voWeights.begin(); i != voWeights.end(); i++) {
        VoName voName = i->first;
        double voWeight = i->second;

        activeAndPendingCounts[voName] = 0;

        if (queueActiveCounts.count(voName) > 0) {
            std::map<ActivityName, int>> activityActiveCounts = queueActiveCounts[voName];
            for (auto j = activityActiveCounts.begin(); j != activityActiveCounts.end(); j++) {
                activeAndPendingCounts[voName] += j->second;
            }
        }
        if (queueSubmittedCounts.count(voName) > 0) {
            std::map<ActivityName, int>> activitySubmittedCounts = queueSubmittedCounts[voName];
            for (auto j = activitySubmittedCounts.begin(); j != activitySubmittedCounts.end(); j++) {
                activeAndPendingCounts[voName] += j->second;
            }
        }
    }

    return assignShouldBeSlotsUsingHuntingtonHill(voWeights, maxPairSlots, activeAndPendingCounts);
}

std::map<ActivityName, int> Scheduler::assignShouldBeSlotsToActivities(
    std::map<ActivityName, double> &activityWeights,
    int voMaxSlots,
    std::map<ActivityName, int> &activityActiveCounts,
    std::map<ActivityName, int> &activitySubmittedCounts
){
    // Compute total number of active and pending transfers for each activity
    std::map<ActivityName, int> activeAndPendingCounts;

    for (auto i = activityWeights.begin(); i != activityWeights.end(); i++) {
        ActivityName activityName = i->first;
        double activityWeight = i->second;

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
    std::map<std::string, int> &activeAndPendingCounts
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
    double weightSum = 0
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

            // Compute priority
            priority = pow(weight, 2) / 2.0;
            pq.push(std::make_tuple(priority, queueName));
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
    std::map<VoName, std::map<ActivityName, int>> &queueActiveCounts,
    std::map<VoName, std::map<ActivityName, int>> &queueSubmittedCounts
){
    std::map<VoName, std::map<ActivityName, int>> &deficits = Scheduler::allQueueDeficits;

    for (auto i = queueActiveCounts.begin(); i != queueActiveCounts.end(); i++) {
        VoName voName = i->first;
        std::map<ActivityName, int> activityActiveCount = i->second;

        for (auto j = activityActiveCount.begin(); j != activityActiveCount.end(); j++) {
            ActivityName activityName = j->first;
            int activeCount = j->second;

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
        }
    }
}

/**
 * Transfers in unschedulable queues must be set to fail
 */
void failUnschedulable(const std::vector<QueueId> &unschedulable, std::map<Pair, int> &slotsPerLink)
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