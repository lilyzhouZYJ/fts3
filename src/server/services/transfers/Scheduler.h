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

#include "db/generic/QueueId.h"
#include "db/generic/TransferFile.h"
#include "VoShares.h"
#include <queue>

#ifndef SCHEDULER_H_
#define SCHEDULER_H_
namespace fts3 {
namespace server {

class Scheduler
{
public:
    enum SchedulerAlgorithm {
        RANDOMIZED,
        DEFICIT
    };

    // Define VoName type, which is std::string, for the sake of clarity
    using VoName = std::string;

    // Define ActivityName type, which is std::string, for the sake of clarity
    using ActivityName = std::string;

    // Function pointer to the scheduler algorithm
    using SchedulerFunction = std::map<VoName, std::list<TransferFile>> (*)(std::map<Pair, int>&, std::vector<QueueId>&, int);

    // Stores deficits of queues
    static std::map<VoName, std::map<ActivityName, int>> allQueueDeficits;

    // Returns the scheduling algorithm based on the config
    static SchedulerAlgorithm getSchedulerAlgorithm();

    // Returns function pointer to the scheduler algorithm
    static SchedulerFunction getSchedulerFunction();

    // The scheduling functions below should execute the corresponding
    // scheduling algorithm, and they should return a mapping from
    // VOs to the list of TransferFiles to be scheduled by TransferService.

    /**
     * Run scheduling using weighted randomization.
     * @param slotsPerLink number of slots assigned to each link, as determined by allocator
     * @param queues All current pending transfers
     * @param availableUrlCopySlots Max number of slots available in the system
     * TODO: can remove availableUrlCopySlots
     * @return Mapping from each VO to the list of transfers to be scheduled.
     */
    static std::map<VoName, std::list<TransferFile>> doRandomizedSchedule(std::map<Pair, int> &slotsPerLink, std::vector<QueueId> &queues, int availableUrlCopySlots);

    /**
     * Run deficit-based priority queueing scheduling.
     * @param slotsPerLink number of slots assigned to each link, as determined by allocator
     * @param queues All current pending transfers
     * @param availableUrlCopySlots Max number of slots available in the system
     * TODO: can remove availableUrlCopySlots
     * @return Mapping from each VO to the list of transfers to be scheduled.
     */
    static std::map<VoName, std::list<TransferFile>> doDeficitSchedule(std::map<Pair, int> &slotsPerLink, std::vector<QueueId> &queues, int availableUrlCopySlots);

    /* Helper functions */

    /**
     * Compute the number of active transfers for each activity in each vo for the pair.
     * @param src Source node
     * @param dest Destination node
     * @param voActivityWeights Maps each VO to a mapping between each of its activities to the activity's weight
    */
    static std::map<VoName, std::map<ActivityName, long long>> computeActiveCounts(
        std::string src,
        std::string dest,
        std::map<std::string, std::map<std::string, double>> &voActivityWeights
    );

    /**
     * Compute the number of submitted transfers for each activity in each vo for the pair.
     * @param src Source node
     * @param dest Destination node
     * @param voActivityWeights Maps each VO to a mapping between each of its activities to the activity's weight
    */
    static std::map<VoName, std::map<ActivityName, long long>> computeSubmittedCounts(
        std::string src,
        std::string dest,
        std::map<std::string, std::map<std::string, double>> &voActivityWeights
    );

    /**
     * Compute the number of should-be-allocated slots.
     * @param p Pair of src-dest nodes.
     * @param maxPairSlots Max number of slots given to the pair, as determined by allocator.
     * @param voWeights Maps each VO in this pair to the VO's weight.
     * @param voActivityWeights Maps each VO to a mapping between each of its activities to the activity's weight.
     * @param queueActiveCounts Maps each VO to a mapping between each of its activities to the activity's number of active slots.
     * @param queueSubmittedCounts Maps each VO to a mapping between each of its activities to the activity's number of submitted slots.
    */
    static std::map<VoName, std::map<ActivityName, int>> computeShouldBeSlots(
        const Pair &p,
        int maxPairSlots,
        std::map<VoName, double> &voWeights,
        std::map<VoName, std::map<ActivityName, double>> &voActivityWeights,
        std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
        std::map<VoName, std::map<ActivityName, long long>> &queueSubmittedCounts
    );

    /**
     * Assign should-be-allocated slots to each VO, using Huntington-Hill algorithm.
     * @param voWeights Weight of each VO.
     * @param maxPairSlots Max number of slots to be allocated to the VOs.
     * @param queueActiveCounts Number of active transfers associated with each VO and each activity in the VO.
     * @param queueSubmittedCounts Number of submitted transfers associated with each VO and each activity in the VO.
    */
    static std::map<VoName, int> assignShouldBeSlotsToVos(
        std::map<VoName, double> &voWeights,
        int maxPairSlots,
        std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
        std::map<VoName, std::map<ActivityName, long long>> &queueSubmittedCounts
    );

    /**
     * Assign should-be-allocated slots to each activity, using Huntington-Hill algorithm.
     * @param activityWeights Weight of each activity.
     * @param voMaxSlots Max number of slots to be allocated to the activities.
     * @param activityActiveCounts Number of active transfers associated with each activity.
     * @param activitySubmittedCounts Number of submitted transfers associated with each activity.
    */
    static std::map<ActivityName, int> assignShouldBeSlotsToActivities(
        std::map<ActivityName, double> &activityWeights,
        int voMaxSlots,
        std::map<ActivityName, long long> &activityActiveCounts,
        std::map<ActivityName, long long> &activitySubmittedCounts
    );

    /**
     * Assign slots to the VOs/activities via the Huntington-Hill algorithm.
     * (Both VO and activity will be referred to as queue here, because this function will be used for both).
     * @param weights Maps each queue name to the respective weight.
     * @param maxSlots Max number of slots to be allocated.
     * @param activeAndPendingCounts Number of active or pending transfers for each queue.
    */
    static std::map<std::string, int> assignShouldBeSlotsUsingHuntingtonHill(
        std::map<std::string, double> &weights,
        int maxSlots,
        std::map<std::string, long long> &activeAndPendingCounts
    );

    /**
     * Compute the deficit for each queue in a pair. This will update Scheduler::allQueueDeficits.
     * @param queueShouldBeAllocated Number of should-be-allocated slots for each activity in each VO.
     * @param queueActiveCounts Number of active slots for each activity in each VO.
     * @param queueSubmittedCounts Number of submitted transfers associated with each VO and each activity in the VO.
    */
    static void computeDeficits(
        std::map<VoName, std::map<ActivityName, int>> &queueShouldBeAllocated,
        std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
        std::map<VoName, std::map<ActivityName, long long>>& queueSubmittedCounts
    );

    /**
     * Assign slots to each queue using the priority queue of deficits.
     * @param maxSlots Max number of slots available to this link.
     * @param deficitPq A priority queue of deficits of all queues in this link.
     * @param queueActiveCounts Number of active slots for each activity in each VO.
     * @param queueSubmittedCounts Number of submitted transfers associated with each VO and each activity in the VO.
    */
    static std::map<VoName, std::map<ActivityName, int>> assignSlotsUsingDeficit(
        int maxSlots,
        std::priority_queue<std::tuple<int, VoName, ActivityName>>& deficitPq,
        std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
        std::map<VoName, std::map<ActivityName, long long>> &queueSubmittedCounts
    );

    /**
     * Fetch TransferFiles based on the number of slots assigned to each queue.
     * @param pair The link we are currently processing.
     * @param assignedSlotCounts The number of slots assigned to a queue.
     * @param[out] scheduledFiles Mapping from each VO to the list of transfers to be scheduled.
    */
    static void getTransferFilesBasedOnSlots(
        Pair pair,
        std::map<VoName, std::map<ActivityName, int>>& assignedSlotCounts,
        std::map<VoName, std::list<TransferFile>>& scheduledFiles
    );

private:
    /**
     * Transfers in unschedulable queues must be set to fail.
     * @param[out] unschedulable    List of unschedulable transfers.
     * @param slotsPerLink          Number of slots allocated to a link.
    */
    static void failUnschedulable(const std::vector<QueueId> &unschedulable, std::map<Pair, int> &slotsPerLink);
};


} // end namespace server
} // end namespace fts3

#endif // DAEMONTOOLS_H_
