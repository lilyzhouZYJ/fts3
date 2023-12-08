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

/*
 * The Scheduler class implements the scheduling process of TransfersService. It takes as input
 * a mapping from each pair to the maximum number of slots allocated to that pair, as computed
 * by the Allocator component, and produces an output that maps each vo to a list of TransferFiles
 * that will be scheduled.
 *
 * Currently, this class provides two scheduling algorithms: randomized algorithm, and deficit-based
 * priority-queueing algorithm. The randomized algorithm is implemented in doRandomizedSchedule, and
 * the deficit-based algorithm is implemented in doDeficitScheduleUsingSlot. The suffix  "UsingSlot"
 * is used to indicate that this algorithm uses slots as resource constraints, and future development
 * may introduce a deficit-based algorithm using bandwidth as constraints.
 *
 * This class interfaces with TransfersService by providing a function pointer to either doRandomizedSchedule
 * or doDeficitScheduleUsingSlot, depending on the configuration specification for TransfersServiceSchedulingAlgorithm,
 * which can be set to either RANDOMIZED or DEFICIT.
 *
 * For future development, if a new scheduling algorithm is introduced, the operator should introduce
 * a new function doNewSchedule that implements the logic of the algorithm. The operator should also
 * modify the getSchedulerAlgorithm and getSchedulerFunction functions to include the new algorithm.
 * Additionally, the operator needs to modify serverconfigreader.cpp to include the new scheduler
 * algorithm as options for the configuration file.
 */

class Scheduler
{
public:
    // Define the included scheduling algorithms
    enum SchedulerAlgorithm {
        RANDOMIZED,
        DEFICIT_SLOT
    };

    // Define VoName type, which is std::string, for the sake of clarity
    using VoName = std::string;

    // Define ActivityName type, which is std::string, for the sake of clarity
    using ActivityName = std::string;

    // Function pointer to the scheduler algorithm
    using SchedulerFunction = std::map<VoName, std::list<TransferFile>> (*)(std::map<Pair, int>&, std::vector<QueueId>&);

    // Stores deficits of queues
    static std::map<VoName, std::map<ActivityName, int>> allQueueDeficitSlots;

    // Returns the scheduling algorithm based on the config
    static SchedulerAlgorithm getSchedulerAlgorithm();

    // Returns function pointer to the scheduler algorithm
    static SchedulerFunction getSchedulerFunction();

    // The scheduling functions below should execute the corresponding
    // scheduling algorithm, and they should return a mapping from
    // VOs to the list of TransferFiles to be scheduled by TransferService.

    /**
     * Run scheduling using weighted randomization.
     * @param slotsPerLink Number of slots assigned to each link, as determined by allocator
     * @param queues All current pending transfers
     * @return Mapping from each VO to the list of transfers to be scheduled.
     */
    static std::map<VoName, std::list<TransferFile>> doRandomizedSchedule(std::map<Pair, int> &slotsPerLink, std::vector<QueueId> &queues);

    /**
     * Run deficit-based priority queueing scheduling, using slots as resource constraint.
     * @param slotsPerLink number of slots assigned to each link, as determined by allocator
     * @param queues All current pending transfers
     * @return Mapping from each VO to the list of transfers to be scheduled.
     */
    static std::map<VoName, std::list<TransferFile>> doDeficitScheduleUsingSlot(std::map<Pair, int> &slotsPerLink, std::vector<QueueId> &queues);

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
     * Compute the number of should-be-allocated slots for each queue in a given pair.
     * @param maxPairSlots Max number of slots given to the pair, as determined by allocator.
     * @param voWeights Maps each VO in this pair to the VO's weight.
     * @param voActivityWeights Maps each VO to a mapping between each of its activities to the activity's weight.
     * @param queueActiveCounts Maps each VO to a mapping between each of its activities to the activity's number of active slots.
     * @param queueSubmittedCounts Maps each VO to a mapping between each of its activities to the activity's number of submitted slots.
    */
    static std::map<VoName, std::map<ActivityName, int>> computeShouldBeSlots(
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
     * Compute the deficit for each queue in a pair. This will update Scheduler::allQueueDeficitSlots.
     * @param queueShouldBeAllocated Number of should-be-allocated slots for each activity in each VO.
     * @param queueActiveCounts Number of active slots for each activity in each VO.
     * @param queueSubmittedCounts Number of submitted transfers associated with each VO and each activity in the VO.
    */
    static void computeDeficitSlots(
        std::map<VoName, std::map<ActivityName, int>> &queueShouldBeAllocated,
        std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
        std::map<VoName, std::map<ActivityName, long long>>& queueSubmittedCounts
    );

    /**
     * Assign slots to each queue using a priority queue of deficits.
     * @param maxSlots Max number of slots available to this link.
     * @param queueActiveCounts Number of active slots for each activity in each VO.
     * @param queueSubmittedCounts Number of submitted transfers associated with each VO and each activity in the VO.
    */
    static std::map<VoName, std::map<ActivityName, int>> assignSlotsUsingDeficitPriorityQueue(
        int maxSlots,
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
     * Fetch from the database the activity weights.
     * @param queues All current pending transfers
    */
    static std::map<VoName, std::map<ActivityName, double>> getActivityWeights(std::vector<QueueId> &queues);

    /**
     * Fetch from the database the VO weights. Then process "public" weights and populate
     * the vector of "unschedulabe" transfers (i.e. VO weight <= 0).
     * @param slotsPerLink Number of slots assigned to each link, as determined by allocator
     * @param queues All current pending transfers
     * @param[out] unschedulable [Output] Unschedulable transfers
     * @return A map from each pair to a map from each of the pair's VOs and the VO weights
    */
    static std::map<Pair, std::map<VoName, double>> getVoWeightsInEachPair(
        std::map<Pair, int> &slotsPerLink, 
        std::vector<QueueId> &queues,
        std::vector<QueueId> &unschedulable
    );

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
