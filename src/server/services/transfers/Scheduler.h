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
 * The Scheduler class implements the scheduling process of TransfersService. Its main scheduling
 * functionality resides in the doSchedule() method, which takes as input a mapping from each pair
 * to the maximum number of slots allocated to that pair (as computed by the Allocator component),
 * and produces an output that maps each VO to a list of TransferFiles that will be scheduled.
 *
 * Currently, there are two scheduling algorithms provided: randomized algorithm, and deficit-based
 * priority-queueing algorithm. They are implemented in two child classes that inherit from Scheduler:
 * RandomizedScheduler and DeficitScheduler, respectively. Both child classes implement their own
 * doSchedule() that executes their respective algorithms.
 *
 * TransfersService interfaces with the Scheduler component by having a Scheduler instance as a
 * member variable. The TransfersService constructor is responsible for instantiating a Scheduler
 * object of the correct child class, based on the configuration specification for TransfersServiceSchedulingAlgorithm,
 * which can be set to either RANDOMIZED or DEFICIT.
 *
 * For future development, if a new scheduling algorithm is introduced, the operator should introduce
 * a new child class that inherits from Scheduler. The child class must implement the doSchedule() method.
 * Additionally, the operator needs to add the new algorithm to Scheduler::SchedulerAlgorithm and 
 * Scheduler::getSchedulerAlgorithm(), as well as the TransfersService constructor.
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

    Scheduler(){
    };

    /**
     * Returns the scheduling algorithm based on the config.
    */
    static Scheduler::SchedulerAlgorithm getSchedulerAlgorithm();

    /**
     * Run scheduling. This is a pure virtual function that must be implemented by child classes.
     * @param slotsPerLink Number of slots assigned to each link, as determined by allocator
     * @param queues All current pending transfers
     * @return Mapping from each VO to the list of transfers to be scheduled.
     */
    virtual std::map<VoName, std::list<TransferFile>> doSchedule(std::map<Pair, int> &slotsPerLink, std::vector<QueueId> &queues) = 0;

protected:
    /**
     * Transfers in unschedulable queues must be set to fail.
     * @param[out] unschedulable    List of unschedulable transfers.
     * @param slotsPerLink          Number of slots allocated to a link.
    */
    void failUnschedulable(const std::vector<QueueId> &unschedulable, std::map<Pair, int> &slotsPerLink);
};


} // end namespace server
} // end namespace fts3

#endif
