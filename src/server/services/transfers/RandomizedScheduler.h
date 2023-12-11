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
#include "db/generic/QueueId.h"
#include "db/generic/TransferFile.h"
#include <queue>

#ifndef RANDOMIZED_SCHEDULER_H_
#define RANDOMIZED_SCHEDULER_H_

namespace fts3 {
namespace server {

class RandomizedScheduler : public Scheduler
{
public:
    /**
     * Run scheduling using weighted randomization.
     * @param slotsPerLink Number of slots assigned to each link, as determined by allocator
     * @param queues All current pending transfers
     * @return Mapping from each VO to the list of transfers to be scheduled.
     */
    std::map<VoName, std::list<TransferFile>> doSchedule(std::map<Pair, int> &slotsPerLink, std::vector<QueueId> &queues);
};

} // end namespace server
} // end namespace fts3

#endif