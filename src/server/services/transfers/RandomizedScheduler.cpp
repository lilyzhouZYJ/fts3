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

#include "RandomizedScheduler.h"
#include "common/Logger.h"
#include "config/ServerConfig.h"
#include "db/generic/SingleDbInstance.h"

using namespace fts3::common;
using namespace db;

namespace fts3 {
namespace server {

std::map<Scheduler::VoName, std::list<TransferFile>> RandomizedScheduler::doSchedule(
    std::map<Pair, int> &slotsPerLink, 
    std::vector<QueueId> &queues
){
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "RandomizedScheduler: in doSchedule (lzhou)" << commit;
    
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

} // end namespace server
} // end namespace fts3