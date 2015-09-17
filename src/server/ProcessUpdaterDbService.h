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

#pragma once
#ifndef PROCESSUPDATERDBSERVICE_H_
#define PROCESSUPDATERDBSERVICE_H_

#include <grp.h>
#include <pwd.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/function.hpp>

#include "db/generic/SingleDbInstance.h"
#include "common/error.h"
#include "common/definitions.h"
#include "common/Logger.h"
#include "common/ThreadSafeList.h"
#include "config/serverconfig.h"
#include "ws/SingleTrStateInstance.h"
#include "DrainMode.h"

extern bool stopThreads;
extern time_t stallRecords;


namespace fts3 {
namespace server {


class ProcessUpdaterDBService
{
public:

    /// Constructor
    ProcessUpdaterDBService()
    {
        messages.reserve(500);
    }

    /// Destructor
    virtual ~ProcessUpdaterDBService()
    {
    }

    /// Run the service
    void operator () ()
    {
        static unsigned int counter1 = 0;
        static unsigned int counterFailAll = 0;
        static unsigned int countReverted = 0;
        static unsigned int counterTimeoutWaiting = 0;
        static unsigned int counterCanceled = 0;
        std::vector<int> requestIDs;

        while (true)
        {
            stallRecords = time(0);
            try
            {
                if (stopThreads && messages.empty() && requestIDs.empty())
                {
                    break;
                }

                //if we drain a host, no need to check if url_copy are reporting being alive
                if (DrainMode::getInstance())
                {
                    FTS3_COMMON_LOGGER_NEWLOG(INFO)<< "Set to drain mode, no more checking url_copy for this instance!" << commit;
                    messages.clear();
                    sleep(15);
                    continue;
                }

                ThreadSafeList::get_instance().checkExpiredMsg(messages);

                if (!messages.empty())
                {
                    boost::filesystem::path p("/var/lib/fts3/");
                    boost::filesystem::space_info s = boost::filesystem::space(p);

                    if (s.free <= 0 || s.available <= 0)
                    {
                        bool updated =
                                DBSingleton::instance().getDBObjectInstance()->markAsStalled(
                                        messages, true);
                        if (updated)
                        {
                            ThreadSafeList::get_instance().deleteMsg(messages);
                            std::vector<struct message_updater>::const_iterator iter;
                            for (iter = messages.begin();
                                    iter != messages.end(); ++iter)
                            {
                                if (iter->msg_errno == 0 && (*iter).file_id > 0
                                        && std::string((*iter).job_id).length()
                                                > 0)
                                {
                                    SingleTrStateInstance::instance().sendStateMessage(
                                            (*iter).job_id, (*iter).file_id);
                                }
                            }
                        }
                    }
                    else
                    {
                        bool updated =
                                DBSingleton::instance().getDBObjectInstance()->markAsStalled(
                                        messages, false);
                        if (updated)
                        {
                            ThreadSafeList::get_instance().deleteMsg(messages);
                            std::vector<struct message_updater>::const_iterator iter;
                            for (iter = messages.begin();
                                    iter != messages.end(); ++iter)
                            {
                                if (iter->msg_errno == 0 && (*iter).file_id > 0
                                        && std::string((*iter).job_id).length()
                                                > 0)
                                {
                                    SingleTrStateInstance::instance().sendStateMessage(
                                            (*iter).job_id, (*iter).file_id);
                                }
                            }
                        }
                    }
                    messages.clear();
                }

                if (stopThreads)
                    return;

                /*also get jobs which have been canceled by the client*/
                counterCanceled++;
                if (counterCanceled == 10)
                {
                    try
                    {
                        DBSingleton::instance().getDBObjectInstance()->getCancelJob(requestIDs);
                        if (!requestIDs.empty()) /*if canceled jobs found and transfer already started, kill them*/
                        {
                            killRunningJob(requestIDs);
                            requestIDs.clear(); /*clean the list*/
                        }
                        counterCanceled = 0;
                    }
                    catch (const std::exception& e)
                    {
                        try
                        {
                            sleep(1);
                            DBSingleton::instance().getDBObjectInstance()->getCancelJob(requestIDs);
                            if (!requestIDs.empty()) /*if canceled jobs found and transfer already started, kill them*/
                            {
                                killRunningJob(requestIDs);
                                requestIDs.clear(); /*clean the list*/
                            }
                            counterCanceled = 0;
                        }
                        catch (const std::exception& e)
                        {
                            counterCanceled = 0;
                            FTS3_COMMON_LOGGER_NEWLOG(ERR)<< "Message updater thrown exception "
                            << e.what()
                            << commit;
                        }
                        catch (...)
                        {
                            counterCanceled = 0;
                            FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Message updater thrown unhandled exception" << commit;

                        }
                    }
                    catch (...)
                    {
                        try
                        {
                            sleep(1);
                            DBSingleton::instance().getDBObjectInstance()->getCancelJob(requestIDs);
                            if (!requestIDs.empty()) /*if canceled jobs found and transfer already started, kill them*/
                            {
                                killRunningJob(requestIDs);
                                requestIDs.clear(); /*clean the list*/
                            }
                            counterCanceled = 0;
                        }
                        catch (const std::exception& e)
                        {
                            counterCanceled = 0;
                            FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Message updater thrown exception "
                            << e.what()
                            << commit;
                        }
                        catch (...)
                        {
                            counterCanceled = 0;
                            FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Message updater thrown unhandled exception" << commit;
                        }
                    }
                }

                if (stopThreads)
                    return;

                /*revert to SUBMITTED if stayed in READY for too long (300 secs)*/
                countReverted++;
                if (countReverted == 300)
                {
                    DBSingleton::instance().getDBObjectInstance()->revertToSubmitted();
                    countReverted = 0;
                }

                if (stopThreads)
                    return;

                /*this routine is called periodically every 300 seconds*/
                counterTimeoutWaiting++;
                if (counterTimeoutWaiting == 300)
                {
                    std::set<std::string> canceled;
                    DBSingleton::instance().getDBObjectInstance()->cancelWaitingFiles(canceled);
                    if (!canceled.empty())
                    {
                        for (auto iterCan = canceled.begin();
                                iterCan != canceled.end(); ++iterCan)
                        {
                            SingleTrStateInstance::instance().sendStateMessage(
                                    (*iterCan), -1);
                        }
                        canceled.clear();
                    }

                    // sanity check to make sure there are no files that have all replicas in not used state
                    //DBSingleton::instance().getDBObjectInstance()->revertNotUsedFiles();

                    counterTimeoutWaiting = 0;
                }

                if (stopThreads)
                    return;

                /*force-fail stalled ACTIVE transfers*/
                counter1++;
                if (counter1 == 300)
                {
                    std::map<int, std::string> collectJobs;
                    DBSingleton::instance().getDBObjectInstance()->forceFailTransfers(collectJobs);
                    if (!collectJobs.empty())
                    {
                        std::map<int, std::string>::const_iterator iterCollectJobs;
                        for (iterCollectJobs = collectJobs.begin(); iterCollectJobs != collectJobs.end(); ++iterCollectJobs)
                        {
                            SingleTrStateInstance::instance().sendStateMessage(
                                    (*iterCollectJobs).second,
                                    (*iterCollectJobs).first);
                        }
                        collectJobs.clear();
                    }
                    counter1 = 0;
                }

                if (stopThreads)
                    return;

                /*set to fail all old queued jobs which have exceeded max queue time*/
                counterFailAll++;
                if (counterFailAll == 300)
                {
                    std::vector<std::string> jobs;
                    DBSingleton::instance().getDBObjectInstance()->setToFailOldQueuedJobs(
                            jobs);
                    if (!jobs.empty())
                    {
                        std::vector<std::string>::const_iterator iter2;
                        for (iter2 = jobs.begin(); iter2 != jobs.end(); ++iter2)
                        {
                            SingleTrStateInstance::instance().sendStateMessage((*iter2), -1);
                        }
                        jobs.clear();
                    }
                    counterFailAll = 0;
                }

                messages.clear();
            }
            catch (const std::exception& e)
            {
                FTS3_COMMON_LOGGER_NEWLOG(ERR)<< "Message updater thrown exception "
                << e.what()
                << commit;
                sleep(10);
                counter1 = 0;
                counterFailAll = 0;
                countReverted = 0;
                counterTimeoutWaiting = 0;
                counterCanceled = 0;
                messages.clear();
            }
            catch (...)
            {
                FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Message updater thrown unhandled exception" << commit;
                sleep(10);
                counter1 = 0;
                counterFailAll = 0;
                countReverted = 0;
                counterTimeoutWaiting = 0;
                counterCanceled = 0;
                messages.clear();
            }
            sleep(1);
        }
    }

private:
    std::vector<struct message_updater> messages;

    void killRunningJob(const std::vector<int>& pids)
    {
        for (auto iter = pids.begin(); iter != pids.end(); ++iter)
        {
            int pid = *iter;
            FTS3_COMMON_LOGGER_NEWLOG(INFO)<< "Canceling and killing running processes: " << pid << commit;
            kill(pid, SIGTERM);
        }
    }

};

} // end namespace server
} // end namespace fts3

#endif // PROCESSUPDATERDBSERVICE_H_