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

#include "FileTransferExecutor.h"

#include "../common/Logger.h"
#include "common/ThreadSafeList.h"
#include "common/Uri.h"
#include "cred/CredUtility.h"
#include "cred/DelegCred.h"
#include "db/generic/OptimizerSample.h"
#include "common/name_to_uid.h"
#include "common/producer_consumer_common.h"
#include "ConfigurationAssigner.h"
#include "ExecuteProcess.h"
#include "FileTransferScheduler.h"
#include "ProtocolResolver.h"
#include "services/webservice/ws/SingleTrStateInstance.h"

#include "oauth.h"

#include "UrlCopyCmd.h"

extern bool stopThreads;


namespace fts3
{

namespace server
{


FileTransferExecutor::FileTransferExecutor(TransferFile& tf,
        TransferFileHandler& tfh, bool monitoringMsg, std::string infosys,
        std::string ftsHostName, std::string proxy, std::string logDir) :
    tf(tf),
    tfh(tfh),
    monitoringMsg(monitoringMsg),
    infosys(infosys),
    ftsHostName(ftsHostName),
    proxy(proxy),
    logsDir(logDir),
    db(DBSingleton::instance().getDBObjectInstance())
{
}

FileTransferExecutor::~FileTransferExecutor()
{

}


void FileTransferExecutor::run(boost::any & ctx)
{
    if (ctx.empty()) ctx = 0;

    int & scheduled = boost::any_cast<int &>(ctx);

    //stop forking when a signal is received to avoid deadlocks
    if (tf.fileId == 0 || stopThreads) return;

    try
        {
            std::string source_hostname = tf.sourceSe;
            std::string destin_hostname = tf.destSe;
            std::string params;

            // if the pair was already checked and not scheduled skip it
            if (notScheduled.count(make_pair(source_hostname, destin_hostname))) return;

            /*check if manual config exist for this pair and vo*/

            std::vector< std::shared_ptr<ShareConfig> > cfgs;

            ConfigurationAssigner cfgAssigner(tf);
            cfgAssigner.assign(cfgs);

            FileTransferScheduler scheduler(
                tf,
                cfgs,
                tfh.getDestinations(source_hostname),
                tfh.getSources(destin_hostname),
                tfh.getDestinationsVos(source_hostname),
                tfh.getSourcesVos(destin_hostname)
            );

            int currentActive = 0;
            if (scheduler.schedule(currentActive))   /*SET TO READY STATE WHEN TRUE*/
                {
                    SeProtocolConfig protocol;
                    UrlCopyCmd cmd_builder;

                    boost::optional<ProtocolResolver::protocol> user_protocol = ProtocolResolver::getUserDefinedProtocol(tf);

                    if (user_protocol.is_initialized())
                        {
                            cmd_builder.setManualConfig(true);
                            cmd_builder.setFromProtocol(user_protocol.get());
                        }
                    else
                        {
                            cmd_builder.setManualConfig(false);
                            ProtocolResolver::protocol protocol;

                            int optimizerLevel = db->getBufferOptimization();
                            if(optimizerLevel == 2)
                                {
                                    protocol.nostreams = db->getStreamsOptimization(source_hostname, destin_hostname);
                                    if(protocol.nostreams == 0)
                                        protocol.nostreams = DEFAULT_NOSTREAMS;
                                }
                            else
                                {
                                    protocol.nostreams = DEFAULT_NOSTREAMS;
                                }

                            protocol.urlcopy_tx_to = db->getGlobalTimeout();
                            if(protocol.urlcopy_tx_to == 0)
                                {
                                    protocol.urlcopy_tx_to = DEFAULT_TIMEOUT;
                                }
                            else
                                {
                                    cmd_builder.setGlobalTimeout(protocol.urlcopy_tx_to);
                                }

                            int secPerMB = db->getSecPerMb();
                            if(secPerMB > 0)
                                {
                                    cmd_builder.setSecondsPerMB(secPerMB);
                                }

                            cmd_builder.setFromProtocol(protocol);
                        }

                    if (!cfgs.empty())
                        {
                            FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Check link config for: " << source_hostname << " -> " << destin_hostname << commit;
                            ProtocolResolver resolver(tf, cfgs);
                            bool protocolExists = resolver.resolve();
                            if (protocolExists)
                                {
                                    ProtocolResolver::protocol protocol;
                                    cmd_builder.setManualConfig(true);
                                    protocol.nostreams = resolver.getNoStreams();
                                    protocol.tcp_buffer_size = resolver.getTcpBufferSize();
                                    protocol.urlcopy_tx_to = resolver.getUrlCopyTxTo();
                                    cmd_builder.setFromProtocol(protocol);
                                }

                            if (resolver.isAuto())
                                {
                                    cmd_builder.setAutoTuned(true);
                                }
                        }

                    // Update from the transfer
                    cmd_builder.setFromTransfer(tf);

                    // OAuth credentials
                    std::string oauth_file = generateOauthConfigFile(db, tf);
                    if (!oauth_file.empty())
                        cmd_builder.setOAuthFile(oauth_file);

                    // Debug level
                    cmd_builder.setDebugLevel(db->getDebugLevel(source_hostname, destin_hostname));

                    // Show user DN
                    cmd_builder.setShowUserDn(db->getUserDnVisible());

                    // Enable monitoring
                    cmd_builder.setMonitoring(monitoringMsg);

                    // Proxy
                    if (!proxy.empty())
                        cmd_builder.setProxy(proxy);

                    // Info system
                    if (!infosys.empty())
                        cmd_builder.setInfosystem(infosys);

                    // UDT and IPv6
                    cmd_builder.setUDT(db->isProtocolUDT(source_hostname, destin_hostname));
                    if (!cmd_builder.isIPv6Explicit())
                        cmd_builder.setIPv6(db->isProtocolIPv6(source_hostname, destin_hostname));

                    // FTS3 host name
                    cmd_builder.setFTSName(ftsHostName);

                    // Pass the number of active transfers for this link to url_copy
                    cmd_builder.setNumberOfActive(currentActive);

                    // Number of retries and maximum number allowed
                    int retry_times = db->getRetryTimes(tf.jobId, tf.fileId);
                    cmd_builder.setNumberOfRetries(retry_times < 0 ? 0 : retry_times);

                    int retry_max = db->getRetry(tf.jobId);
                    cmd_builder.setMaxNumberOfRetries(retry_max < 0 ? 0 : retry_max);

                    // Log directory
                    cmd_builder.setLogDir(logsDir);

                    // Build the parameters
                    std::string params = cmd_builder.generateParameters();
                    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Transfer params: " << UrlCopyCmd::Program << " " << params << commit;
                    ExecuteProcess pr(UrlCopyCmd::Program, params);

                    /*check if fork/execvp failed, */
                    std::string forkMessage;
                    bool failed = false;

                    //check again here if the server has stopped - just in case
                    if(stopThreads)
                        return;

                    //send current state message
                    SingleTrStateInstance::instance().sendStateMessage(tf.jobId, tf.fileId);

                    scheduled += 1;

                    bool fileUpdated = false;
                    fileUpdated = db->updateTransferStatus(
                            tf.jobId, tf.fileId, 0.0, "READY", "",
                            0, 0, 0, false);
                    db->updateJobStatus(tf.jobId, "ACTIVE",0);

                    // If fileUpdated == false, the transfer was *not* updated, which means we got
                    // probably a collision with some other node
                    if (!fileUpdated) {
                        FTS3_COMMON_LOGGER_NEWLOG(WARNING)
                              << "Transfer " <<  tf.jobId << " " << tf.fileId
                              << " not updated. Probably picked by another node" << commit;
                        return;
                    }

                    // Spawn the fts_url_copy
                    if (-1 == pr.executeProcessShell(forkMessage))
                        {
                            if(forkMessage.empty())
                                {
                                    failed = true;
                                    FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Transfer failed to fork " <<  tf.jobId << "  " << tf.fileId << commit;
                                    fileUpdated = db->updateTransferStatus(
                                            tf.jobId, tf.fileId, 0.0, "FAILED",
                                            "Transfer failed to fork, check fts3server.log for more details",
                                            (int) pr.getPid(), 0, 0, false);
                                    db->updateJobStatus(tf.jobId, "FAILED", 0);
                                }
                            else
                                {
                                    failed = true;
                                    FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Transfer failed to fork " <<  forkMessage << "   " <<  tf.jobId << "  " << tf.fileId << commit;
                                    fileUpdated = db->updateTransferStatus(
                                            tf.jobId, tf.fileId, 0.0, "FAILED",
                                            "Transfer failed to fork, check fts3server.log for more details",
                                            (int) pr.getPid(), 0, 0, false);
                                    db->updateJobStatus(tf.jobId, "FAILED", 0);
                                }
                        }

                    db->updateTransferStatus(
                            tf.jobId, tf.fileId, 0.0, "ACTIVE", "",
                            (int) pr.getPid(), 0, 0, false);

                    // Send current state
                    SingleTrStateInstance::instance().sendStateMessage(tf.jobId, tf.fileId);
                    struct message_updater msg;
                    strncpy(msg.job_id, std::string(tf.jobId).c_str(), sizeof(msg.job_id));
                    msg.job_id[sizeof(msg.job_id) - 1] = '\0';
                    msg.file_id = tf.fileId;
                    msg.process_id = (int) pr.getPid();
                    msg.timestamp = milliseconds_since_epoch();

                    if(!failed) //only set watcher when the file has started
                        ThreadSafeList::get_instance().push_back(msg);
                }
            else
                {
                    notScheduled.insert(make_pair(source_hostname, destin_hostname));
                }
        }
    catch(std::exception& e)
        {
            FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Process thread exception " << e.what() <<  commit;
        }
    catch(...)
        {
            FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Process thread exception unknown" <<  commit;
        }
}

} /* namespace server */
} /* namespace fts3 */