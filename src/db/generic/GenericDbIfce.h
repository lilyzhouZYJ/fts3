/********************************************//**
 * Copyright @ Members of the EMI Collaboration, 2010.
 * See www.eu-emi.eu for details on the copyright holders.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 ***********************************************/

/**
 * @file GenericDbIfce.h
 * @brief generic database interface
 * @author Michail Salichos
 * @date 09/02/2012
 * 
 **/



#pragma once

#include <iostream>
#include <vector>
#include <map>
#include "JobStatus.h"
#include "FileTransferStatus.h"
#include "SePair.h"
#include "TransferJobSummary.h"
#include "Se.h"
#include "SeConfig.h"
#include "SeAndConfig.h"
#include "TransferJobs.h"
#include "TransferFiles.h"

/**
 * GenericDbIfce class declaration
 **/


/**
 * Map source/destination with the checksum provided
 **/
struct src_dest_checksum_tupple{
    std::string source;
    std::string destination;
    std::string checksum;
};
 
class GenericDbIfce {
public:

/**
 * Intialize database connection  by providing information from fts3config file
 **/
 
    virtual void init(std::string username, std::string password, std::string connectString) = 0;

/**
 * Submit a transfer request to be stored in the database
 **/ 
    virtual void submitPhysical(const std::string & jobId, std::vector<src_dest_checksum_tupple> src_dest_pair, const std::string & paramFTP,
                                 const std::string & DN, const std::string & cred, const std::string & voName, const std::string & myProxyServer,
                                 const std::string & delegationID, const std::string & spaceToken, const std::string & overwrite, 
                                 const std::string & sourceSpaceToken, const std::string & sourceSpaceTokenDescription, const std::string & lanConnection, int copyPinLifeTime,
                                 const std::string & failNearLine, const std::string & checksumMethod) = 0;

    virtual JobStatus* getTransferJobStatus(std::string requestID) = 0;
    
    virtual std::vector<std::string> getSiteGroupNames() = 0;

    virtual std::vector<std::string> getSiteGroupMembers(std::string GroupName) = 0;

    virtual void removeGroupMember(std::string groupName, std::string siteName) = 0;

    virtual void addGroupMember(std::string groupName, std::string siteName) = 0;    

    virtual void listRequests(std::vector<JobStatus*>& jobs, std::vector<std::string>& inGivenStates, std::string restrictToClientDN, std::string forDN, std::string VOname) = 0;
 
    virtual void getSubmittedJobs(std::vector<TransferJobs*>& jobs) = 0;
    
    virtual void getByJobId(std::vector<TransferJobs*>& jobs, std::vector<TransferFiles*>& files) = 0;
 
    
 /*
    virtual std::vector<FileTransferStatus> getFileStatus(std::string requestID, int offset, int limit) = 0;

    virtual TransferJobSummary* getTransferJobSummary(std::string requestID) = 0;

    virtual void cancel(std::vector<std::string> requestIDs) = 0;

    virtual void addChannel(std::string channelName, std::string sourceSite, std::string destSite, std::string contact, int numberOfStreams,
            int numberOfFiles, int bandwidth, int nominalThroughput, std::string state) = 0;

    virtual void setJobPriority(std::string requestID, int priority) = 0;

    virtual SePair* getSEPairName(std::string sePairName) = 0;

    virtual void dropChannel(std::string name) = 0;

    virtual void setState(std::string channelName, std::string state, std::string message) = 0;

    virtual std::vector<std::string> listChannels() = 0;

    virtual void setNumberOfStreams(std::string channelName, int numberOfStreams, std::string message) = 0;


    virtual void setNumberOfFiles(std::string channelName, int numberOfFiles, std::string message) = 0;


    virtual void setBandwidth(std::string channelName, int utilisation, std::string message) = 0;


    virtual void setContact(std::string channelName, std::string contact, std::string message) = 0;


    virtual void setNominalThroughput(std::string channelName, int nominalThroughput, std::string message) = 0;


    virtual void changeStateForHeldJob(std::string jobID, std::string state) = 0;


    virtual void changeStateForHeldJobs(std::string channelName, std::string state) = 0;


    virtual void addChannelManager(std::string channelName, std::string principal) = 0;


    virtual void removeChannelManager(std::string channelName, std::string principal) = 0;


    virtual std::vector<std::string> listChannelManagers(std::string channelName) = 0;

    virtual std::map<std::string, std::string> getChannelManager(std::string channelName, std::vector<std::string> principals) = 0;


    virtual void addVOManager(std::string VOName, std::string principal) = 0;


    virtual void removeVOManager(std::string VOName, std::string principal) = 0;


    virtual std::vector<std::string> listVOManagers(std::string VOName) = 0;

    virtual std::map<std::string, std::string> getVOManager(std::string VOName, std::vector<std::string> principals) = 0;


    virtual bool isRequestManager(std::string requestID, std::string clientDN, std::vector<std::string> principals, bool includeOwner) = 0;


    virtual void removeVOShare(std::string channelName, std::string VOName) = 0;


    virtual void setVOShare(std::string channelName, std::string VOName, int share) = 0;


    virtual bool isAgentAvailable(std::string name, std::string type) = 0;


    virtual std::string getSchemaVersion() = 0;

    virtual void setTcpBufferSize(std::string channelName, std::string bufferSize, std::string message) = 0;


    virtual void setTargetDirCheck(std::string channelName, int targetDirCheck, std::string message) = 0;


    virtual void setUrlCopyFirstTxmarkTo(std::string channelName, int urlCopyFirstTxmarkTo, std::string message) = 0;


    virtual void setChannelType(std::string channelName, std::string channelType, std::string message) = 0;


    virtual void setBlockSize(std::string channelName, std::string blockSize, std::string message) = 0;


    virtual void setHttpTimeout(std::string channelName, int httpTimeout, std::string message) = 0;


    virtual void setTransferLogLevel(std::string channelName, std::string transferLogLevel, std::string message) = 0;


    virtual void setPreparingFilesRatio(std::string channelName, double preparingFilesRatio, std::string message) = 0;


    virtual void setUrlCopyPutTimeout(std::string channelName, int urlCopyPutTimeout, std::string message) = 0;


    virtual void setUrlCopyPutDoneTimeout(std::string channelName, int urlCopyPutDoneTimeout, std::string message) = 0;


    virtual void setUrlCopyGetTimeout(std::string channelName, int urlCopyGetTimeout, std::string message) = 0;


    virtual void setUrlCopyGetDoneTimeout(std::string channelName, int urlCopyGetDoneTimeout, std::string message) = 0;


    virtual void setUrlCopyTransferTimeout(std::string channelName, int urlCopyTransferTimeout, std::string message) = 0;


    virtual void setUrlCopyTransferMarkersTimeout(std::string channelName, int urlCopyTransferMarkersTimeout, std::string message) = 0;


    virtual void setUrlCopyNoProgressTimeout(std::string channelName, int urlCopyNoProgressTimeout, std::string message) = 0;


    virtual void setUrlCopyTransferTimeoutPerMB(std::string channelName, double urlCopyTransferTimeoutPerMB, std::string message) = 0;


    virtual void setSrmCopyDirection(std::string channelName, std::string srmCopyDirection, std::string message) = 0;


    virtual void setSrmCopyTimeout(std::string channelName, int srmCopyTimeout, std::string message) = 0;


    virtual void setSrmCopyRefreshTimeout(std::string channelName, int srmCopyRefreshTimeout, std::string message) = 0;

    virtual void removeVOLimit(std::string channelUpperName, std::string voName) = 0;

    virtual void setVOLimit(std::string channelUpperName, std::string voName, int limit) = 0;

*/

    /*NEW API*/
    virtual void getSe(Se* &se, std::string seName) = 0;

    virtual void getSeCreditsInUse(int &creditsInUse, std::string srcSeName, std::string destSeName, std::string voName) = 0;

    virtual void getSiteCreditsInUse(int &creditsInUse, std::string srcSiteName, std::string destSiteName, std::string voName) = 0;

    virtual void updateFileStatus(TransferFiles* file, const std::string status) = 0;

    virtual void getAllSeInfoNoCritiria(std::vector<Se*>& se) = 0;
    
    virtual void getAllSeConfigNoCritiria(std::vector<SeConfig*>& seConfig) = 0;
    
    virtual void getAllSeAndConfigWithCritiria(std::vector<SeAndConfig*>& seAndConfig, std::string SE_NAME, std::string SHARE_ID, std::string SHARE_TYPE, std::string SHARE_VALUE) = 0;    

    virtual void addSe(std::string ENDPOINT, std::string SE_TYPE, std::string SITE, std::string NAME, std::string STATE, std::string VERSION, std::string HOST,
            std::string SE_TRANSFER_TYPE, std::string SE_TRANSFER_PROTOCOL, std::string SE_CONTROL_PROTOCOL, std::string GOCDB_ID) = 0;

    virtual void updateSe(std::string ENDPOINT, std::string SE_TYPE, std::string SITE, std::string NAME, std::string STATE, std::string VERSION, std::string HOST,
            std::string SE_TRANSFER_TYPE, std::string SE_TRANSFER_PROTOCOL, std::string SE_CONTROL_PROTOCOL, std::string GOCDB_ID) = 0;

    virtual void deleteSe(std::string NAME) = 0;

    virtual void addSeConfig( std::string SE_NAME, std::string SHARE_ID, std::string SHARE_TYPE, std::string SHARE_VALUE) = 0;

    virtual void updateSeConfig(std::string SE_NAME, std::string SHARE_ID, std::string SHARE_TYPE, std::string SHARE_VALUE) = 0;

    virtual void deleteSeConfig(std::string SE_NAME, std::string SHARE_ID, std::string SHARE_TYPE) = 0;
};



