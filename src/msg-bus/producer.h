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
#ifndef PRODUCER_H
#define PRODUCER_H

#include <string>
#include "DirQ.h"
#include "messages.h"


class Producer {
private:
    std::string baseDir;
    DirQ monitoringQueue;
    DirQ statusQueue;
    DirQ stalledQueue;
    DirQ logQueue;
    DirQ deletionQueue;
    DirQ stagingQueue;

    int writeMessage(DirQ &dirqHandle, const void *buffer, size_t bufsize);

public:
    Producer(const std::string &baseDir);

    ~Producer();

    int runProducerMonitoring(const struct MessageMonitoring &msg);

    int runProducerStatus(const struct Message &msg);

    int runProducerStall(const struct MessageUpdater &msg);

    int runProducerLog(const struct MessageLog &msg);

    int runProducerDeletions(const struct MessageBringonline &msg);

    int runProducerStaging(const struct MessageBringonline &msg);
};


#endif // PRODUCER_H
