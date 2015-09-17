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
#ifndef OPTIMIZERSAMPLE_H_
#define OPTIMIZERSAMPLE_H_

class OptimizerSample
{
public:
    OptimizerSample(): streamsPerFile(0), numOfFiles(0),
    bufferSize(0), goodput(0), timeout(0), throughput(0),
    avgThroughput(0)
    {}

    int streamsPerFile;
    int numOfFiles;
    int bufferSize;
    float goodput;
    int timeout;
    double throughput;
    double avgThroughput;
};

#endif // OPTIMIZERSAMPLE_H_
