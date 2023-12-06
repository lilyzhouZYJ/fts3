/*
 * Copyright (c) CERN 2017
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

#include <boost/test/unit_test_suite.hpp>
#include <boost/test/test_tools.hpp>
#include <queue>

#include "server/services/transfers/Scheduler.h"

using namespace fts3::server;
using namespace fts3::common;

BOOST_AUTO_TEST_SUITE(server)
BOOST_AUTO_TEST_SUITE(SchedulerTestSuite)

BOOST_AUTO_TEST_CASE(TestComputeShouldBeAllocatedSlots)
{
    std::string vo1 = "vo1";
    std::string act11 = "act11";
    std::string act12 = "act12";

    Pair pair("srcSe", "dstSe");
    int maxSlots = 100;

    std::map<Scheduler::VoName, double> voWeights;
    voWeights[vo1] = 1;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, double>> voActivityWeights;
    voActivityWeights[vo1][act11] = 0.3;
    voActivityWeights[vo1][act12] = 0.7;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueActiveCounts;
    queueActiveCounts[vo1][act11] = 0;
    queueActiveCounts[vo1][act12] = 0;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueSubmittedCounts;
    queueSubmittedCounts[vo1][act11] = 60;
    queueSubmittedCounts[vo1][act12] = 30;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> shouldBeAllocated;
    shouldBeAllocated = Scheduler::computeShouldBeSlots(
        maxSlots,
        voWeights,
        voActivityWeights,
        queueActiveCounts,
        queueSubmittedCounts);

    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act11], 60);
    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act12], 30);
}

BOOST_AUTO_TEST_CASE(TestComputeDeficitsWithEmptyQueue)
{
    std::string vo1 = "vo1";
    std::string act11 = "act11";
    std::string act12 = "act12";

    Pair pair("srcSe", "dstSe");
    int maxSlots = 100;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueActiveCounts;
    queueActiveCounts[vo1][act11] = 10;
    queueActiveCounts[vo1][act12] = 0;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueSubmittedCounts;
    queueSubmittedCounts[vo1][act11] = 50;
    queueSubmittedCounts[vo1][act12] = 0;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> queueShouldBeAllocated;
    queueShouldBeAllocated[vo1][act11] = 60;
    queueShouldBeAllocated[vo1][act12] = 0;

    Scheduler::computeDeficits(queueShouldBeAllocated, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(Scheduler::allQueueDeficits[vo1][act11], 50);
    BOOST_CHECK_EQUAL(Scheduler::allQueueDeficits[vo1][act12], 0);
}

BOOST_AUTO_TEST_CASE(TestAssignSlotsUsingDeficitPriorityQueue)
{
    std::string vo1 = "vo1";
    std::string act11 = "act11";
    std::string act12 = "act12";

    Pair pair("srcSe", "dstSe");
    int maxSlots = 100;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueActiveCounts;
    queueActiveCounts[vo1][act11] = 10;
    queueActiveCounts[vo1][act12] = 0;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueSubmittedCounts;
    queueSubmittedCounts[vo1][act11] = 50;
    queueSubmittedCounts[vo1][act12] = 10;

    Scheduler::allQueueDeficits[vo1][act11] = 50;
    Scheduler::allQueueDeficits[vo1][act12] = 10;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> assignedSlotCounts;
    assignedSlotCounts = Scheduler::assignSlotsUsingDeficitPriorityQueue(maxSlots, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(Scheduler::allQueueDeficits[vo1][act11], 50);
    BOOST_CHECK_EQUAL(Scheduler::allQueueDeficits[vo1][act12], 10);
}


BOOST_AUTO_TEST_SUITE_END()
BOOST_AUTO_TEST_SUITE_END()
