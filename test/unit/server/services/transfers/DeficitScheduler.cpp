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

#include "server/services/transfers/DeficitScheduler.h"

using namespace fts3::server;
using namespace fts3::common;

BOOST_AUTO_TEST_SUITE(server)
BOOST_AUTO_TEST_SUITE(DeficitSchedulerTestSuite)

/**
 * This test checks that given sufficient pending transfers in queues, their number
 * of should-be-allocated slots will be proportional to their relative weights.
*/
BOOST_FIXTURE_TEST_CASE(TestDeficitScheduler_ComputeShouldBeAllocatedSlots_ProportionalToWeights, DeficitScheduler)
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
    queueSubmittedCounts[vo1][act11] = 100;
    queueSubmittedCounts[vo1][act12] = 100;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> shouldBeAllocated;
    shouldBeAllocated = this->computeShouldBeSlots(
        maxSlots,
        voWeights,
        voActivityWeights,
        queueActiveCounts,
        queueSubmittedCounts);

    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act11], 30);
    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act12], 70);
}

/**
 * This test checks that if the higher-weight queue is emptied of pending transfers,
 * it will not be allocated more should-be-allocated slots. Rather, its slots would
 * go to its peer queue. This makes sure that the computation of should-be-allocated 
 * slots is work-conserving.
*/
BOOST_FIXTURE_TEST_CASE(TestDeficitScheduler_ComputeShouldBeAllocatedSlots_WithEmptyQueue, DeficitScheduler)
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
    shouldBeAllocated = this->computeShouldBeSlots(
        maxSlots,
        voWeights,
        voActivityWeights,
        queueActiveCounts,
        queueSubmittedCounts);

    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act11], 60);
    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act12], 30);
}

/**
 * This test checks that the number of should-be-allocated slots include the number of
 * currently active slots in the computation.
 * 
 * E.g. if the queue has 10 active slots and 50 pending transfers, it should receive 60
 * slots instead of 50.
*/
BOOST_FIXTURE_TEST_CASE(TestDeficitScheduler_ComputeShouldBeAllocatedSlots_WithActiveSlots, DeficitScheduler)
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
    queueActiveCounts[vo1][act11] = 10;
    queueActiveCounts[vo1][act12] = 0;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueSubmittedCounts;
    queueSubmittedCounts[vo1][act11] = 50;
    queueSubmittedCounts[vo1][act12] = 0;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> shouldBeAllocated;
    shouldBeAllocated = this->computeShouldBeSlots(
        maxSlots,
        voWeights,
        voActivityWeights,
        queueActiveCounts,
        queueSubmittedCounts);

    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act11], 60);
    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act12], 0);
}

/**
 * This test checks the computation of deficit values.
*/
BOOST_FIXTURE_TEST_CASE(TestDeficitScheduler_ComputeDeficits, DeficitScheduler)
{
    std::string vo1 = "vo1";
    std::string act11 = "act11";
    std::string act12 = "act12";

    Pair pair("srcSe", "dstSe");

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueActiveCounts;
    queueActiveCounts[vo1][act11] = 10;
    queueActiveCounts[vo1][act12] = 0;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueSubmittedCounts;
    queueSubmittedCounts[vo1][act11] = 50;
    queueSubmittedCounts[vo1][act12] = 0;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> queueShouldBeAllocated;
    queueShouldBeAllocated[vo1][act11] = 60;
    queueShouldBeAllocated[vo1][act12] = 0;

    this->computeDeficitSlots(queueShouldBeAllocated, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act11], 50);
    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act12], 0);
}

/**
 * This test checks that the Scheduler will schedule based on deficit values,
 * and that the deficit value decreases as slots are assigned to a queue.
*/
BOOST_FIXTURE_TEST_CASE(TestDeficitScheduler_AssignSlotsUsingDeficitPriorityQueue, DeficitScheduler)
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

    this->allQueueDeficitSlots[vo1][act11] = 50;
    this->allQueueDeficitSlots[vo1][act12] = 10;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> assignedSlotCounts;
    assignedSlotCounts = this->assignSlotsUsingDeficitPriorityQueue(maxSlots, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act11], 0);
    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act12], 0);

    BOOST_CHECK_EQUAL(assignedSlotCounts[vo1][act11], 50);
    BOOST_CHECK_EQUAL(assignedSlotCounts[vo1][act12], 10);
}

/**
 * This test checks that the deficit value is reset to 0 when the number of slots
 * assigned reaches the number of pending transfers for a queue.
*/
BOOST_FIXTURE_TEST_CASE(TestDeficitScheduler_AssignSlotsUsingDeficitPriorityQueue_DeficitResetToZero, DeficitScheduler)
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

    this->allQueueDeficitSlots[vo1][act11] = 100; // give it a high deficit
    this->allQueueDeficitSlots[vo1][act12] = 10;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> assignedSlotCounts;
    assignedSlotCounts = this->assignSlotsUsingDeficitPriorityQueue(maxSlots, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act11], 0);
    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act12], 0);

    BOOST_CHECK_EQUAL(assignedSlotCounts[vo1][act11], 50);
    BOOST_CHECK_EQUAL(assignedSlotCounts[vo1][act12], 10);
}

/**
 * This test checks if the deficit-based algorithm will avoid starvation by prioritizing
 * lower-weight queues that have had to wait.
 * 
 * Setup:
 *  - act11 has a weight of 0.2, while act12 has a weight of 0.8. There is only 1 vo.
 *  - There are a total of 10 slots.
 * Round 1: 
 *  - act11 gets all 10 slots because act12 has no pending transfers.
 * Round 2: 
 *  - act12 now has pending transfers, but all of the 10 act11 transfers are still active.
 *    Hence no slots are assigned to act12, which accumulates deficit.
 * Round 3: 
 *  - 5 slots are freed up.
 *  - Both act11 and act12 will be allocated slots, but act11 will receive 4 slots while 
 *    act12 receives 1. This does not reflect the relative weights of the two activities
 *    but rather is a result of the deficit algorithm avoiding starvation.
*/
BOOST_FIXTURE_TEST_CASE(TestDeficitScheduler_AccumulationOfDeficitPreventsStarvation, DeficitScheduler)
{
    std::string vo1 = "vo1";
    std::string act11 = "act11";
    std::string act12 = "act12";

    Pair pair("srcSe", "dstSe");
    int maxSlots = 10;

    std::map<Scheduler::VoName, double> voWeights;
    voWeights[vo1] = 1;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, double>> voActivityWeights;
    voActivityWeights[vo1][act11] = 0.2;
    voActivityWeights[vo1][act12] = 0.8;

    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueActiveCounts;
    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, long long>> queueSubmittedCounts;
    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> shouldBeAllocated;
    std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> assignedSlotCounts;

    /* Round 1 */

    queueActiveCounts[vo1][act11] = 0;
    queueActiveCounts[vo1][act12] = 0;
    
    queueSubmittedCounts[vo1][act11] = 0;
    queueSubmittedCounts[vo1][act12] = 20;

    shouldBeAllocated = this->computeShouldBeSlots(
        maxSlots,
        voWeights,
        voActivityWeights,
        queueActiveCounts,
        queueSubmittedCounts);

    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act11], 0);
    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act12], 10);

    this->computeDeficitSlots(shouldBeAllocated, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act11], 0);
    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act12], 10);

    assignedSlotCounts = this->assignSlotsUsingDeficitPriorityQueue(maxSlots, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act11], 0);
    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act12], 0);
    BOOST_CHECK_EQUAL(assignedSlotCounts[vo1][act11], 0);
    BOOST_CHECK_EQUAL(assignedSlotCounts[vo1][act12], 10);

    /* Round 2 */

    queueActiveCounts[vo1][act11] = 0;
    queueActiveCounts[vo1][act12] = 10;

    queueSubmittedCounts[vo1][act11] = 5;
    queueSubmittedCounts[vo1][act12] = 10;

    shouldBeAllocated = this->computeShouldBeSlots(
        maxSlots,
        voWeights,
        voActivityWeights,
        queueActiveCounts,
        queueSubmittedCounts);

    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act11], 2);
    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act12], 8);

    this->computeDeficitSlots(shouldBeAllocated, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act11], 2);
    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act12], -2);

    assignedSlotCounts = this->assignSlotsUsingDeficitPriorityQueue(maxSlots, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act11], 2);
    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act12], -2);
    BOOST_CHECK_EQUAL(assignedSlotCounts[vo1][act11], 0);
    BOOST_CHECK_EQUAL(assignedSlotCounts[vo1][act12], 0);

    /* Round 3 */
    
    queueActiveCounts[vo1][act11] = 0;
    queueActiveCounts[vo1][act12] = 5;

    queueSubmittedCounts[vo1][act11] = 5;
    queueSubmittedCounts[vo1][act12] = 10;

    shouldBeAllocated = this->computeShouldBeSlots(
        maxSlots,
        voWeights,
        voActivityWeights,
        queueActiveCounts,
        queueSubmittedCounts);

    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act11], 2);
    BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act12], 8);

    this->computeDeficitSlots(shouldBeAllocated, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act11], 4);
    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act12], 1);

    assignedSlotCounts = this->assignSlotsUsingDeficitPriorityQueue(maxSlots, queueActiveCounts, queueSubmittedCounts);

    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act11], 0);
    BOOST_CHECK_EQUAL(this->allQueueDeficitSlots[vo1][act12], 0);
    BOOST_CHECK_EQUAL(assignedSlotCounts[vo1][act11], 4);
    BOOST_CHECK_EQUAL(assignedSlotCounts[vo1][act12], 1);
}

BOOST_AUTO_TEST_SUITE_END()
BOOST_AUTO_TEST_SUITE_END()
