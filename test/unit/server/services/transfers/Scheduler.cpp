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

BOOST_AUTO_TEST_CASE(TestScheduler)
{
    int tester = 0;
    BOOST_CHECK_EQUAL(tester, 0);
}

// BOOST_AUTO_TEST_CASE(TestGetSchedulerAlgorithm)
// {
//     Scheduler::SchedulerAlgorithm schedulerAlgorithm = Scheduler::getSchedulerAlgorithm();
//     BOOST_CHECK_EQUAL(schedulerAlgorithm, Scheduler::SchedulerAlgorithm::DEFICIT);
// }

BOOST_AUTO_TEST_CASE(TestSomething)
{
    // Setup
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

    // BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act11], 60);
    // BOOST_CHECK_EQUAL(shouldBeAllocated[vo1][act12], 30);
}

std::map<Scheduler::VoName, std::map<Scheduler::ActivityName, int>> Scheduler::computeShouldBeSlots(
    int maxPairSlots,
    std::map<VoName, double> &voWeights,
    std::map<VoName, std::map<ActivityName, double>> &voActivityWeights,
    std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
    std::map<VoName, std::map<ActivityName, long long>> &queueSubmittedCounts
){
    std::map<VoName, std::map<ActivityName, int>> result;

    // (1) Assign slots to vo's
    std::map<VoName, int> voShouldBeSlots = assignShouldBeSlotsToVos(voWeights, maxPairSlots, queueActiveCounts, queueSubmittedCounts);

    // (2) Assign slots of each vo to its activities
    for (auto j = voShouldBeSlots.begin(); j != voShouldBeSlots.end(); j++) {
        VoName voName = j->first;
        int voMaxSlots = j->second;

        std::map<ActivityName, int> activityShouldBeSlots = assignShouldBeSlotsToActivities(voActivityWeights[voName], voMaxSlots, queueActiveCounts[voName], queueSubmittedCounts[voName]);

        // Add to result
        for (auto k = activityShouldBeSlots.begin(); k != activityShouldBeSlots.end(); k++) {
            ActivityName activityName = k->first;
            int activitySlots = k->second;

            result[voName][activityName] = activitySlots;
        }
    }

    return result;
}

std::map<Scheduler::VoName, int> Scheduler::assignShouldBeSlotsToVos(
    std::map<VoName, double> &voWeights,
    int maxPairSlots,
    std::map<VoName, std::map<ActivityName, long long>> &queueActiveCounts,
    std::map<VoName, std::map<ActivityName, long long>> &queueSubmittedCounts
){
    // Compute total number of active and pending transfers for each vo
    std::map<VoName, long long> activeAndPendingCounts;

    for (auto i = voWeights.begin(); i != voWeights.end(); i++) {
        VoName voName = i->first;
        activeAndPendingCounts[voName] = 0;

        if (queueActiveCounts.count(voName) > 0) {
            std::map<ActivityName, long long> activityActiveCounts = queueActiveCounts[voName];
            for (auto j = activityActiveCounts.begin(); j != activityActiveCounts.end(); j++) {
                activeAndPendingCounts[voName] += j->second;
            }
        }
        if (queueSubmittedCounts.count(voName) > 0) {
            std::map<ActivityName, long long> activitySubmittedCounts = queueSubmittedCounts[voName];
            for (auto j = activitySubmittedCounts.begin(); j != activitySubmittedCounts.end(); j++) {
                activeAndPendingCounts[voName] += j->second;
            }
        }
    }

    return assignShouldBeSlotsUsingHuntingtonHill(voWeights, maxPairSlots, activeAndPendingCounts);
}

std::map<Scheduler::ActivityName, int> Scheduler::assignShouldBeSlotsToActivities(
    std::map<ActivityName, double> &activityWeights,
    int voMaxSlots,
    std::map<ActivityName, long long> &activityActiveCounts,
    std::map<ActivityName, long long> &activitySubmittedCounts
){
    // Compute total number of active and pending transfers for each activity
    std::map<ActivityName, long long> activeAndPendingCounts;

    for (auto i = activityWeights.begin(); i != activityWeights.end(); i++) {
        ActivityName activityName = i->first;
        activeAndPendingCounts[activityName] = 0;

        if (activityActiveCounts.count(activityName) > 0) {
            activeAndPendingCounts[activityName] += activityActiveCounts[activityName];
        }
        if (activitySubmittedCounts.count(activityName) > 0) {
            activeAndPendingCounts[activityName] += activitySubmittedCounts[activityName];
        }
    }

    return assignShouldBeSlotsUsingHuntingtonHill(activityWeights, voMaxSlots, activeAndPendingCounts);
}

std::map<std::string, int> Scheduler::assignShouldBeSlotsUsingHuntingtonHill(
    std::map<std::string, double> &weights,
    int maxSlots,
    std::map<std::string, long long> &activeAndPendingCounts
){
    std::map<std::string, int> allocation;

    // Default all queues to 0 in allocation
    for (auto i = weights.begin(); i != weights.end(); i++) {
        std::string queueName = i->first;
        allocation[queueName] = 0;
    }

    if (maxSlots == 0) {
        return allocation;
    }

    // Compute qualification threshold;
    // this step only includes non-empty queues (with either active or pending transfers).
    double weightSum = 0;
    for (auto i = activeAndPendingCounts.begin(); i != activeAndPendingCounts.end(); i++) {
        std::string queueName = i->first;
        double count = i->second;
        if (count > 0) {
            weightSum += weights[queueName];
        }
    }
    double threshold = weightSum / maxSlots;

    // Assign one slot to every queue that meets the threshold; compute A_{1}
    std::priority_queue<std::tuple<double, std::string>> pq;

    for (auto i = weights.begin(); i != weights.end(); i++) {
        std::string queueName = i->first;
        double weight = i->second;

        if (activeAndPendingCounts[queueName] > 0 && weight >= threshold) {
            allocation[queueName] = 1;
            maxSlots -= 1;

            // Compute priority
            double priority = pow(weight, 2) / 2.0;
            pq.push(std::make_tuple(priority, queueName));
        }
    }

    // Assign remaining slots:
    // only assign slot to a queue if the queue has active or pending transfers.
    for (int i = 0; i < maxSlots; i++) {
        if (pq.empty()) {
            break;
        }

        std::tuple<double, std::string> p = pq.top();
        pq.pop();
        double priority = std::get<0>(p);
        std::string queueName = std::get<1>(p);

        allocation[queueName] += 1;
        activeAndPendingCounts[queueName] -= 1;

        if (activeAndPendingCounts[queueName] > 0) {
            // Recompute priority and push back to pq
            double n = (double) allocation[queueName];
            priority *= (n-1) / (n+1);
            pq.push(std::make_tuple(priority, queueName));
        }
    }

    return allocation;
}

BOOST_AUTO_TEST_SUITE_END()
BOOST_AUTO_TEST_SUITE_END()
