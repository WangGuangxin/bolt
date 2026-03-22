/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::functions::aggregate::test;
namespace bytedance::bolt::aggregate::test {

class CovarianceAggregationTest
    : public virtual AggregationTestBase,
      public testing::WithParamInterface<std::string> {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  void testGroupBy(const std::string& aggName, const RowVectorPtr& data) {
    auto partialAgg = fmt::format("{}(c1, c2)", aggName);
    auto sql =
        fmt::format("SELECT c0, {}(c1, c2) FROM tmp GROUP BY 1", aggName);

    testAggregations({data}, {"c0"}, {partialAgg}, sql);
  }

  void testGlobalAgg(const std::string& aggName, const RowVectorPtr& data) {
    auto partialAgg = fmt::format("{}(c1, c2)", aggName);
    auto sql = fmt::format("SELECT {}(c1, c2) FROM tmp", aggName);

    testAggregations({data}, {}, {partialAgg}, sql);
  }

  void testDistinctGroupBy(
      const std::string& aggName,
      const RowVectorPtr& data,
      const std::vector<RowVectorPtr>& expected = {},
      const std::unordered_map<std::string, std::string>& config = {}) {
    SCOPED_TRACE("Run distinct");
    auto singleAgg = fmt::format("{}(distinct c1, c2)", aggName);
    auto sql = fmt::format(
        "SELECT c0, {}(distinct c1, c2) FROM tmp GROUP BY 1", aggName);
    auto plan = PlanBuilder()
                    .values({data})
                    .singleAggregation({"c0"}, {singleAgg})
                    .planNode();
    if (expected.empty()) {
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .configs(config)
          .assertResults(sql);
    } else {
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .configs(config)
          .assertResults(expected);
    }
  }

  void testDistinctGlobalAgg(
      const std::string& aggName,
      const RowVectorPtr& data,
      const std::vector<RowVectorPtr>& expected = {},
      const std::unordered_map<std::string, std::string>& config = {}) {
    SCOPED_TRACE("Run distinct global");
    auto singleAgg = fmt::format("{}(distinct c1, c2)", aggName);
    auto sql = fmt::format("SELECT {}(distinct c1, c2) FROM tmp", aggName);
    auto plan = PlanBuilder()
                    .values({data})
                    .singleAggregation({}, {singleAgg})
                    .planNode();
    if (expected.empty()) {
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .configs(config)
          .assertResults(sql);
    } else {
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .configs(config)
          .assertResults(expected);
    }
  }
};

TEST_P(CovarianceAggregationTest, doubleNoNulls) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<double>(size, [](auto row) { return row * 0.1; }),
      makeFlatVector<double>(size, [](auto row) { return row * 0.2; }),
  });

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data);

  testGroupBy(aggName, data);

  testDistinctGlobalAgg(aggName, data);
  testDistinctGroupBy(aggName, data);
}

TEST_P(CovarianceAggregationTest, doubleSomeNulls) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<double>(
          size, [](auto row) { return row * 0.1; }, nullEvery(11)),
      makeFlatVector<double>(
          size, [](auto row) { return row * 0.2; }, nullEvery(17)),
  });

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data);
  testGroupBy(aggName, data);
  testDistinctGlobalAgg(aggName, data);
  testDistinctGroupBy(aggName, data);
}

TEST_P(CovarianceAggregationTest, floatNoNulls) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<float>(size, [](auto row) { return row * 0.1; }),
      makeFlatVector<float>(size, [](auto row) { return row * 0.2; }),
  });

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data);
  testGroupBy(aggName, data);
  testDistinctGlobalAgg(aggName, data);
  testDistinctGroupBy(aggName, data);
}

TEST_P(CovarianceAggregationTest, floatSomeNulls) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<float>(
          size, [](auto row) { return row * 0.1; }, nullEvery(11)),
      makeFlatVector<float>(
          size, [](auto row) { return row * 0.2; }, nullEvery(17)),
  });

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data);
  testGroupBy(aggName, data);
  testDistinctGlobalAgg(aggName, data);
  testDistinctGroupBy(aggName, data);
}

TEST_P(CovarianceAggregationTest, allSameValue) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<float>(size, [](auto row) { return 1; }),
      makeFlatVector<float>(size, [](auto row) { return 2; }),
  });

  createDuckDbTable({data});

  auto aggName = GetParam();
#ifdef SPARK_COMPATIBLE
  bool isInSpark = true;
#else
  bool isInSpark = false;
#endif
  if (isInSpark && (aggName == "covar_samp" || aggName == "corr")) {
    std::unordered_map<std::string, std::string> config = {
        {core::QueryConfig::kSparkLegacyStatisticalAggregate, "true"}};
    auto globalResult = std::vector<RowVectorPtr>{makeRowVector(
        {makeFlatVector<float>(1, [](auto row) { return NAN; })})};
    auto groupResult = std::vector<RowVectorPtr>{makeRowVector(
        {makeFlatVector<int32_t>(7, [](auto row) { return row; }),
         makeFlatVector<float>(7, [](auto row) { return NAN; })})};
    testDistinctGlobalAgg(aggName, data, globalResult, config);
    testDistinctGroupBy(aggName, data, groupResult, config);
  }
  testDistinctGlobalAgg(aggName, data);
  testDistinctGroupBy(aggName, data);
  testGlobalAgg(aggName, data);
  testGroupBy(aggName, data);
}

TEST_P(CovarianceAggregationTest, constantY) {
  vector_size_t size = 1'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<float>(size, [](auto /*row*/) { return 1; }),
      makeFlatVector<float>(size, [](auto row) { return row * 0.2; }),
  });

  createDuckDbTable({data});

  auto aggName = GetParam();
  testGlobalAgg(aggName, data);
  testGroupBy(aggName, data);
  testDistinctGlobalAgg(aggName, data);
  testDistinctGroupBy(aggName, data);
}

TEST_F(CovarianceAggregatesTest, covarSamp) {
  auto agg = "covar_samp";
  auto input = makeRowVector(
      {makeFlatVector<double>({1, 2, 3}), makeFlatVector<double>({1, 2, 3})});
  auto expected =
      makeRowVector({makeFlatVector<double>(std::vector<double>{1.0})});
  testCovarianceAggResult(agg, input, expected);

  input = makeRowVector(
      {makeFlatVector<double>({1, 3, 5}), makeFlatVector<double>({2, 4, 6})});
  expected = makeRowVector({makeFlatVector<double>(std::vector<double>{4.0})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 0.
  input = makeRowVector(
      {makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt}),
       makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 1.
  input = makeRowVector({makeFlatVector<double>(1), makeFlatVector<double>(1)});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 0 for legacy aggregate.
  input = makeRowVector(
      {makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt}),
       makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected, true);

  // Output NaN when m2 equals 1 for legacy aggregate.
  input = makeRowVector({makeFlatVector<double>(1), makeFlatVector<double>(1)});
  expected = makeRowVector({makeFlatVector<double>(
      std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
  testCovarianceAggResult(agg, input, expected, true);

  // Output NaN when c2 is ±inf.
  input = makeRowVector(
      {makeFlatVector<double>({22, std::numeric_limits<double>::infinity()}),
       makeFlatVector<double>({0.688, 0.225})});
  expected = makeRowVector({makeFlatVector<double>(
      std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
  testCovarianceAggResult(agg, input, expected);
  testCovarianceAggResult(agg, input, expected, true);
}

BOLT_INSTANTIATE_TEST_SUITE_P(
    CovarianceAggregationTest,
    CovarianceAggregationTest,
    testing::Values(
        "covar_samp",
        "covar_pop",
        "corr",
        "regr_intercept",
        "regr_slope",
        "regr_slope",
        "regr_count",
        "regr_avgy",
        "regr_avgx",
        "regr_sxy",
        "regr_sxx",
        "regr_syy",
        "regr_r2"));
} // namespace bytedance::bolt::aggregate::test
