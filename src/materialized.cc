/*
Copyright 2019 Materialize, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "materialized.h"
#include <chrono>
#include "timing.h"

/**
 * Return a created/all pair
 *
 * - .first will be the results that were returned by `CREATE SOURCES LIKE`
 * - .second will be the results that were returned by `SHOW SOURCES LIKE`
 */
std::pair<std::vector<std::string>, std::vector<std::string>>
mz::createAllSources(pqxx::connection &c, std::string from, std::string registry, std::optional<std::string> like) {
    std::vector<std::string> createdResults;
    pqxx::nontransaction w(c);
    from = c.quote(from);
    registry = c.quote(registry);
    auto realLike = c.quote(like ? like.value() : std::string("%"));
    auto pqResult = w.exec("CREATE SOURCES LIKE " + realLike + " FROM " + from + " USING SCHEMA REGISTRY " + registry);
    auto cols = pqResult.columns();
    if (cols != 1) {
        throw UnexpectedCreateSourcesResult {"Wrong number of columns: " + std::to_string(cols)};
    }
    // TODO -- assert that the column is a string.
    createdResults.reserve(pqResult.size());
    for (const auto& row: pqResult) {
        createdResults.push_back(row[0].as<std::string>());
    }

    std::vector<std::string> allSources;
    auto pqResult = w.exec("SHOW SOURCES LIKE " + realLike);
    auto cols = pqResult.columns();
    if (cols != 1) {
        throw UnexpectedCreateSourcesResult {"Wrong number of columns: " + std::to_string(cols)};
    }
    // TODO -- assert that the column is a string.
    allSources.reserve(pqResult.size());
    for (const auto& row: pqResult) {
        allSources.push_back(row[0].as<std::string>());
    }
    return std::make_pair(createdResults, allResults);
}

void mz::createMaterializedView(pqxx::connection& c, const std::string &name, const std::string &query) {
    pqxx::nontransaction w(c);
    w.exec0("DROP VIEW IF EXISTS " + name);
    w.exec0("CREATE VIEW " + name + " AS " + query);
}

mz::PeekResults mz::peekView(pqxx::connection &c, const std::string &name, const std::optional<std::string> &order,
                             std::optional<unsigned> limit) {
    std::string query = "SELECT * FROM " + name;
    if (order) {
        query += " ORDER BY " + order.value();
    }
    if (limit) {
        query += " LIMIT " + std::to_string(limit.value());
    }

    auto [time, results] = timeInvocation([&query, &c]() {
        pqxx::nontransaction w(c);
        return w.exec(query);
    });
    
    return {time, results};
}

const char *mz::UnexpectedCreateSourcesResult::what() const noexcept {
    return what_rendered.c_str();
}

mz::UnexpectedCreateSourcesResult::UnexpectedCreateSourcesResult(const std::string& explanation) {
    what_rendered = std::string {"Unexpected result from CREATE SOURCES: "} + explanation;
}
