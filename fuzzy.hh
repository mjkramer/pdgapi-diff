#pragma once

#include <string>
#include <vector>

struct FuzzyMatches {
    using match_t = std::tuple<size_t, size_t>;
    std::vector<match_t> matches;
    std::vector<size_t> rem1, rem2;

    FuzzyMatches(const std::vector<std::string>& v1,
                 const std::vector<std::string>& v2);
};
