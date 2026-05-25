#include "fuzzy.hh"

#include <algorithm>
#include <cassert>
#include <ranges>
#include <set>

using namespace std;

static float distance(string_view s1, string_view s2) { return 42; }

static auto all_distances(const vector<string>& v1, const vector<string>& v2)
{
    const auto idcs1 = views::iota(size_t{0}, v1.size());
    const auto idcs2 = views::iota(size_t{0}, v2.size());
    auto to_result = [&](auto ij) {
        auto [i1, i2] = ij;
        return tuple{distance(v1[i1], v2[i2]), i1, i2};
    };
    auto results = views::cartesian_product(idcs1, idcs2) |
                   views::transform(to_result) | ranges::to<vector>();
    ranges::sort(results);
    return results;
}

FuzzyMatches::FuzzyMatches(const vector<string>& v1, const vector<string>& v2)
{
    auto unused1 = views::iota(size_t{0}, v1.size()) | ranges::to<set>();
    auto unused2 = views::iota(size_t{0}, v2.size()) | ranges::to<set>();

    const auto results = all_distances(v1, v2);
    for (const auto [distance, i1, i2] : results) {
        if ((not unused1.contains(i1)) or (not unused2.contains(i2)))
            continue;

        matches.push_back({i1, i2});
        unused1.erase(i1);
        unused2.erase(i2);

        if (unused1.size() == 0 or unused2.size() == 0)
            break;
    }

    assert(unused1.size() == 0 or unused2.size() == 0);
    for (const auto& i : unused1)
        rem1.push_back(i);
    for (const auto& i : unused2)
        rem2.push_back(i);
}
