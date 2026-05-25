#include "util.hh"

#include <sstream>

using namespace std;

namespace util {

template <typename T> string to_str(const T& v)
{
    ostringstream os;
    os << v;
    return os.str();
}

} // namespace util
