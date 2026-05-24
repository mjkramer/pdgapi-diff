#pragma once


namespace util {

template <class... Ts> struct cases : Ts... { using Ts::operator()...; };

}
