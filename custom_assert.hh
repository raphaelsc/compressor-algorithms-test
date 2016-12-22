/*
 * Copyright (C) 2016 Raphael S. Carvalho
 *
 * This program can be distributed under the terms of the GNU GPL.
 * See the file COPYING.
 */

#pragma once

#include <boost/format.hpp>
#include <assert.h>

static inline void assert_fail(const char* assertion, const char* file, unsigned int line, const char* function) {
    auto f = (boost::format("%1%: %2%: %3%: assertion %4% failed.") % file % line % function % assertion);
    throw std::runtime_error(f.str());
}

#undef assert
#define assert(expr) \
   ((expr)                                                               \
   ? __ASSERT_VOID_CAST (0)                                             \
   : assert_fail (__STRING(expr), __FILE__, __LINE__, __ASSERT_FUNCTION))
