////
// @file datatype.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <algorithm>
#include <db/datatype.h>

namespace db {

static bool compareChar(const void *x, const void *y, size_t sx, size_t sy)
{
    return strncmp(
               (const char *) x, (const char *) y, std::max<size_t>(sx, sy)) <
           0;
}
static bool equalChar(const void *x, const void *y, size_t sx, size_t sy)
{
    return strncmp(
               (const char *) x, (const char *) y, std::max<size_t>(sx, sy)) <
           0;
}
static bool copyChar(void *x, const void *y, size_t sx, size_t sy)
{
    if (sx < sy) return false;
    ::memcpy(x, y, sy);
    return true;
}

static bool compareInt(const void *x, const void *y, size_t sx, size_t sy)
{
    return *(int *) x < *(int *) y; //(ptrdiff_t) x < (ptrdiff_t) y;
}
static bool equalInt(const void *x, const void *y, size_t sx, size_t sy)
{
    return *(int *) x == *(int *) y; //(ptrdiff_t) x < (ptrdiff_t) y;
}
static bool copyInt(void *x, const void *y, size_t sx, size_t sy)
{
    ::memcpy(x, y, sy);
    return true;
}

DataType *findDataType(const char *name)
{
    static DataType gdatatype[] = {
        {"CHAR", 65535, compareChar, equalChar, copyChar},     // 0
        {"VARCHAR", -65535, compareChar, equalChar, copyChar}, // 1
        {"TINYINT", 1, compareInt, equalInt, copyInt},         // 2
        {"SMALLINT", 2, compareInt, equalInt, copyInt},        // 3
        {"INT", 4, compareInt, equalInt, copyInt},
        {"EINT", 4, equalInt, equalInt, copyInt},     // 4
        {"BIGINT", 8, compareInt, equalInt, copyInt}, // 5
        {},                                           // x
    };

    int index = 0;
    do {
        if (gdatatype[index].name == NULL)
            break;
        else if (strcmp(gdatatype[index].name, name) == 0)
            return &gdatatype[index];
        else
            ++index;
    } while (true);
    return NULL;
}

} // namespace db