////
// @file record.cc
// @brief
// 实现物理记录
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <vector>
#include <db/record.h>

namespace db {

// TODO: 加上log
std::pair<size_t, size_t> Record::size(const iovec *iov, int iovcnt)
{

    size_t iovoff=1; //iov偏移量
    size_t tot = 0; //总长度
    size_t head = 0; //head位置
    Integer it;
    for (int i = 0; i < iovcnt; i++) 
    {
        it.set(iovoff);
        head += it.size();
        iovoff += iov[i].iov_len;
    }
    tot = head + iovoff;
    it.set(tot);
    tot += it.size();
    head += it.size();
    return std::pair<size_t, size_t>(tot,head);

    /*size_t s1 = 0, s2 = 0;

    Integer it;
    for (int i = 0; i < iovcnt; ++i) {
        it.set(iov[i].iov_len);
        s1 += it.size() + iov[i].iov_len;
        s2 += it.size();
    }
    s1 += HEADER_SIZE; // header
    it.set(s1);
    s1 += it.size(); // 整个记录长度
    s2 += it.size(); // header偏移量

    return std::pair<size_t, size_t>(s1, s2);*/
}

size_t Record::set(const iovec *iov, int iovcnt, const unsigned char *header)
{
    // 偏移量
    unsigned int offset = 0;

    // 先计算所需空间大小
    std::pair<size_t, size_t> s1 = size(iov, iovcnt);
    if ((size_t) length_ < s1.first)
        return 0;
    else
        length_ = (unsigned short) s1.second;

    // 输出记录长度
    Integer it;
    it.set(s1.first);
    it.encode((char *) buffer_ + offset, length_);
    offset += (unsigned int) it.size();

    // 输出字段偏移量数组
    size_t len = HEADER_SIZE;
    for (int i = 0; i < iovcnt; ++i)
        len += iov[i].iov_len; // 计算总长
    // 逆序输出
    for (int i = iovcnt; i > 0; --i) {
        len -= iov[i - 1].iov_len;
        it.set(len);
        it.encode((char *) buffer_ + offset, length_);
        offset += (unsigned int) it.size();
    }

    // 输出头部
    memcpy(buffer_ + offset, header, HEADER_SIZE);

    // 顺序输出各字段
    for (int i = 0; i < iovcnt; ++i) {
        memcpy(buffer_ + offset, iov[i].iov_base, iov[i].iov_len);
        offset += (unsigned int) iov[i].iov_len;
    }

#if 0
    // 输出padding
    size_t s2 = offset % ALIGN_SIZE;
    if (s2)
        for (size_t i = 0; i < ALIGN_SIZE - s2; ++i) {
            this->buffer_[offset] = 0;
            offset;
        }
#endif

    // 设置length
    size_t ret = (s1.first + ALIGN_SIZE - 1) / ALIGN_SIZE * ALIGN_SIZE;
    length_ = (unsigned short) ret;

    return ret;
}

size_t Record::length()
{
    Integer it;
    return it.decode((char *) buffer_, length_) ? it.value_ : 0;
}

size_t Record::fields()
{
    // bypass总长
    Integer it;
    bool ret = it.decode((char *) buffer_, length_);
    if (!ret) return 0;
    unsigned short offset = it.size();

    // 枚举所有字段长度
    size_t total = 0;
    while (true) {
        if (offset >= length_) return 0;
        ret = it.decode((char *) buffer_ + offset, length_ - offset);
        if (!ret)
            return 0;
        else
            offset += it.size();

        ++total;
        // 找到尾部
        if (it.value_ == (unsigned long) HEADER_SIZE) break;
    }

    return total;
}

bool Record::get(iovec *iov, int iovcnt, unsigned char *header)
{
    //if (header == NULL) return false;
    size_t offset = 0;

    // 总长
    size_t length = 0;
    Integer it;
    bool ret = it.decode((char *) buffer_, length_);
    if (!ret) return false;
    length = it.get();

    // 枚举所有字段长度
    size_t total = 0;
    offset += it.size();
    std::vector<size_t> vec(iovcnt); // 存放各字段长度
    while (true) {
        if (offset >= length_) return false;
        ret = it.decode((char *) buffer_ + offset, length_ - offset);
        if (!ret) return false;

        vec[total] = it.get(); // 临时存放偏移量，注意是逆序
        if (++total > (size_t) iovcnt) return false;

        // 找到尾部
        if (it.value_ == (unsigned long) HEADER_SIZE)
            break;
        else
            offset += it.size();
    }
    if (total != (size_t) iovcnt) return false; // 字段数目不对
    // 逆序，先交换
    for (int i = 0; i < iovcnt / 2; ++i) {
        size_t tmp = vec[i];
        vec[i] = vec[iovcnt - i - 1];
        vec[iovcnt - i - 1] = tmp;
    }
    // check长度
    for (int i = 0; i < iovcnt - 1; ++i) {
        vec[i] = vec[i + 1] - vec[i];
        if (vec[i] > iov[i].iov_len)
            return false; // 要求iov长度足够
        else
            iov[i].iov_len = vec[i];
    }
    // 最后一个字段长度
    vec[iovcnt - 1] = length - vec[iovcnt - 1] - offset - HEADER_SIZE;
    if (vec[iovcnt - 1] > iov[iovcnt - 1].iov_len)
        return false;
    else
        iov[iovcnt - 1].iov_len = vec[iovcnt - 1];

    // 拷贝header
    ::memcpy(header, buffer_ + offset, HEADER_SIZE);
    offset += HEADER_SIZE;

    // 拷贝字段
    for (int i = 0; i < iovcnt; ++i) {
        ::memcpy(iov[i].iov_base, buffer_ + offset, iov[i].iov_len);
        offset += iov[i].iov_len;
    }

#if 0
    // padding
    size_t s2 = offset % ALIGN_SIZE;
    if (s2) offset += ALIGN_SIZE - s2;
#endif

    return true;
}

bool Record::ref(iovec *iov, int iovcnt, unsigned char *header)
{
    if (header == NULL) return false;
    size_t offset = 0;

    // 总长
    size_t length = 0;
    Integer it;
    bool ret = it.decode((char *) buffer_, length_);
    if (!ret) return false;
    length = it.get();

    // 枚举所有字段长度
    size_t total = 0;
    offset += it.size();
    std::vector<size_t> vec(iovcnt); // 存放各字段长度
    while (true) {
        if (offset >= length_) return false;
        ret = it.decode((char *) buffer_ + offset, length_ - offset);
        if (!ret) return false;

        vec[total] = it.get(); // 临时存放偏移量，注意是逆序
        if (++total > (size_t) iovcnt) return false;

        // 找到尾部
        if (it.value_ == (unsigned long) HEADER_SIZE)
            break;
        else
            offset += it.size();
    }
    if (total != (size_t) iovcnt) return false; // 字段数目不对
    // 逆序，先交换
    for (int i = 0; i < iovcnt / 2; ++i) {
        size_t tmp = vec[i];
        vec[i] = vec[iovcnt - i - 1];
        vec[iovcnt - i - 1] = tmp;
    }
    // 设置长度
    for (int i = 0; i < iovcnt - 1; ++i) {
        vec[i] = vec[i + 1] - vec[i];
        iov[i].iov_len = vec[i];
    }
    // 最后一个字段长度
    vec[iovcnt - 1] = length - vec[iovcnt - 1] - offset - HEADER_SIZE;
    iov[iovcnt - 1].iov_len = vec[iovcnt - 1];

    // 拷贝header
    ::memcpy(header, buffer_ + offset, HEADER_SIZE);
    offset += HEADER_SIZE;

    // 引用各字段
    for (int i = 0; i < iovcnt; ++i) {
        iov[i].iov_base = (void *) (buffer_ + offset);
        offset += iov[i].iov_len;
    }

    return true;
}

} // namespace db
