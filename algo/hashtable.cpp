/*
    Copyright 2018. simba wei

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "hashtable.h"
#include "../common/exceptions.h"

void HashTable::init(unsigned int nbuckets, unsigned int bucksize, unsigned int tuplesize)
{
    lock_ = new Lock[nbuckets];
    bucket_ = (void**)new char[sizeof(void*) * nbuckets];
    for (int i=0; i<nbuckets; ++i) {
        // allocating space for data + free pointer + next pointer
        bucket_[i] = new char[bucksize + 2*sizeof(void*)];
        void** free = (void**)(((char*)bucket_[i]) + bucksize);
        void** next = (void**)(((char*)bucket_[i]) + bucksize + sizeof(void*));
        *next = 0;
        *free = bucket_[i];
    }

    this->bucksize_ = bucksize;
    this->tuplesize_ = tuplesize;
    this->nbuckets_ = nbuckets;
    this->start_value_ = 0;
    this->end_value_ = 0;
}


void HashTable::destroy()
{
    for (int i=0; i<nbuckets_; ++i) {
        void* tmp = bucket_[i];
        void* cur = *(void**)(((char*)bucket_[i]) + bucksize_ + sizeof(void*));
        while (cur) {
            delete[] (char*)tmp;
            tmp = cur;
            cur = *(void**)(((char*)cur) + bucksize_ + sizeof(void*));
        }
        delete[] (char*) tmp;
    }

    delete[] lock_;
    delete[] (char*) bucket_;
}

void* HashTable::allocate(unsigned int offset)
{
#ifdef DEBUG
    assert(0 <= offset && offset<nbuckets_);
#endif
    void* data = bucket_[offset];
    void** freeloc = (void**)((char*)data + bucksize_);
    void* ret;
    if ((*freeloc) <= ((char*)data + bucksize_ - tuplesize_)) {
        // Fast path: it fits!
        //
        ret = *freeloc;
        *freeloc = ((char*)(*freeloc)) + tuplesize_;
        return ret;
    }

    // Allocate new page and make bucket[offset] point to it.
    //
    //throw PageFullException(offset);

    ret = new char[bucksize_ + 2*sizeof(void*)];
    bucket_[offset] = ret;

    void** nextloc = (void**)(((char*)ret) + bucksize_ + sizeof(void*));
    *nextloc = data;

    freeloc = (void**)(((char*)ret) + bucksize_);
    *freeloc = ((char*)ret) + tuplesize_; ///<point to the left of bucksize

    return ret;
}

HashTable::Iterator HashTable::create_iterator()
{
    return Iterator(bucksize_,tuplesize_);
}


HashTable::Iterator::Iterator(unsigned int bucksize, unsigned int tuplesize)
    : bucksize_(bucksize), tuplesize_(tuplesize), cur_(0), free_(0), next_(0)
{

}






























